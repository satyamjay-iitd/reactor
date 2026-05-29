use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{MatchedPath, State},
    http::{HeaderMap, Request},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tower_http::cors::{Any, CorsLayer};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{Span, info_span};
#[cfg(feature = "swagger")]
use utoipa::{OpenApi, ToSchema};
#[cfg(feature = "swagger")]
use utoipa_swagger_ui::SwaggerUi;

#[cfg(feature = "chaos")]
use crate::ChaosMsg;
use crate::{ActorLifeCycle, JobControllerReq, SpawnActor};

#[derive(Clone)]
struct AppState {
    tx: UnboundedSender<JobControllerReq>,
}

//////////////////////////////////////////////////////////////////////
////////////////////////// COMPILE APIs //////////////////////////////
//////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "swagger", derive(ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CompilationArgs {
    pub lib_name: String,
    pub args: HashMap<String, Value>,
}

#[cfg_attr(feature = "swagger", utoipa::path(
    post,
    path = "/compile_lib",
    tag = "compile",
    request_body(
        content = CompilationArgs,
        description = "Arguments to compile an operator",
        content_type = "application/json"
    ),
    responses(
        (status = 201, description = "Compilation successful"),
        (status = 400, description = "Compilation Unsuccessful"),
        (status = 501, description = "Compilation Not Supported on this node")
    )
))]
async fn compile_lib(
    State(_state): State<Arc<AppState>>,
    Json(_reg_arg): Json<CompilationArgs>,
) -> impl IntoResponse {
    #[cfg(feature = "dynop")]
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        _state
            .clone()
            .tx
            .send(JobControllerReq::CompileOps {
                lib_name: _reg_arg.lib_name,
                args: _reg_arg.args,
                resp_tx: tx,
            })
            .unwrap();
        match rx.await.unwrap() {
            Ok(()) => (axum::http::StatusCode::CREATED, String::new()),
            Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()),
        }
    }
    #[cfg(not(feature = "dynop"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

//////////////////////////////////////////////////////////////////////
/////////////////////// ACTOR LIFECYCLE APIs /////////////////////////
//////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "swagger", derive(ToSchema))]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct SpawnArgs {
    pub actor_name: String,
    pub operator_name: String,
    pub lib_name: String,
    pub payload: HashMap<String, Value>,
}

#[cfg_attr(feature = "swagger", derive(ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RemoteActorInfo {
    pub name: String,
    pub hostname: String,
    pub port: u16,
}

#[cfg_attr(feature = "swagger", derive(ToSchema))]
#[derive(Serialize)]
struct StatusResponse {
    actors: Vec<String>,
    loaded_libs: HashMap<String, Vec<String>>,
}

#[cfg_attr(feature = "swagger", utoipa::path(
    post,
    path = "/start_actor",
    tag = "actor_lifecycle",
    request_body(
        content = SpawnArgs,
        description = "Actor arguments as arbitrary JSON",
        content_type = "application/json"
    ),
    responses(
        (status = 201, description = "Start a new actor", body = RemoteActorInfo)
    )
))]
async fn start_actor(
    State(state): State<Arc<AppState>>,
    Json(args): Json<SpawnArgs>,
) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .clone()
        .tx
        .send(JobControllerReq::ActorLifeCycle(
            ActorLifeCycle::SpawnActor(SpawnActor {
                addr: args.actor_name.clone(),
                resp_tx: tx,
                op_name: args.operator_name,
                lib_name: args.lib_name,
                payload: args.payload,
            }),
        ))
        .unwrap();
    let status = rx.await.unwrap();
    assert!(status.is_some());

    let detail = RemoteActorInfo {
        name: args.actor_name,
        hostname: "".to_string(),
        port: status.unwrap().port,
    };
    (axum::http::StatusCode::CREATED, Json(detail))
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/actor_added",
    tag = "actor_lifecycle",
    request_body(
        content = RemoteActorInfo,
        description = "Remote Actor Detail",
        content_type = "application/json"
    ),
    responses(
        (status = 201, description = "Notify actor start on remote")
    )
))]
async fn actor_added(
    State(state): State<Arc<AppState>>,
    Json(actor_info): Json<RemoteActorInfo>,
) -> impl IntoResponse {
    let remote_ip: IpAddr = actor_info.hostname.parse().unwrap();
    state
        .clone()
        .tx
        .send(JobControllerReq::ActorLifeCycle(
            ActorLifeCycle::RemoteActorAdded {
                addr: actor_info.name,
                sock_addr: SocketAddr::new(remote_ip, actor_info.port),
            },
        ))
        .unwrap();
    (axum::http::StatusCode::CREATED, "Actor added!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/stop_actor",
    tag = "actor_lifecycle",
    responses(
        (status = 200, description = "Actor stop initiated"),
        (status = 404, description = "Actor not found")
    )
))]
async fn stop_actor(
    State(state): State<Arc<AppState>>,
    Json(actor_addr): Json<String>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::ActorLifeCycle(
            ActorLifeCycle::StopActor {
                addr: actor_addr.clone(),
            },
        ))
        .unwrap();
    (
        axum::http::StatusCode::OK,
        format!("Actor {} Stopped!", actor_addr),
    )
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/stop_all_actors",
    tag = "actor_lifecycle",
    responses(
        (status = 200, description = "Actors stop initiated")
    )
))]
async fn stop_all_actors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::ActorLifeCycle(
            ActorLifeCycle::StopAllActors,
        ))
        .unwrap();
    (axum::http::StatusCode::OK, "Actors Stopped!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    get,
    path = "/status",
    tag = "actor_lifecycle",
    responses(
        (status = 200, description = "Status of the node", body = StatusResponse)
    )
))]
async fn get_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .tx
        .send(JobControllerReq::ActorLifeCycle(
            ActorLifeCycle::GetStatus { resp_tx: tx },
        ))
        .unwrap();
    let result = rx.await.unwrap();
    (
        axum::http::StatusCode::OK,
        Json(StatusResponse {
            actors: result.actors,
            loaded_libs: result.loaded_libs,
        }),
    )
}

//////////////////////////////////////////////////////////////////////
////////////////////////// CHAOS APIs ////////////////////////////////
//////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "swagger", derive(utoipa::ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub struct MsgLossRequest {
    pub actor_name: String,
    pub probability: f32,
}

#[cfg_attr(feature = "swagger", derive(utoipa::ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub struct MsgDuplicationRequest {
    pub actor_name: String,
    pub factor: u32,
    pub probability: f32,
}

#[cfg_attr(feature = "swagger", derive(utoipa::ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub struct MsgDelayRequest {
    pub actor_name: String,
    pub delay_range_start: u64,
    pub delay_range_end: u64,
    pub senders: Vec<String>,
}

#[cfg_attr(feature = "swagger", derive(utoipa::ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub struct DisableMsgDelayRequest {
    pub actor_name: String,
    pub senders: Vec<String>,
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_duplication",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Duplication Config Applied")
    )
))]
async fn set_duplication(
    State(_state): State<Arc<AppState>>,
    Json(_dupl_request): Json<MsgDuplicationRequest>,
) -> impl IntoResponse {
    #[cfg(feature = "chaos")]
    {
        _state
            .clone()
            .tx
            .send(JobControllerReq::ChaosMsg(ChaosMsg::MsgDuplication {
                actor_name: _dupl_request.actor_name,
                factor: _dupl_request.factor,
                probability: _dupl_request.probability,
            }))
            .unwrap();
        (axum::http::StatusCode::OK, "Chaos Config Applied!")
    }
    #[cfg(not(feature = "chaos"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_msg_loss",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Loss Config Applied")
    )
))]
async fn set_msg_loss(
    State(_state): State<Arc<AppState>>,
    Json(_loss_request): Json<MsgLossRequest>,
) -> impl IntoResponse {
    #[cfg(feature = "chaos")]
    {
        _state
            .clone()
            .tx
            .send(JobControllerReq::ChaosMsg(ChaosMsg::MsgLoss {
                actor_name: _loss_request.actor_name,
                probability: _loss_request.probability,
            }))
            .unwrap();
        (axum::http::StatusCode::OK, "Chaos Config Applied!")
    }
    #[cfg(not(feature = "chaos"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_msg_delay",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Delay Config Applied")
    )
))]
async fn set_msg_delay(
    State(_state): State<Arc<AppState>>,
    Json(_delay_request): Json<MsgDelayRequest>,
) -> impl IntoResponse {
    #[cfg(feature = "chaos")]
    {
        _state
            .clone()
            .tx
            .send(JobControllerReq::ChaosMsg(ChaosMsg::MsgDelay {
                actor_name: _delay_request.actor_name,
                senders: _delay_request.senders,
                delay_range_ms: (
                    _delay_request.delay_range_start,
                    _delay_request.delay_range_end,
                ),
            }))
            .unwrap();
        (axum::http::StatusCode::OK, "Chaos Config Applied!")
    }
    #[cfg(not(feature = "chaos"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_duplication",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Duplication Config Removed")
    )
))]
async fn unset_msg_duplication(
    State(_state): State<Arc<AppState>>,
    Json(_actor_addr): Json<String>,
) -> impl IntoResponse {
    #[cfg(feature = "chaos")]
    {
        _state
            .clone()
            .tx
            .send(JobControllerReq::ChaosMsg(
                ChaosMsg::DisableMsgDuplication {
                    actor_name: _actor_addr,
                },
            ))
            .unwrap();
        (axum::http::StatusCode::OK, "Chaos Config Removed!")
    }
    #[cfg(not(feature = "chaos"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_loss",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Loss Config Removed")
    )
))]
async fn unset_msg_loss(
    State(_state): State<Arc<AppState>>,
    Json(_actor_addr): Json<String>,
) -> impl IntoResponse {
    #[cfg(feature = "chaos")]
    {
        _state
            .clone()
            .tx
            .send(JobControllerReq::ChaosMsg(ChaosMsg::DisableMsgLoss {
                actor_name: _actor_addr,
            }))
            .unwrap();
        (axum::http::StatusCode::OK, "Chaos Config Removed!")
    }
    #[cfg(not(feature = "chaos"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_delay",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Delay Config Removed")
    )
))]
async fn unset_msg_delay(
    State(_state): State<Arc<AppState>>,
    Json(_disable_delay_request): Json<DisableMsgDelayRequest>,
) -> impl IntoResponse {
    #[cfg(feature = "chaos")]
    {
        _state
            .clone()
            .tx
            .send(JobControllerReq::ChaosMsg(ChaosMsg::DisableMsgDelay {
                actor_name: _disable_delay_request.actor_name,
                senders: _disable_delay_request.senders,
            }))
            .unwrap();
        (axum::http::StatusCode::OK, "Chaos Config Removed!")
    }
    #[cfg(not(feature = "chaos"))]
    axum::http::StatusCode::NOT_IMPLEMENTED
}

//////////////////////////////////////////////////////////////////////

#[cfg(feature = "swagger")]
#[derive(OpenApi)]
#[openapi(
    paths(
        compile_lib,
        start_actor,
        actor_added,
        stop_actor,
        stop_all_actors,
        get_status,
        set_duplication,
        set_msg_loss,
        set_msg_delay,
        unset_msg_duplication,
        unset_msg_loss,
        unset_msg_delay,
    ),
    tags(
        (name = "compile", description = "Compile dynamic operator libraries"),
        (name = "actor_lifecycle", description = "Spawn, stop, and inspect actors"),
        (name = "chaos", description = "Inject message faults for chaos testing"),
    )
)]
struct ApiDoc;

pub async fn webserver(
    job_control_tx: UnboundedSender<JobControllerReq>,
    port: u16,
    extension: crate::NodeExtension,
) {
    let state = Arc::new(AppState { tx: job_control_tx });
    let app = Router::new()
        // compile
        .route("/compile_lib", post(compile_lib))
        // actor lifecycle
        .route("/status", get(get_status))
        .route("/start_actor", post(start_actor))
        .route("/actor_added", post(actor_added))
        .route("/stop_actor", post(stop_actor))
        .route("/stop_all_actors", post(stop_all_actors))
        // chaos
        .route("/set_duplication", post(set_duplication))
        .route("/set_msg_loss", post(set_msg_loss))
        .route("/set_msg_delay", post(set_msg_delay))
        .route("/unset_msg_duplication", post(unset_msg_duplication))
        .route("/unset_msg_loss", post(unset_msg_loss))
        .route("/unset_msg_delay", post(unset_msg_delay))
        .with_state(state)
        .merge(extension.router);
    let app = app
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    let matched_path = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str);

                    info_span!(
                        "http_request",
                        method = ?request.method(),
                        matched_path,
                        some_other_field = ?request.headers().get("user-agent"),
                    )
                })
                .on_request(|request: &Request<_>, _span: &Span| {
                    tracing::info!(method = ?request.method(), uri = %request.uri(), "received request");
                })
                .on_response(|response: &Response<_>, latency: Duration, _span: &Span| {
                    tracing::info!(status = %response.status(), latency = ?latency, "sending response");
                })
                .on_body_chunk(|chunk: &Bytes, latency: Duration, _span: &Span| {
                    tracing::debug!(size = chunk.len(), latency = ?latency, "sending body chunk");
                })
                .on_eos(|trailers: Option<&HeaderMap>, stream_duration: Duration, _span: &Span| {
                    tracing::debug!(trailers = ?trailers, stream_duration = ?stream_duration, "stream closed");
                })
                .on_failure(|error: ServerErrorsFailureClass, latency: Duration, _span: &Span| {
                    tracing::error!(error = ?error, latency = ?latency, "request failed");
                }),
        );

    #[cfg(feature = "swagger")]
    let app = {
        let mut doc = ApiDoc::openapi();
        doc.merge(extension.openapi);
        app.merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", doc))
    };

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
