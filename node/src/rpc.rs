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

use crate::JobControllerReq;

#[derive(Clone)]
struct AppState {
    tx: UnboundedSender<JobControllerReq>,
}

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

// #[cfg_attr(feature = "swagger", derive(utoipa::ToSchema))]
// #[derive(Serialize, Deserialize, Debug)]
// pub struct CrashRequest {
//     pub actor_name: String,
// }

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

#[cfg_attr(feature = "swagger", derive(ToSchema))]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RegistrationArgs {
    pub lib_name: String,
    pub args: HashMap<String, Value>,
}

#[cfg_attr(feature = "swagger", utoipa::path(
    post,
    path = "/register_lib",
    request_body(
        content = RegistrationArgs,
        description = "Arguments to compile an operator",
        content_type = "application/json"
    ),
    responses(
        (status = 201, description = "Registration successful"),
        (status = 400, description = "Registration Unsuccessful"),
        (status = 501, description = "Registration Not Supported on this node")
    )
))]
async fn register_lib(
    State(_state): State<Arc<AppState>>,
    Json(_reg_arg): Json<RegistrationArgs>,
) -> impl IntoResponse {
    #[cfg(feature = "dynop")]
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        _state
            .clone()
            .tx
            .send(JobControllerReq::RegisterOps {
                lib_name: _reg_arg.lib_name,
                args: _reg_arg.args,
                resp_tx: tx,
            })
            .unwrap();
        let register_result = rx.await.unwrap();
        assert!(register_result.is_some());

        axum::http::StatusCode::CREATED
    }
    #[cfg(not(feature = "dynop"))]
    {
        axum::http::StatusCode::NOT_IMPLEMENTED
    }
}

#[cfg_attr(feature = "swagger", utoipa::path(
    post,
    path = "/start_actor",
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
        .send(JobControllerReq::SpawnActor {
            addr: args.actor_name.clone(),
            resp_tx: tx,
            op_name: args.operator_name,
            lib_name: args.lib_name,
            payload: args.payload,
        })
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
    /*if let Ok(mut hosts) = lookup_host(&actor_info.hostname).await {
        if let Some(socket_addr) = hosts.next() {
            let remote_ip = socket_addr.ip();
            println!("Resolved remote IP: {}", remote_ip);

            let sock_addr = SocketAddr::new(remote_ip, actor_info.port);
            println!("Full socket address: {}", sock_addr);

            state
                .clone()
                .tx
                .send(JobControllerReq::RemoteActorAdded {
                    addr: actor_info.name.leak(),
                    sock_addr,
                })
                .unwrap();

            (axum::http::StatusCode::CREATED, "Actor added!")
        } else {
            eprintln!("No IPs resolved for hostname: {}", actor_info.hostname);
            (axum::http::StatusCode::BAD_REQUEST, "Hostname could not be resolved")
        }
    } else {
        eprintln!("Failed to lookup host: {}", actor_info.hostname);
        (axum::http::StatusCode::BAD_REQUEST, "Invalid hostname")
    }*/

    /*let remote_ip = lookup_host(actor_info.hostname)
    .await
    .unwrap()
    .next()
    .unwrap()
    .ip();*/
    let remote_ip: IpAddr = actor_info.hostname.parse().unwrap();
    state
        .clone()
        .tx
        .send(JobControllerReq::RemoteActorAdded {
            addr: actor_info.name,
            sock_addr: SocketAddr::new(remote_ip, actor_info.port),
        })
        .unwrap();
    (axum::http::StatusCode::CREATED, "Actor added!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/stop_actor",
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
        .send(JobControllerReq::StopActor {
            addr: actor_addr.clone(),
        })
        .unwrap();
    (
        axum::http::StatusCode::OK,
        format!("Actor {} Stopped!", actor_addr),
    )
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/stop_all_actors",
    responses(
        (status = 200, description = "Actors stop initiated")
    )
))]
async fn stop_all_actors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::StopAllActors)
        .unwrap();
    (axum::http::StatusCode::OK, "Actors Stopped!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_duplication",
    responses(
        (status = 200, description = "Msg Duplication Config Applied")
    )
))]
async fn set_duplication(
    State(state): State<Arc<AppState>>,
    Json(dupl_request): Json<MsgDuplicationRequest>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::MsgDuplication {
            actor_name: dupl_request.actor_name,
            factor: dupl_request.factor,
            probability: dupl_request.probability,
        })
        .unwrap();
    (axum::http::StatusCode::OK, "Chaos Config Applied!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_msg_loss",
    responses(
        (status = 200, description = "Msg Loss Config Applied")
    )
))]
async fn set_msg_loss(
    State(state): State<Arc<AppState>>,
    Json(loss_request): Json<MsgLossRequest>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::MsgLoss {
            actor_name: loss_request.actor_name,
            probability: loss_request.probability,
        })
        .unwrap();
    (axum::http::StatusCode::OK, "Chaos Config Applied!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_msg_delay",
    responses(
        (status = 200, description = "Msg Delay Config Applied")
    )
))]
async fn set_msg_delay(
    State(state): State<Arc<AppState>>,
    Json(delay_request): Json<MsgDelayRequest>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::MsgDelay {
            actor_name: delay_request.actor_name,
            senders: delay_request.senders,
            delay_range_ms: (
                delay_request.delay_range_start,
                delay_request.delay_range_end,
            ),
        })
        .unwrap();
    (axum::http::StatusCode::OK, "Chaos Config Applied!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_duplication",
    responses(
        (status = 200, description = "Msg Duplication Config Removed")
    )
))]
async fn unset_msg_duplication(
    State(state): State<Arc<AppState>>,
    Json(actor_addr): Json<String>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::DisableMsgDuplication {
            actor_name: actor_addr,
        })
        .unwrap();
    (axum::http::StatusCode::OK, "Chaos Config Removed!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_loss",
    responses(
        (status = 200, description = "Msg Loss Config Removed")
    )
))]
async fn unset_msg_loss(
    State(state): State<Arc<AppState>>,
    Json(actor_addr): Json<String>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::DisableMsgLoss {
            actor_name: actor_addr,
        })
        .unwrap();
    (axum::http::StatusCode::OK, "Chaos Config Removed!")
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_delay",
    responses(
        (status = 200, description = "Msg Delay Config Removed")
    )
))]
async fn unset_msg_delay(
    State(state): State<Arc<AppState>>,
    Json(disable_delay_request): Json<DisableMsgDelayRequest>,
) -> impl IntoResponse {
    state
        .clone()
        .tx
        .send(JobControllerReq::DisableMsgDelay {
            actor_name: disable_delay_request.actor_name,
            senders: disable_delay_request.senders,
        })
        .unwrap();
    (axum::http::StatusCode::OK, "Chaos Config Removed!")
}
#[derive(Serialize, ToSchema)]
struct StatusResponse {
    actors: Vec<String>,
    loaded_libs: HashMap<String, Vec<String>>,
}
#[cfg_attr(feature="swagger", utoipa::path(
    get,
    path = "/status",
    responses(
        (status = 200, description = "Status of the node", body = StatusResponse)
    )
))]
async fn get_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .tx
        .send(JobControllerReq::GetStatus { resp_tx: tx })
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

#[cfg(feature = "swagger")]
#[derive(OpenApi)]
#[openapi(paths(
    start_actor,
    actor_added,
    register_lib,
    stop_actor,
    stop_all_actors,
    set_duplication,
    set_msg_loss,
    set_msg_delay,
    unset_msg_duplication,
    unset_msg_loss,
    unset_msg_delay,
    get_status
))]
struct ApiDoc;

pub async fn webserver(job_control_tx: UnboundedSender<JobControllerReq>, port: u16) {
    let state = Arc::new(AppState { tx: job_control_tx });
    let app = Router::new()
        .route("/status", get(get_status))
        .route("/start_actor", post(start_actor))
        .route("/actor_added", post(actor_added))
        .route("/register_lib", post(register_lib))
        .route("/stop_actor", post(stop_actor))
        .route("/stop_all_actors", post(stop_all_actors))
        .route("/set_duplication", post(set_duplication))
        .route("/set_msg_loss", post(set_msg_loss))
        .route("/set_msg_delay", post(set_msg_delay))
        .route("/unset_msg_duplication", post(unset_msg_duplication))
        .route("/unset_msg_loss", post(unset_msg_loss))
        .route("/unset_msg_delay", post(unset_msg_delay))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)       // allow all origins
                .allow_methods(Any)      // allow all methods (GET, POST, etc.)
                .allow_headers(Any),     // allow all headers
        )
        .with_state(state)
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
    let app = app.merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", ApiDoc::openapi()));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
