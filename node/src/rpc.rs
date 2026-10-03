use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{MatchedPath, State},
    http::{HeaderMap, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{Span, info_span};
#[cfg(feature = "swagger")]
use utoipa::{OpenApi, ToSchema};
#[cfg(feature = "swagger")]
use utoipa_swagger_ui::SwaggerUi;

#[cfg(feature = "chaos")]
use crate::ChaosMsg;
use crate::{ActorLifeCycle, JobControllerReq, NodeConfig, SpawnActor, SpawnError};

#[derive(Clone)]
struct AppState {
    tx: UnboundedSender<JobControllerReq>,
}

impl AppState {
    /// Sends a request to the node controller and waits for its reply.
    async fn request<T>(
        &self,
        make_request: impl FnOnce(oneshot::Sender<T>) -> JobControllerReq,
    ) -> Result<T, Response> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx.send(make_request(resp_tx)).map_err(|_| controller_unavailable())?;
        resp_rx.await.map_err(|_| controller_unavailable())
    }

    /// Sends a request to the node controller without waiting for it to be handled.
    fn notify(&self, request: JobControllerReq) -> Result<(), Response> {
        self.tx.send(request).map_err(|_| controller_unavailable())
    }
}

/// A plain-text error response.
fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

fn controller_unavailable() -> Response {
    error(StatusCode::INTERNAL_SERVER_ERROR, "the node controller is not running")
}

/// The node's host as the client addressed it (the `Host` header without its port).
fn request_host(headers: &HeaderMap) -> String {
    let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) else {
        return String::new();
    };
    if let Some(bracketed) = host.strip_prefix('[') {
        // IPv6: [::1]:8080
        return bracketed.split(']').next().unwrap_or_default().to_string();
    }
    match host.rsplit_once(':') {
        Some((name, port)) if port.chars().all(|c| c.is_ascii_digit()) => name.to_string(),
        _ => host.to_string(),
    }
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
        (status = 201, description = "Compiled and loaded"),
        (status = 200, description = "Already loaded, compiled from identical arguments; nothing to do"),
        (status = 400, description = "Code generation or compilation failed (message in the body)"),
        (status = 500, description = "The node controller is not running"),
        (status = 501, description = "Compilation Not Supported on this node")
    )
))]
async fn compile_lib(
    State(_state): State<Arc<AppState>>,
    Json(_reg_arg): Json<CompilationArgs>,
) -> Response {
    #[cfg(feature = "dynop")]
    {
        use crate::lib_builder::BuildError;
        let result = _state
            .request(|resp_tx| JobControllerReq::CompileOps {
                lib_name: _reg_arg.lib_name,
                args: _reg_arg.args,
                resp_tx,
            })
            .await;
        match result {
            Ok(Ok(true)) => StatusCode::CREATED.into_response(),
            Ok(Ok(false)) => StatusCode::OK.into_response(),
            Ok(Err(e @ BuildError::CompilationNotSupported)) => error(StatusCode::NOT_IMPLEMENTED, e.to_string()),
            Ok(Err(e)) => error(StatusCode::BAD_REQUEST, e.to_string()),
            Err(response) => response,
        }
    }
    #[cfg(not(feature = "dynop"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
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
        (status = 201, description = "Started; `hostname` is the node's host as addressed by the client", body = RemoteActorInfo),
        (status = 400, description = "The operator failed to start, e.g. because of an invalid payload"),
        (status = 404, description = "No such library or operator"),
        (status = 409, description = "An actor with this name already exists"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn start_actor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(args): Json<SpawnArgs>,
) -> Result<(StatusCode, Json<RemoteActorInfo>), Response> {
    let name = args.actor_name.clone();
    let spawned = state
        .request(|resp_tx| {
            JobControllerReq::ActorLifeCycle(ActorLifeCycle::SpawnActor(SpawnActor {
                addr: args.actor_name,
                resp_tx,
                op_name: args.operator_name,
                lib_name: args.lib_name,
                payload: args.payload,
            }))
        })
        .await?
        .map_err(|e| {
            let status = match e {
                SpawnError::LibraryNotFound(_) | SpawnError::OperatorNotFound { .. } => StatusCode::NOT_FOUND,
                SpawnError::ActorExists(_) => StatusCode::CONFLICT,
                SpawnError::OperatorFailed { .. } => StatusCode::BAD_REQUEST,
            };
            error(status, e.to_string())
        })?;
    let detail = RemoteActorInfo {
        name,
        hostname: request_host(&headers),
        port: spawned.port,
    };
    Ok((StatusCode::CREATED, Json(detail)))
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
        (status = 201, description = "Notify actor start on remote"),
        (status = 400, description = "`hostname` is not an IP address"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn actor_added(
    State(state): State<Arc<AppState>>,
    Json(actor_info): Json<RemoteActorInfo>,
) -> Result<(StatusCode, &'static str), Response> {
    let remote_ip: IpAddr = actor_info.hostname.parse().map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            format!("hostname must be an IP address, got '{}'", actor_info.hostname),
        )
    })?;
    state.notify(JobControllerReq::ActorLifeCycle(ActorLifeCycle::RemoteActorAdded {
        addr: actor_info.name,
        sock_addr: SocketAddr::new(remote_ip, actor_info.port),
    }))?;
    Ok((StatusCode::CREATED, "Actor added!"))
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/stop_actor",
    tag = "actor_lifecycle",
    responses(
        (status = 200, description = "Actor stop initiated"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn stop_actor(State(state): State<Arc<AppState>>, actor_addr: String) -> Response {
    let found = state
        .request(|resp_tx| {
            JobControllerReq::ActorLifeCycle(ActorLifeCycle::StopActor {
                addr: actor_addr.clone(),
                resp_tx,
            })
        })
        .await;
    match found {
        Ok(true) => (StatusCode::OK, format!("Actor {actor_addr} Stopped!")).into_response(),
        Ok(false) => error(StatusCode::NOT_FOUND, format!("no actor named '{actor_addr}'")),
        Err(response) => response,
    }
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/stop_all_actors",
    tag = "actor_lifecycle",
    responses(
        (status = 200, description = "Actors stop initiated"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn stop_all_actors(State(state): State<Arc<AppState>>) -> Result<(StatusCode, &'static str), Response> {
    state.notify(JobControllerReq::ActorLifeCycle(ActorLifeCycle::StopAllActors))?;
    Ok((StatusCode::OK, "Actors Stopped!"))
}

#[cfg_attr(feature="swagger", utoipa::path(
    get,
    path = "/status",
    tag = "actor_lifecycle",
    responses(
        (status = 200, description = "Status of the node", body = StatusResponse),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn get_status(State(state): State<Arc<AppState>>) -> Result<Json<StatusResponse>, Response> {
    let status = state
        .request(|resp_tx| JobControllerReq::ActorLifeCycle(ActorLifeCycle::GetStatus { resp_tx }))
        .await?;
    Ok(Json(StatusResponse {
        actors: status.actors,
        loaded_libs: status.loaded_libs,
    }))
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

/// Applies a fault-injection setting: 404 if the actor does not exist.
#[cfg(feature = "chaos")]
async fn apply_chaos(state: &AppState, actor_name: String, msg: ChaosMsg, done: &'static str) -> Response {
    match state.request(|resp_tx| JobControllerReq::ChaosMsg { msg, resp_tx }).await {
        Ok(true) => (StatusCode::OK, done).into_response(),
        Ok(false) => error(StatusCode::NOT_FOUND, format!("no actor named '{actor_name}'")),
        Err(response) => response,
    }
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_duplication",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Duplication Config Applied"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn set_duplication(State(_state): State<Arc<AppState>>, Json(_dupl_request): Json<MsgDuplicationRequest>) -> Response {
    #[cfg(feature = "chaos")]
    {
        apply_chaos(&_state, _dupl_request.actor_name.clone(), ChaosMsg::MsgDuplication { actor_name: _dupl_request.actor_name, factor: _dupl_request.factor, probability: _dupl_request.probability }, "Chaos Config Applied!").await
    }
    #[cfg(not(feature = "chaos"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_msg_loss",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Loss Config Applied"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn set_msg_loss(State(_state): State<Arc<AppState>>, Json(_loss_request): Json<MsgLossRequest>) -> Response {
    #[cfg(feature = "chaos")]
    {
        apply_chaos(&_state, _loss_request.actor_name.clone(), ChaosMsg::MsgLoss { actor_name: _loss_request.actor_name, probability: _loss_request.probability }, "Chaos Config Applied!").await
    }
    #[cfg(not(feature = "chaos"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/set_msg_delay",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Delay Config Applied"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn set_msg_delay(State(_state): State<Arc<AppState>>, Json(_delay_request): Json<MsgDelayRequest>) -> Response {
    #[cfg(feature = "chaos")]
    {
        apply_chaos(&_state, _delay_request.actor_name.clone(), ChaosMsg::MsgDelay { actor_name: _delay_request.actor_name, senders: _delay_request.senders, delay_range_ms: (_delay_request.delay_range_start, _delay_request.delay_range_end) }, "Chaos Config Applied!").await
    }
    #[cfg(not(feature = "chaos"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_duplication",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Duplication Config Removed"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn unset_msg_duplication(State(_state): State<Arc<AppState>>, _actor_addr: String) -> Response {
    #[cfg(feature = "chaos")]
    {
        apply_chaos(&_state, _actor_addr.clone(), ChaosMsg::DisableMsgDuplication { actor_name: _actor_addr }, "Chaos Config Removed!").await
    }
    #[cfg(not(feature = "chaos"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_loss",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Loss Config Removed"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn unset_msg_loss(State(_state): State<Arc<AppState>>, _actor_addr: String) -> Response {
    #[cfg(feature = "chaos")]
    {
        apply_chaos(&_state, _actor_addr.clone(), ChaosMsg::DisableMsgLoss { actor_name: _actor_addr }, "Chaos Config Removed!").await
    }
    #[cfg(not(feature = "chaos"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
}

#[cfg_attr(feature="swagger", utoipa::path(
    post,
    path = "/unset_msg_delay",
    tag = "chaos",
    responses(
        (status = 200, description = "Msg Delay Config Removed"),
        (status = 404, description = "Actor not found"),
        (status = 500, description = "The node controller is not running")
    )
))]
async fn unset_msg_delay(State(_state): State<Arc<AppState>>, Json(_disable_delay_request): Json<DisableMsgDelayRequest>) -> Response {
    #[cfg(feature = "chaos")]
    {
        apply_chaos(&_state, _disable_delay_request.actor_name.clone(), ChaosMsg::DisableMsgDelay { actor_name: _disable_delay_request.actor_name, senders: _disable_delay_request.senders }, "Chaos Config Removed!").await
    }
    #[cfg(not(feature = "chaos"))]
    StatusCode::NOT_IMPLEMENTED.into_response()
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

/// Checks the configuration and binds the API's listener.
pub(crate) async fn listen(config: &NodeConfig) -> std::io::Result<tokio::net::TcpListener> {
    config
        .validate()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let addr = SocketAddr::new(config.bind, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        std::io::Error::new(e.kind(), format!("cannot listen on {addr}: {e}"))
    })?;
    let auth = if config.auth_token.is_some() { "bearer token required" } else { "no auth" };
    println!("\nWill Now Listen on {addr} ({auth})");
    Ok(listener)
}

/// Answers 401 unless the request carries the node's token.
async fn require_token(
    State(config): State<Arc<NodeConfig>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if config.authorized(request.headers().get(header::AUTHORIZATION)) {
        next.run(request).await
    } else {
        let mut response = error(StatusCode::UNAUTHORIZED, "missing or wrong auth token");
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, header::HeaderValue::from_static("Bearer"));
        response
    }
}

pub async fn webserver(
    job_control_tx: UnboundedSender<JobControllerReq>,
    listener: tokio::net::TcpListener,
    config: NodeConfig,
    extension: crate::NodeExtension,
) {
    let config = Arc::new(config);
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
    let cors_config = config.clone();
    let app = app
        // last resort: a panicking handler answers 500 instead of dropping the connection
        .layer(CatchPanicLayer::new())
        // inside the CORS layer, which answers browsers' preflight requests without a token
        .layer(middleware::from_fn_with_state(config, require_token))
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::predicate(move |origin, _| {
                    cors_config.origin_allowed(origin)
                }))
                .allow_methods([Method::GET, Method::POST])
                .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]),
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
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
        let mut doc = ApiDoc::openapi();
        doc.merge(extension.openapi);
        // every route, including the extension's, sits behind `require_token`
        doc.components.get_or_insert_with(Default::default).add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
        doc.security = Some(vec![SecurityRequirement::new("bearer_auth", Vec::<String>::new())]);
        app.merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", doc))
    };

    axum::serve(listener, app).await.unwrap();
}
