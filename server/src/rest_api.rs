use std::{
    collections::HashSet, fmt::Debug, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use axum::{
    body::Body,
    extract::{
        connect_info::IntoMakeServiceWithConnectInfo, ConnectInfo, DefaultBodyLimit, Multipart,
        Path, Query, State,
    },
    http::{Method, Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::get, //, post},
    Json,
    Router,
};
use tower_http::{
    trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};

use distd_core::{
    chunk_storage::ChunkStorage,
    chunks::{ChunkAlgorithm, ChunkingPolicy},
    feed::{Feed, Name as FeedName},
    hash::Hash,
    metadata::Server as ServerMetadata,
    utils::path::validate_relative,
    utils::serde::empty_string_as_none,
    version::Version,
};

use crate::Client;
use crate::Server as RawServer;

type Server<T> = Arc<RawServer<T>>;

fn bearer_token(request: &Request<Body>) -> Option<&str> {
    request
        .headers()
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

async fn authorize_rest(request: Request<Body>, next: Next) -> impl IntoResponse {
    if matches!(request.uri().path(), "/" | "/version") {
        return next.run(request).await;
    }

    let variable = if request.method() == Method::POST && request.uri().path() == "/items" {
        "DISTD_PUBLISH_TOKEN"
    } else {
        "DISTD_ADMIN_TOKEN"
    };
    let Ok(expected) = std::env::var(variable) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if expected.trim().is_empty() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(provided) = bearer_token(&request) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if ring::constant_time::verify_slices_are_equal(expected.as_bytes(), provided.as_bytes())
        .is_err()
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

#[derive(Deserialize, Serialize)]
struct ClientPostObj {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub version: Option<Version>,
    pub name: String,
    pub uuid: Option<String>,
    //pub realm: Option<Realm>,
}

/// Register a client
///
/// This endpoint is used by clients to register themselves to the server
/// The client should  provide a name and a version, the server will return
/// a UUID that the client should use to identify itself in future requests.
///
/// The version is optional and can be used to identify the client version
/// The name is mandatory and should be unique
///
/// # Errors
/// If the name is already in use, the server will return a 409 status code
async fn register_client<T>(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(client): Query<ClientPostObj>,
    State(server): State<Server<T>>,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    server
        .register_client(
            client.name,
            addr,
            client.version,
            client.uuid.and_then(|s| Uuid::from_str(&s).ok()),
        )
        .await
        .map(|uuid| uuid.to_string())
        .map_err(|_| StatusCode::CONFLICT)
}

/// Get version
async fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Get all clients
async fn get_clients<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    Json(
        server
            .clients
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<Client>>(),
    )
}

/// Get one client
async fn get_one_client<T>(
    Path(uuid): Path<String>,
    State(server): State<Server<T>>,
) -> Result<Json<Client>, StatusCode>
where
    T: ChunkStorage + Sync + Send,
{
    let uuid = Uuid::from_str(&uuid).ok().ok_or(StatusCode::BAD_REQUEST)?;

    server
        .clients
        .read()
        .await
        .get(&uuid)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Get all chunks
async fn get_chunks<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    Json(
        server
            .storage()
            .chunks()
            .into_iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>(),
    )
}

/// Get sum of all chunks sizes
async fn get_chunks_size_sum<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    Json(server.storage().size())
}

/// Get all feeds
async fn get_feeds<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    Json(
        server
            .metadata_snapshot()
            .await
            .feeds
            .values()
            .cloned()
            .collect::<Vec<Feed>>(),
    )
}

/// Get all items
async fn get_items<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    Json(
        server
            .metadata_snapshot()
            .await
            .items
            .keys()
            .cloned()
            .collect::<Vec<String>>(),
    )
}

#[derive(Deserialize, Serialize)]
struct ItemGetObj {
    /// Look up by artifact_id (preferred) or fall back to path
    pub artifact_id: Option<String>,
    pub path: Option<PathBuf>,
}

/// Get one item
async fn get_one_item<T>(
    Query(item): Query<ItemGetObj>,
    State(server): State<Server<T>>,
) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    let metadata = server.metadata_snapshot().await;
    let found = if let Some(ref aid) = item.artifact_id {
        metadata.items.get(aid).cloned()
    } else if let Some(ref path) = item.path {
        metadata
            .items
            .values()
            .find(|i| &i.metadata.path == path)
            .cloned()
    } else {
        None
    };
    Json(found)
}

#[derive(Deserialize, Serialize)]
struct ItemPostObj {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub description: Option<String>,
    pub path: PathBuf,
    pub name: String,
    pub artifact_id: Option<String>,
    pub chunking: Option<String>,
}

fn sanitize_relative_upload_path(path: &str) -> Option<PathBuf> {
    validate_relative(std::path::Path::new(path)).ok()
}

/// Publish an item
async fn publish_item<T>(
    Query(item_data): Query<ItemPostObj>,
    State(server): State<Server<T>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    let destination = validate_relative(&item_data.path).map_err(|_| StatusCode::BAD_REQUEST)?;
    let artifact_id = item_data
        .artifact_id
        .clone()
        .unwrap_or_else(|| item_data.name.clone());
    if artifact_id.is_empty() || artifact_id.len() > 256 || item_data.name.len() > 256 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut uploaded_files = Vec::new();
    let mut uploaded_paths = HashSet::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        if field.name() != Some("item") {
            continue;
        }
        let file_name = match field.file_name() {
            Some(file_name) => {
                sanitize_relative_upload_path(file_name).ok_or(StatusCode::BAD_REQUEST)?
            }
            None => PathBuf::from("item"),
        };
        if !uploaded_paths.insert(file_name.clone()) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let data = field
            .bytes()
            .await
            .inspect_err(|e| tracing::warn!("Cannot extract bytes from item field: {e}"))
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        uploaded_files.push((file_name, data));
    }

    if uploaded_files.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let requested_policy = item_data
        .chunking
        .as_deref()
        .unwrap_or("auto")
        .parse::<ChunkingPolicy>()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let chunk_algorithm = if uploaded_files.len() == 1 {
        let (relative_path, data) = &uploaded_files[0];
        let probe_path = if relative_path.as_os_str().is_empty() {
            &item_data.path
        } else {
            relative_path
        };
        ChunkAlgorithm::from_policy(requested_policy, probe_path, data.as_ref(), false)
    } else {
        ChunkAlgorithm::from_policy(requested_policy, &item_data.path, &[], true)
    };

    let result = if uploaded_files.len() == 1 {
        let (_, file) = uploaded_files.pop().unwrap();
        server
            .publish_item_as(
                artifact_id,
                item_data.name,
                destination,
                item_data.description,
                file,
                chunk_algorithm,
            )
            .await
    } else {
        server
            .publish_item_from_files_as(
                artifact_id,
                item_data.name,
                destination,
                item_data.description,
                uploaded_files,
                chunk_algorithm,
            )
            .await
    };
    let result = result.map(|item| item.metadata);
    tracing::debug!("{:?}", result);
    result
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Get one feed
async fn get_one_feed<T>(
    Path(name): Path<FeedName>,
    State(server): State<Server<T>>,
) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    Json(server.metadata_snapshot().await.feeds.get(&name).cloned())
}

/// Download data associated with an hash
/// This is a simple wrapper around `ChunkStorage::get`
///
/// # Errors
/// Returns `StatusCode::BAD_REQUEST` if the hash is not a valid `Hash`
/// Returns `StatusCode::NOT_FOUND` if the hash is not found in the storage
async fn get_chunk<T>(
    Path(hash): Path<String>,
    State(server): State<Server<T>>,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send,
{
    let hash = Hash::from_str(hash.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?;
    server
        .storage()
        .get(&hash)
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

/// Download data associated with an hash-tree from its root
async fn get_metadata<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send,
{
    let metadata = server.metadata_snapshot().await;
    Json(ServerMetadata::from(metadata))
}

/// Create a new `axum::Router` with all the routes
pub fn make_app<T>(server: RawServer<T>) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    Router::new()
        .route("/", get(version))
        .route("/version", get(version))
        .route("/clients", get(get_clients).post(register_client))
        .route("/clients/{uuid}", get(get_one_client))
        .route("/items/all", get(get_items))
        .route("/items", get(get_one_item).post(publish_item))
        .route("/chunks", get(get_chunks))
        .route("/chunks/size-sum", get(get_chunks_size_sum))
        .route("/chunks/get/{hash}", get(get_chunk))
        .route("/feeds", get(get_feeds))
        .route("/feeds/{feed_name}", get(get_one_feed))
        .route("/metadata", get(get_metadata))
        .with_state(Arc::new(server))
        .layer(middleware::from_fn(authorize_rest))
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(false))
                .on_request(DefaultOnRequest::new().level(tracing::Level::TRACE))
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::INFO)
                        .latency_unit(LatencyUnit::Micros),
                ),
        )
        .into_make_service_with_connect_info::<SocketAddr>()
}
