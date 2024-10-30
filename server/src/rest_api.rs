use std::{fmt::Debug, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use axum::{
    extract::{
        connect_info::IntoMakeServiceWithConnectInfo, ConnectInfo, DefaultBodyLimit, Multipart,
        Path, Query, State,
    },
    http::StatusCode,
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
    feed::{Feed, Name as FeedName},
    hash::Hash,
    metadata::Server as ServerMetadata,
    utils::serde::empty_string_as_none,
    version::Version,
};

use crate::Client;
use crate::Server as RawServer;

type Server<T> = Arc<RawServer<T>>;

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
    T: ChunkStorage + Sync + Send + Default + Debug,
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
    T: ChunkStorage + Sync + Send + Default,
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
    T: ChunkStorage + Sync + Send + Default,
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
    T: ChunkStorage + Sync + Send + Default,
{
    Json(
        server
            .storage
            .read()
            .await
            .chunks()
            .into_iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>(),
    )
}

/// Get sum of all chunks sizes
async fn get_chunks_size_sum<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Default,
{
    Json(server.storage.read().await.size())
}

/// Get all feeds
async fn get_feeds<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Default,
{
    Json(
        server
            .metadata
            .read()
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
    T: ChunkStorage + Sync + Send + Default,
{
    Json(
        server
            .metadata
            .read()
            .await
            .items
            .keys()
            .cloned()
            .map(|x| x.to_string_lossy().into())
            .collect::<Vec<String>>(),
    )
}

#[derive(Deserialize, Serialize)]
struct ItemGetObj {
    pub path: PathBuf,
}

/// Get one item
async fn get_one_item<T>(
    Query(item): Query<ItemGetObj>,
    State(server): State<Server<T>>,
) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Default,
{
    Json(server.metadata.read().await.items.get(&item.path).cloned())
}

#[derive(Deserialize, Serialize)]
struct ItemPostObj {
    //pub name: ItemName,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub description: Option<String>,
    pub path: PathBuf,
    pub name: String,
}

/// Publish an item
async fn publish_item<T>(
    Query(item_data): Query<ItemPostObj>,
    State(server): State<Server<T>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Default + Debug,
{
    while let Some(field) = multipart
        .next_field()
        .await
        .expect("Failed to get next field!")
    {
        if field.name().unwrap() != "item" {
            continue;
        }
        let res = server
            .publish_item(
                item_data.name,
                item_data.path,
                item_data.description,
                field
                    .bytes()
                    .await
                    .inspect_err(|e| tracing::warn!("Cannot extract bytes from item field: {e}"))
                    .map_err(|_| StatusCode::BAD_REQUEST)?,
            )
            .await;
        let res = res.map(|x| x.metadata);
        tracing::debug!("{:?}", res);
        return res.map(Json).map_err(|e| match e {
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        });
    }
    Err(StatusCode::BAD_REQUEST)
}

/// Get one feed
async fn get_one_feed<T>(
    Path(name): Path<FeedName>,
    State(server): State<Server<T>>,
) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Default,
{
    Json(server.metadata.read().await.feeds.get(&name).cloned())
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
    T: ChunkStorage + Sync + Send + Default,
{
    let hash = Hash::from_str(hash.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?;
    server
        .storage
        .read()
        .await
        .get(&hash)
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

#[derive(Deserialize, Serialize)]
struct TransferGetObj {
    got: String,
}

/// Download data associated with an hash-tree from its root
async fn get_metadata<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Default,
{
    let metadata = (*server.metadata.read().await).clone();
    Json(ServerMetadata::from(metadata))
}

/// Create a new `axum::Router` with all the routes
pub fn make_app<T>(server: RawServer<T>) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr>
where
    T: ChunkStorage + Sync + Send + Default + Debug + 'static,
{
    Router::new()
        .route("/", get(version))
        .route("/version", get(version))
        .route("/clients", get(get_clients).post(register_client))
        .route("/clients/:uuid", get(get_one_client))
        .route("/items/all", get(get_items))
        .route("/items", get(get_one_item).post(publish_item))
        .route("/chunks", get(get_chunks))
        .route("/chunks/size-sum", get(get_chunks_size_sum))
        .route("/chunks/get/:hash", get(get_chunk))
        .route("/feeds", get(get_feeds))
        .route("/feeds/:feed_name", get(get_one_feed))
        .route("/metadata", get(get_metadata))
        .with_state(Arc::new(server))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024 * 48))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_request(DefaultOnRequest::new().level(tracing::Level::TRACE))
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::INFO)
                        .latency_unit(LatencyUnit::Micros),
                ),
        )
        .into_make_service_with_connect_info::<SocketAddr>()
}
