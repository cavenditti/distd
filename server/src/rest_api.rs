use std::{fmt::Debug, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use axum_extra::extract::OptionalQuery;
use bitcode;
use blake3::Hash;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use axum::{
    body::Body,
    extract::{
        connect_info::IntoMakeServiceWithConnectInfo, ConnectInfo, Multipart, Path, Query, State,
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

use distd_core::{chunk_storage::StoredChunkRef, utils::serde::empty_string_as_none};
use distd_core::{
    chunks::OwnedHashTreeNode, metadata::ServerMetadata, utils::serde::bitcode::BitcodeSerializable,
};
use distd_core::{item::ItemName, version::Version};

use crate::ChunkStorage;
use crate::Client;
use crate::Feed;
use crate::Server as RawServer;
use crate::{error::ServerError, FeedName};

type Server<T> = Arc<RawServer<T>>;

#[derive(Deserialize, Serialize)]
struct ClientPostObj {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub version: Option<Version>,
    pub name: String,
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
    T: ChunkStorage + Sync + Send + Clone + Default + Debug,
{
    server
        .register_client(client.name, addr, client.version)
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
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(
        server
            .clients
            .read()
            .expect("Poisoned Lock")
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
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Uuid::from_str(&uuid)
        .ok()
        .and_then(|uuid| {
            server
                .clients
                .read()
                .expect("Poisoned Lock")
                .get(&uuid)
                .cloned()
        })
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

/// Get all chunks
async fn get_chunks<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(
        server
            .storage
            .chunks()
            .into_iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>(),
    )
}

/// Get sum of all chunks sizes
async fn get_chunks_size_sum<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(server.storage.size())
}

/// Get all feeds
async fn get_feeds<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(
        server
            .metadata
            .read()
            .expect("Poisoned Lock")
            .feeds
            .values()
            .cloned()
            .collect::<Vec<Feed>>(),
    )
}

/// Get all items
async fn get_items<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(
        server
            .metadata
            .read()
            .expect("Poisoned Lock")
            .items
            .keys()
            .cloned()
            .map(|x| x.to_string_lossy().into())
            .collect::<Vec<String>>(),
    )
}

/// Get one item
async fn get_one_item<T>(
    Query(path): Query<PathBuf>,
    State(server): State<Server<T>>,
) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(
        server
            .metadata
            .read()
            .expect("Poisoned Lock")
            .items
            .get(&path)
            .cloned(),
    )
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
    T: ChunkStorage + Sync + Send + Clone + Default + Debug,
{
    while let Some(field) = multipart
        .next_field()
        .await
        .expect("Failed to get next field!")
    {
        if field.name().unwrap() != "item" {
            continue;
        }
        let res = server.publish_item(
            item_data.name,
            item_data.path,
            0,
            item_data.description,
            field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?,
        );
        let res = res.map(|x| x.metadata);
        tracing::debug!("{:?}", res);
        return res.map(Json).map_err(|e| match e {
            ServerError::ItemInsertionError(..) => StatusCode::NOT_IMPLEMENTED,
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
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(
        server
            .metadata
            .read()
            .expect("Poisoned Lock")
            .feeds
            .get(&name)
            .cloned(),
    )
}

/// Download data associated with an hash
/// This is a simple wrapper around `ChunkStorage::get`
///
/// # Errors
/// Returns `StatusCode::BAD_REQUEST` if the hash is not a valid `blake3::Hash`
/// Returns `StatusCode::NOT_FOUND` if the hash is not found in the storage
async fn get_chunk<T>(
    Path(hash): Path<String>,
    State(server): State<Server<T>>,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    let hash = Hash::from_str(hash.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?;
    server
        .storage
        .get(&hash)
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}

/// Download data associated with an hash-(sub-)trees, differing from the provided hashes
///
/// This endpoint is used to download a set of chunks from the server
/// The server will return a binary bitcode serialized representation of the chunks
async fn get_transfer_diff<T>(
    Path(hash): Path<String>,
    OptionalQuery(from): OptionalQuery<String>,
    State(server): State<Server<T>>,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    tracing::debug!("Transfer {hash} from {from:?}");

    let hash = Hash::from_str(hash.as_str())
        .inspect_err(|e| tracing::error!("Cannot decode hash {}", e))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // handle None `from`
    let from = match from {
        None => vec![],
        Some(from) => {
            let from: Result<Vec<Hash>, blake3::HexError> =
                from.split(',').map(Hash::from_str).collect();
            from.inspect_err(|e| tracing::error!("Cannot decode hash {}", e))
                .map_err(|_| StatusCode::BAD_REQUEST)?
        }
    };

    let hashes = server
        .storage
        .diff(&hash, from)
        .ok_or(StatusCode::NOT_FOUND)
        .inspect(|hs| tracing::debug!("Transferring chunks: {hs:?}"))
        .inspect_err(|_| tracing::warn!("Cannot find hash {hash}"))?;

    let hashes: Result<Vec<Arc<StoredChunkRef>>, StatusCode> = hashes
        .iter()
        .map(|hash| {
            server
                .storage
                .get(hash)
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
                .inspect_err(|_| tracing::error!("Missing hash provided by storage itself: {hash}"))
        }) // this should never happen
        .collect();

    let chunks: Vec<OwnedHashTreeNode> = hashes?
        .iter()
        .map(|x| OwnedHashTreeNode::from((**x).clone()))
        .collect();

    tracing::debug!("Returned chunks: {chunks:?}");

    let serialized: Result<Vec<u8>, StatusCode> = bitcode::serialize(&chunks)
        .inspect_err(|e| tracing::error!("Cannot serialize chunk {}", e))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);

    serialized.map(Body::from)
}

/// Download data associated with one or more hash-(sub-)trees from their root
///
/// This endpoint is used to download a set of chunks from the server
/// The server will return a binary bitcode serialized representation of the chunks
/// The endpoint accepts a comma-separated list of hash
///
/// # Errors
/// Returns `StatusCode::BAD_REQUEST` if some hash is not a valid `blake3::Hash`
/// Returns `StatusCode::NOT_FOUND` if some hash is not found in the storage
async fn get_transfer<T>(
    Path(hashes): Path<String>,
    State(server): State<Server<T>>,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    // Manually splitting at ',' is actually enough for this case
    let hashes: Result<Vec<Hash>, blake3::HexError> =
        hashes.split(',').map(Hash::from_str).collect();

    let hashes: Result<Vec<Arc<StoredChunkRef>>, StatusCode> = hashes
        .inspect_err(|e| tracing::error!("Cannot decode hash {}", e))
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .iter()
        .map(|hash| server.storage.get(hash).ok_or(StatusCode::NOT_FOUND))
        .collect();

    let chunks: Vec<OwnedHashTreeNode> = hashes?
        .iter()
        .map(|x| OwnedHashTreeNode::from((**x).clone()))
        .collect();

    let serialized: Result<Vec<u8>, StatusCode> = bitcode::serialize(&chunks)
        .inspect_err(|e| tracing::error!("Cannot serialize chunk {}", e))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);

    serialized.map(Body::from)
}

/// Download data associated with an hash-tree from its root
async fn get_metadata<T>(State(server): State<Server<T>>) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    let metadata = (*server.metadata.read().expect("Poisoned Lock")).clone();
    ServerMetadata::from(metadata)
        .to_bitcode()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        .map(Body::from)
}

/// Create a new `axum::Router` with all the routes
pub fn make_app<T>(server: RawServer<T>) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr>
where
    T: ChunkStorage + Sync + Send + Clone + Default + Debug + 'static,
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
        // 'transfer' routes return binary bitcode serialized bodies
        .route("/transfer/metadata", get(get_metadata))
        .route("/transfer/diff/:hash", get(get_transfer_diff))
        .route("/transfer/whole/:hashes", get(get_transfer))
        .with_state(Arc::new(server))
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
