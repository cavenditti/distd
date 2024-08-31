use std::{fmt::Debug, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use bitcode;
use blake3::Hash;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use axum::{
    body::Body,
    extract::{connect_info::IntoMakeServiceWithConnectInfo, ConnectInfo, Path, Query, State},
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

use distd_core::utils::serde::empty_string_as_none;
use distd_core::{
    chunks::OwnedHashTreeNode, metadata::ServerMetadata, utils::serde::bitcode::BitcodeSerializable,
};
use distd_core::{item::ItemName, version::Version};

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

use crate::ChunkStorage;
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

async fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

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

use crate::Client;
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

async fn get_chunks_size_sum<T>(State(server): State<Server<T>>) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    Json(server.storage.size())
}

use crate::Feed;
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
            .collect::<Vec<String>>(),
    )
}

async fn get_one_item<T>(
    Path(name): Path<ItemName>,
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
            .get(&name)
            .cloned(),
    )
}

#[derive(Deserialize, Serialize)]
struct ItemPostObj {
    //pub name: ItemName,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub description: Option<String>,
    pub path: PathBuf,
}

use axum::extract::Multipart;
async fn publish_item<T>(
    Path(name): Path<ItemName>,
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
            name,
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

async fn get_chunk<T>(
    Path(hash): Path<String>,
    State(server): State<Server<T>>,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    let hash = Hash::from_str(hash.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(server.storage.get(&hash)))
}

/// Download data associated with an hash-tree from its root
async fn get_transfer<T>(
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
        .and_then(|stored_chunk_ref| {
            Some(OwnedHashTreeNode::from((*stored_chunk_ref).clone()))
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
        })
        .and_then(|packed| {
            bitcode::serialize(&packed).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .map(Body::from)
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

pub fn make_app<T>(server: RawServer<T>) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr>
where
    T: ChunkStorage + Sync + Send + Clone + Default + Debug + 'static,
{
    Router::new()
        .route("/", get(version))
        .route("/version", get(version))
        .route("/clients", get(get_clients).post(register_client))
        .route("/clients/:uuid", get(get_one_client))
        .route("/items", get(get_items))
        .route("/items/:name", get(get_one_item).post(publish_item))
        .route("/chunks", get(get_chunks))
        .route("/chunks/size-sum", get(get_chunks_size_sum))
        .route("/chunks/get/:hash", get(get_chunk))
        .route("/feeds", get(get_feeds))
        .route("/feeds/:feed_name", get(get_one_feed))
        // 'transfer' routes return binary bitcode serialized bodies
        .route("/transfer/metadata", get(get_metadata))
        .route("/transfer/:hash", get(get_transfer))
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
