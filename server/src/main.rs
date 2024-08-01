#![feature(map_try_insert)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::SystemTime;

use axum::body::Bytes;
use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::{self, ChunkStorage};
use distd_core::metadata::{Item, ItemName};
use distd_core::msgpack::MsgPackSerializable;
use ring::error::KeyRejected;
use ring::pkcs8::Document;
use ring::signature::Ed25519KeyPair;
use ring::{
    rand,
    signature::{self},
};
use uuid::Uuid;

use distd_core::feed::{Feed, FeedName};
use distd_core::version::{Version, VERSION};
pub mod client;
pub mod utils;
use crate::client::{Client, ClientName};

type Metadata = String; // TODO

/// distd Server
///
/// Server signature is used to check replicated data among clients when shared p2p,
/// Note that this is different from an eventual "build" signature.
struct Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    key_pair: Ed25519KeyPair,  // needs server restart to be changed
    uuid_nonce: String,        // needs server restart to be changed
    global_metadata: Metadata, // needs server restart to be changed
    feeds: RwLock<HashMap<FeedName, Feed>>,
    clients: RwLock<BTreeMap<Uuid, Client>>,
    storage: T,
    item_map: RwLock<HashMap<ItemName, Item>>,
    version: Version,
}

impl<T> Default for Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    fn default() -> Self {
        // Generate a key pair in PKCS#8 (v2) format.
        let rng = rand::SystemRandom::new();
        let pkcs8_bytes = signature::Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let uuid_nonce = blake3::hash(pkcs8_bytes.as_ref()).to_string();

        // Normally the application would store the PKCS#8 file persistently. Later
        // it would read the PKCS#8 file from persistent storage to use it.
        let key_pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();

        Self {
            key_pair,
            uuid_nonce,
            global_metadata: "".to_string(),
            feeds: RwLock::new(HashMap::<FeedName, Feed>::new()),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()),
            storage: T::default(),
            item_map: RwLock::new(HashMap::<ItemName, Item>::new()),
            version: *VERSION,
        }
    }
}

#[derive(Debug)]
struct RegisterError;

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    pub fn new(
        pkcs8_bytes: Document,
        global_metadata: Metadata,
        feeds: Vec<Feed>,
    ) -> Result<Self, KeyRejected> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref())?;
        Ok(Self {
            key_pair,
            uuid_nonce: blake3::hash(pkcs8_bytes.as_ref()).to_string(),
            global_metadata,
            feeds: RwLock::new(HashMap::<FeedName, Feed>::from_iter(
                feeds.into_iter().map(|x| (x.name.clone(), x)),
            )),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()), // TODO save and reload from disk
            storage: T::default(),
            item_map: RwLock::new(HashMap::<ItemName, Item>::new()),
            version: *VERSION,
        })
    }

    pub fn register_client(
        &self,
        name: ClientName,
        addr: SocketAddr,
        version: Option<Version>,
    ) -> Result<Uuid, RegisterError> {
        let nonced_name = name.clone() + &self.uuid_nonce;
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, &nonced_name.as_bytes());
        let client = Client {
            addr,
            name,
            uuid,
            version,
            last_heartbeat: SystemTime::now(),
        };
        self.clients
            .write()
            .ok()
            .and_then(|mut clients| clients.try_insert(client.uuid, client).cloned().ok())
            .map(|client| client.uuid)
            .ok_or(RegisterError)
    }

    pub fn expose_feed(&self, feed: Feed) -> Result<FeedName, RegisterError> {
        self.feeds
            .write()
            .ok()
            .and_then(|mut feeds| feeds.try_insert(feed.name.clone(), feed).ok().cloned())
            .map(|feed| feed.name.clone())
            .ok_or(RegisterError)
    }

    pub fn publish_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: Bytes,
    ) -> Result<String, RegisterError> {
        Item::new(
            name,
            path,
            revision,
            description,
            file,
            self.storage.clone(),
        )
        .map(|item| item.name)
        .map_err(|_| RegisterError)
    }
}

mod rest_api {
    use distd_core::{metadata::ItemName, version::Version};
    use serde::{Deserialize, Serialize};
    use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
    use uuid::Uuid;

    use axum::{
        extract::{connect_info::IntoMakeServiceWithConnectInfo, ConnectInfo, Path, Query, State},
        http::StatusCode,
        response::IntoResponse,
        routing::{get, post},
        Json, Router,
    };

    use crate::FeedName;
    use crate::Server as RawServer;

    type Server<T> = Arc<RawServer<T>>;

    use crate::utils::serde::empty_string_as_none;

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
        T: ChunkStorage + Sync + Send + Clone + Default,
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
                .unwrap()
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
            .and_then(|uuid| server.clients.read().unwrap().get(&uuid).cloned())
            .ok_or_else(|| StatusCode::NOT_FOUND)
            .map(|client| Json(client))
    }

    use crate::Feed;
    async fn get_feeds<T>(State(server): State<Server<T>>) -> impl IntoResponse
    where
        T: ChunkStorage + Sync + Send + Clone + Default,
    {
        Json(
            server
                .feeds
                .read()
                .unwrap()
                .values()
                .into_iter()
                .cloned()
                .collect::<Vec<Feed>>(),
        )
    }

    async fn get_items<T>(State(server): State<Server<T>>) -> Result<impl IntoResponse, StatusCode>
    where
        T: ChunkStorage + Sync + Send + Clone + Default,
    {
        server
            .item_map
            .read()
            .map(|x| Json(x.keys().cloned().collect::<Vec<String>>()))
            .ok()
            .ok_or(StatusCode::NOT_FOUND)
    }

    async fn get_one_item<T>(
        Path(name): Path<ItemName>,
        State(server): State<Server<T>>,
    ) -> Result<impl IntoResponse, StatusCode>
    where
        T: ChunkStorage + Sync + Send + Clone + Default,
    {
        server
            .item_map
            .read()
            .unwrap()
            .get(&name)
            .cloned()
            .map(|x| Json(x))
            .ok_or( StatusCode::NOT_FOUND)
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
        T: ChunkStorage + Sync + Send + Clone + Default,
    {
        while let Some(field) = multipart
            .next_field()
            .await
            .expect("Failed to get next field!")
        {
            if field.name().unwrap() != "fileupload" {
                continue;
            }
            return server
                .publish_item(
                    name,
                    item_data.path,
                    0,
                    item_data.description,
                    field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?,
                )
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
        }
        Err(StatusCode::BAD_REQUEST)
    }

    async fn get_one_feed<T>(
        Path(feed_name): Path<FeedName>,
        State(server): State<Server<T>>,
    ) -> Result<impl IntoResponse, StatusCode>
    where
        T: ChunkStorage + Sync + Send + Clone + Default,
    {
        server
            .feeds
            .read()
            .unwrap()
            .get(&feed_name)
            .map(|feed| Json(feed.clone()))
            .ok_or_else(|| StatusCode::NOT_FOUND)
    }

    pub fn make_app<T>(server: RawServer<T>) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr>
    where
        T: ChunkStorage + Sync + Send + Clone + Default + 'static,
    {
        Router::new()
            .route("/", get(version))
            .route("/version", get(version))
            .route("/clients", get(get_clients).post(register_client))
            .route("/clients/:uuid", get(get_one_client))
            .route("/items", get(get_items))
            .route("/items/:name", get(get_one_item).post(publish_item))
            .route("/feeds", get(get_feeds))
            .route("/feeds/:feed_name", get(get_one_feed))
            .with_state(Arc::new(server))
            .into_make_service_with_connect_info::<SocketAddr>()
    }
}

#[tokio::main]
async fn main() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    let server: Server<HashMapStorage> = Server::default();
    let feed = Feed::new("A feed");
    let buf = feed.to_msgpack().unwrap();
    println!("Feed is {} bytes", buf.len());
    let feed = Feed::from_msgpack(buf).unwrap();

    println!("{:?}", feed.name);
    server.expose_feed(feed);

    let app = rest_api::make_app(server);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
