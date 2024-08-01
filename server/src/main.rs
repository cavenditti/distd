use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::SystemTime;

use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
use distd_core::chunk_storage::ChunkStorage;
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
struct Server {
    key_pair: Ed25519KeyPair,  // needs server restart to be changed
    uuid_nonce: String,        // needs server restart to be changed
    global_metadata: Metadata, // needs server restart to be changed
    feeds: RwLock<HashMap<FeedName, Feed>>,
    clients: RwLock<BTreeMap<Uuid, Client>>,
    storage: Box<dyn ChunkStorage + Sync + Send>,
    version: Version,
}

impl Default for Server {
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
            storage: Box::new(HashMapStorage::default()),
            version: *VERSION,
        }
    }
}

#[derive(Debug)]
struct RegisterError;

impl Server {
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
            storage: Box::new(HashMapStorage::default()),
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
        let clients_lock = self.clients.write();
        match clients_lock {
            Ok(mut clients) => {
                clients.insert(client.uuid, client);
                Ok(uuid)
            }
            Err(_) => Err(RegisterError),
        }
    }

    pub fn expose_feed(&self, feed: Feed) -> Result<FeedName, RegisterError> {
        let feeds_lock = self.feeds.write();
        match feeds_lock {
            Ok(mut feeds) => {
                let name = feed.name.clone();
                feeds.insert(name.clone(), feed);
                Ok(name)
            }
            Err(_) => Err(RegisterError),
        }
    }
}

mod rest_api {
    use distd_core::version::Version;
    use serde::{Deserialize, Serialize};
    use std::{net::SocketAddr, str::FromStr, sync::Arc};
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

    type Server = Arc<RawServer>;

    use crate::utils::serde::empty_string_as_none;

    #[derive(Deserialize, Serialize)]
    struct ClientPostObj {
        #[serde(default, deserialize_with = "empty_string_as_none")]
        pub version: Option<Version>,
        pub name: String,
        //pub realm: Option<Realm>,
    }

    async fn register_client(
        ConnectInfo(addr): ConnectInfo<SocketAddr>,
        Query(client): Query<ClientPostObj>,
        State(server): State<Server>,
    ) -> Result<impl IntoResponse, StatusCode> {
        match server.register_client(client.name, addr, client.version) {
            Ok(client_uuid) => Ok(client_uuid.to_string()),
            Err(_) => Err(StatusCode::FORBIDDEN),
        }
    }

    async fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    async fn get_clients(State(server): State<Server>) -> impl IntoResponse {
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
    async fn get_one_client(
        Path(uuid): Path<String>,
        State(server): State<Server>,
    ) -> Result<Json<Client>, StatusCode> {
        match Uuid::from_str(&uuid)
            .ok()
            .and_then(|x| server.clients.read().unwrap().get(&x).cloned())
        {
            Some(x) => Ok(Json(x)),
            _ => Err(StatusCode::NOT_FOUND),
        }
    }

    use crate::Feed;
    async fn get_feeds(State(server): State<Server>) -> impl IntoResponse {
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

    async fn get_one_feed(
        Path(feed_name): Path<FeedName>,
        State(server): State<Server>,
    ) -> Result<impl IntoResponse, StatusCode> {
        match server.feeds.read().unwrap().get(&feed_name) {
            Some(feed) => Ok(Json(feed.clone())),
            None => Err(StatusCode::NOT_FOUND),
        }
    }

    pub fn make_app(server: RawServer) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr> {
        Router::new()
            .route("/", get(version))
            .route("/version", get(version))
            .route("/clients", get(get_clients).post(register_client))
            .route("/clients/:uuid", get(get_one_client))
            .route("/feeds", get(get_feeds))
            .route("/feeds/:feed_name", get(get_one_feed))
            .with_state(Arc::new(server))
            .into_make_service_with_connect_info::<SocketAddr>()
    }
}

#[tokio::main]
async fn main() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    let server = Server::default();
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
