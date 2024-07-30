use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use arrayvec::ArrayString;
use blake3::Hash;
use ring::error::KeyRejected;
use ring::pkcs8::Document;
use ring::signature::Ed25519KeyPair;
use ring::{
    rand,
    signature::{self},
};
use uuid::Uuid;
use warp::Filter;

use crate::utils::url_part_utf8_string;
use distd_core::metadata::{Item, RawChunk};
use distd_core::version::{Version, VERSION};
pub mod utils;

// TODO define the Storage trait
pub trait ChunkStorage {
    fn get(self, hash: &Hash) -> Option<RawChunk>;
    fn insert(self, chunk: RawChunk) -> Option<Hash>;
    //fn drop(hash: Hash); // ??
}

// Dead simple in-memory global storage
#[derive(Default)]
struct SimpleStorage {
    // This re-hashes the hashes, but nicely handles collisions in return
    // TODO we may use a Hasher that just returns the first n bytes of the SHA-256?
    data: RwLock<HashMap<Hash, RawChunk>>,
}

impl ChunkStorage for SimpleStorage {
    fn get(self, hash: &Hash) -> Option<RawChunk> {
        let data = match self.data.read() {
            Ok(data) => data,
            Err(_) => return None,
        };
        data.get(hash).copied()
    }

    fn insert(self, chunk: RawChunk) -> Option<Hash> {
        let mut data = match self.data.write() {
            Ok(data) => data,
            Err(_) => return None,
        };
        let hash = blake3::hash(&chunk);
        match data.insert(hash, chunk) {
            Some(_) => Some(hash),
            None => None,
        }
    }
}

type FeedName = ArrayString<256>;

#[derive(Debug)]
struct Feed {
    name: FeedName,
    paths: BTreeMap<PathBuf, Item>,
}

impl Feed {
    pub fn new(name: &str) -> Self {
        Self {
            name: ArrayString::from_str(&name).unwrap(),
            paths: BTreeMap::new(),
        }
    }
}

type Metadata = String; // TODO

/// distd Server
///
/// Server signature is used to check replicated data among clients when shared p2p,
/// Note that this is different from an eventual "build" signature.
struct Server {
    key_pair: Ed25519KeyPair,  // needs server restart to be changed
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

        // Normally the application would store the PKCS#8 file persistently. Later
        // it would read the PKCS#8 file from persistent storage to use it.
        let key_pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();

        Self {
            key_pair,
            global_metadata: "".to_string(),
            feeds: RwLock::new(HashMap::<FeedName, Feed>::new()),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()),
            storage: Box::new(SimpleStorage::default()),
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
            global_metadata,
            feeds: RwLock::new(HashMap::<FeedName, Feed>::from_iter(
                feeds.into_iter().map(|x| (x.name, x)),
            )),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()), // TODO save and reload from disk
            storage: Box::new(SimpleStorage::default()),
            version: *VERSION,
        })
    }

    pub fn register_client(
        &self,
        name: ArrayString<256>,
        addr: SocketAddr,
    ) -> Result<Uuid, RegisterError> {
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes());
        let client = Client {
            ip: addr.ip(),
            name,
            uuid,
            version: None,
            last_heartbeat: Instant::now(),
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
                let name = feed.name;
                feeds.insert(name, feed);
                Ok(name)
            }
            Err(_) => Err(RegisterError),
        }
    }
}

#[derive(Debug)]
struct Client {
    name: ArrayString<256>,
    ip: IpAddr,
    uuid: Uuid,
    //realm: Option<Arc<Realm>>,
    version: Option<Version>,
    last_heartbeat: Instant,
}

impl PartialEq for Client {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}
impl PartialOrd for Client {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.uuid.partial_cmp(&other.uuid)
    }
}

mod handlers {
    use arrayvec::ArrayString;
    use std::{convert::Infallible, net::SocketAddr, sync::Arc};
    use warp::http::StatusCode;
    use warp::reply::{json, with_status};

    use crate::Server as RawServer;

    type Server = Arc<RawServer>;

    pub async fn register_client(
        addr: Option<SocketAddr>,
        client_post_obj: crate::filters::ClientPostObj,
        server: Server,
    ) -> Result<impl warp::Reply, Infallible> {
        let name = ArrayString::<256>::from(&client_post_obj.name);
        if name.is_err() || addr.is_none() {
            return Ok(with_status(json(&()), StatusCode::BAD_REQUEST));
        };
        match server.register_client(name.unwrap(), addr.unwrap()) {
            Ok(client_uuid) => Ok(with_status(json(&client_uuid.to_string()), StatusCode::OK)),
            Err(_) => Ok(with_status(json(&()), StatusCode::INTERNAL_SERVER_ERROR)),
        }
    }
}

mod filters {
    use arrayvec::ArrayString;
    use serde::{Deserialize, Serialize};

    use std::sync::Arc;
    use warp::Filter;

    use crate::{handlers, Server as RawServer};

    type Server = Arc<RawServer>;

    fn with_server(
        server: Server,
    ) -> impl Filter<Extract = (Server,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || server.clone())
    }

    // GET /version
    pub fn version() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
    {
        warp::path!("version").map(|| "distd v0.1.0")
    }

    // GET /clients
    pub fn get_clients(
        server: Server,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("clients")
            .and(warp::get())
            .and(with_server(server))
            .map(|server: Server| format!("clients: {:?}", server.clone().clients.read().unwrap()))
    }

    #[derive(Deserialize, Serialize)]
    pub struct ClientPostObj {
        pub name: String,
    }

    // POST /clients
    pub fn register_client(
        server: Server,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("clients")
            .and(warp::post())
            .and(warp::addr::remote())
            .and(warp::query::<ClientPostObj>())
            .and(with_server(server))
            .and_then(handlers::register_client)
    }

    // GET /feeds
    pub fn get_feeds(
        server: Server,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("feeds")
            .and(with_server(server))
            .map(|server: Server| format!("clients: {:?}", server.clone().feeds.read().unwrap()))
    }

    use crate::url_part_utf8_string::UrlPartUtf8String;

    // GET /feeds/<feed name>
    pub fn get_one_feed(
        // TODO
        server: Server,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("feeds" / UrlPartUtf8String)
            .and(with_server(server))
            .map(|feed_name: UrlPartUtf8String, server: Server| {
                let feed_name = feed_name.to_string();
                let feed_name = ArrayString::<256>::from(&feed_name);
                match feed_name {
                    Ok(feed_name) => match server
                        .feeds
                        .read()
                        .unwrap()
                        .get::<ArrayString<256>>(&feed_name)
                    {
                        Some(feed) => format!("feeds: {:?}", feed),
                        None => "{}".to_owned(),
                    },
                    Err(e) => format!("Cannot parse feed name: {:?}", e),
                }
            })
    }

    pub fn routes(
        server: Server,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        version()
            .or(register_client(server.clone()))
            .or(get_clients(server.clone()))
            .or(get_feeds(server.clone()))
            .or(get_one_feed(server.clone()))
    }
}

#[tokio::main]
async fn main() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    let server = Server::default();
    let feed = Feed::new("A feed");
    println!("{:?}", feed.name);
    server.expose_feed(feed);

    let hello = warp::path!("hello" / String).map(|name| format!("Hello, {}!", name));

    warp::serve(hello.or(filters::routes(Arc::new(server))))
        .run(([127, 0, 0, 1], 3000))
        .await;
}
