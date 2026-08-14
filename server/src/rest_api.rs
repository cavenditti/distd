use std::{
    collections::HashSet, fmt::Debug, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use axum::{
    body::Body,
    extract::{
        connect_info::IntoMakeServiceWithConnectInfo, ConnectInfo, DefaultBodyLimit, Extension,
        Multipart, Path, Query, State,
    },
    http::{header::CONTENT_LENGTH, HeaderMap, HeaderValue, Method, Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::get, //, post},
    Json,
    Router,
};
use tokio::sync::Semaphore;
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
    signature::{
        PublisherPublicKey, PublisherReleaseEnvelope, PublisherReleaseSequence, PublisherSignature,
        PublisherSignatureEnvelope,
    },
    utils::path::validate_relative,
    utils::serde::empty_string_as_none,
    version::Version,
};

use crate::error::Server as ServerError;
use crate::publication::{PublicationError, PublicationRequestAttestation};
use crate::Client;
use crate::Server as RawServer;

type Server<T> = Arc<RawServer<T>>;

const PUBLISHER_VERSION_HEADER: &str = "x-distd-publisher-statement-version";
const PUBLISHER_KEY_HEADER: &str = "x-distd-publisher-public-key";
const PUBLISHER_SIGNATURE_HEADER: &str = "x-distd-publisher-signature";
const PUBLISHER_SEQUENCE_HEADER: &str = "x-distd-publisher-release-sequence";
const MAX_PUBLICATION_BODY_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
struct PublicationAdmission {
    permits: Arc<Semaphore>,
    max_bytes: u32,
    body_timeout: Duration,
}

impl PublicationAdmission {
    fn new(max_bytes: usize, body_timeout: Duration) -> Result<Self, &'static str> {
        let max_bytes = u32::try_from(max_bytes)
            .map_err(|_| "publication memory budget exceeds semaphore capacity")?;
        if max_bytes == 0 {
            return Err("publication memory budget must be nonzero");
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(max_bytes as usize)),
            max_bytes,
            body_timeout,
        })
    }

    fn declared_bytes(&self, headers: &HeaderMap) -> Result<u32, StatusCode> {
        let value = singleton_header(headers, CONTENT_LENGTH.as_str())?
            .ok_or(StatusCode::LENGTH_REQUIRED)?
            .to_str()
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let declared = value.parse::<u64>().map_err(|_| StatusCode::BAD_REQUEST)?;
        if declared > u64::from(self.max_bytes) {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        u32::try_from(declared).map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)
    }

    #[cfg(test)]
    fn available_bytes(&self) -> usize {
        self.permits.available_permits()
    }
}

#[derive(Clone, Copy, Debug)]
struct PublicationBodyDeadline(tokio::time::Instant);

fn singleton_header<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<Option<&'a HeaderValue>, StatusCode> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(first)
}

fn decode_lower_hex<const N: usize>(value: &HeaderValue) -> Result<[u8; N], StatusCode> {
    let value = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut decoded = [0u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |byte: u8| match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => unreachable!("validated lowercase hexadecimal"),
        };
        decoded[index] = nibble(pair[0]) << 4 | nibble(pair[1]);
    }
    Ok(decoded)
}

fn publication_envelope(headers: &HeaderMap) -> Result<PublicationRequestAttestation, StatusCode> {
    let version = singleton_header(headers, PUBLISHER_VERSION_HEADER)?;
    let key = singleton_header(headers, PUBLISHER_KEY_HEADER)?;
    let signature = singleton_header(headers, PUBLISHER_SIGNATURE_HEADER)?;
    let sequence = singleton_header(headers, PUBLISHER_SEQUENCE_HEADER)?;
    match (version, key, signature, sequence) {
        (None, None, None, None) => Ok(PublicationRequestAttestation::Unsigned),
        (Some(version), Some(key), Some(signature), None) if version.as_bytes() == b"1" => Ok(
            PublicationRequestAttestation::ReplayableV1(PublisherSignatureEnvelope::from_parts(
                PublisherPublicKey::from_bytes(decode_lower_hex(key)?),
                PublisherSignature::from_bytes(decode_lower_hex(signature)?),
            )),
        ),
        (Some(version), Some(key), Some(signature), Some(sequence))
            if version.as_bytes() == b"2" =>
        {
            let sequence = sequence.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
            if sequence.is_empty()
                || (sequence.len() > 1 && sequence.starts_with('0'))
                || !sequence.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(StatusCode::BAD_REQUEST);
            }
            Ok(PublicationRequestAttestation::ReleaseV2 {
                sequence: PublisherReleaseSequence::new(
                    sequence.parse().map_err(|_| StatusCode::BAD_REQUEST)?,
                ),
                envelope: PublisherReleaseEnvelope::from_parts(
                    PublisherPublicKey::from_bytes(decode_lower_hex(key)?),
                    PublisherSignature::from_bytes(decode_lower_hex(signature)?),
                ),
            })
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

fn publication_error_status(error: &ServerError) -> StatusCode {
    match error {
        ServerError::Publication(PublicationError::UntrustedPublisher)
        | ServerError::Publication(PublicationError::InvalidSignature(_)) => StatusCode::FORBIDDEN,
        ServerError::Publication(PublicationError::AttestationConflict) => StatusCode::CONFLICT,
        ServerError::Publication(
            PublicationError::UnsignedForbidden | PublicationError::ReplayableForbidden,
        ) => StatusCode::BAD_REQUEST,
        ServerError::State(message) if message.contains("read-only until restart") => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

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

async fn authorize_publisher_headers<T>(
    State(server): State<Server<T>>,
    request: Request<Body>,
    next: Next,
) -> impl IntoResponse
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    if request.method() == Method::POST && request.uri().path() == "/items" {
        let envelope = match publication_envelope(request.headers()) {
            Ok(envelope) => envelope,
            Err(status) => return status.into_response(),
        };
        if let Err(error) = server.precheck_publication_attestation(&envelope) {
            return publication_error_status(&error).into_response();
        }
    }
    next.run(request).await
}

async fn admit_publication_body(
    State(admission): State<PublicationAdmission>,
    mut request: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    if request.method() != Method::POST || request.uri().path() != "/items" {
        return next.run(request).await;
    }
    let declared = match admission.declared_bytes(request.headers()) {
        Ok(declared) => declared,
        Err(status) => return status.into_response(),
    };
    let permit = match Arc::clone(&admission.permits).try_acquire_many_owned(declared) {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    request.extensions_mut().insert(PublicationBodyDeadline(
        tokio::time::Instant::now() + admission.body_timeout,
    ));
    let response = next.run(request).await;
    drop(permit);
    response
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
        metadata
            .items
            .get(aid)
            .map(|revision| revision.item().clone())
    } else if let Some(ref path) = item.path {
        metadata
            .items
            .values()
            .find(|revision| &revision.item().metadata.path == path)
            .map(|revision| revision.item().clone())
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

async fn collect_uploaded_files(
    mut multipart: Multipart,
) -> Result<Vec<(PathBuf, axum::body::Bytes)>, StatusCode> {
    let mut uploaded_files = Vec::new();
    let mut uploaded_paths = HashSet::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| error.status())?
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
            .inspect_err(|_| {
                tracing::warn!(
                    phase = "multipart_body",
                    "publication body extraction failed"
                );
            })
            .map_err(|error| error.status())?;
        uploaded_files.push((file_name, data));
    }

    Ok(uploaded_files)
}

/// Publish an item
async fn publish_item<T>(
    headers: HeaderMap,
    Query(item_data): Query<ItemPostObj>,
    State(server): State<Server<T>>,
    Extension(body_deadline): Extension<PublicationBodyDeadline>,
    multipart: Multipart,
) -> Result<impl IntoResponse, StatusCode>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    let envelope = publication_envelope(&headers)?;
    let destination = validate_relative(&item_data.path).map_err(|_| StatusCode::BAD_REQUEST)?;
    let artifact_id = item_data
        .artifact_id
        .clone()
        .unwrap_or_else(|| item_data.name.clone());
    if artifact_id.is_empty() || artifact_id.len() > 256 || item_data.name.len() > 256 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut uploaded_files =
        tokio::time::timeout_at(body_deadline.0, collect_uploaded_files(multipart))
            .await
            .map_err(|_| StatusCode::REQUEST_TIMEOUT)??;

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
        match envelope {
            PublicationRequestAttestation::ReleaseV2 { sequence, envelope } => {
                server
                    .publish_release_item_as(
                        artifact_id,
                        item_data.name,
                        destination,
                        item_data.description,
                        file,
                        chunk_algorithm,
                        sequence,
                        envelope,
                    )
                    .await
            }
            PublicationRequestAttestation::ReplayableV1(envelope) => {
                server
                    .publish_signed_item_as(
                        artifact_id,
                        item_data.name,
                        destination,
                        item_data.description,
                        file,
                        chunk_algorithm,
                        envelope,
                    )
                    .await
            }
            PublicationRequestAttestation::Unsigned => {
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
            }
        }
    } else {
        match envelope {
            PublicationRequestAttestation::ReleaseV2 { sequence, envelope } => {
                server
                    .publish_release_item_from_files_as(
                        artifact_id,
                        item_data.name,
                        destination,
                        item_data.description,
                        uploaded_files,
                        chunk_algorithm,
                        sequence,
                        envelope,
                    )
                    .await
            }
            PublicationRequestAttestation::ReplayableV1(envelope) => {
                server
                    .publish_signed_item_from_files_as(
                        artifact_id,
                        item_data.name,
                        destination,
                        item_data.description,
                        uploaded_files,
                        chunk_algorithm,
                        envelope,
                    )
                    .await
            }
            PublicationRequestAttestation::Unsigned => {
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
            }
        }
    };
    let result = result.map(|item| item.metadata);
    tracing::debug!("{:?}", result);
    result
        .map(Json)
        .map_err(|error| publication_error_status(&error))
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
fn make_router_with_admission<T>(
    server: RawServer<T>,
    body_limit: usize,
    admission: PublicationAdmission,
) -> Router
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    let server = Arc::new(server);
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
        .with_state(Arc::clone(&server))
        .layer(middleware::from_fn_with_state(
            admission,
            admit_publication_body,
        ))
        .layer(middleware::from_fn_with_state(
            server,
            authorize_publisher_headers::<T>,
        ))
        .layer(middleware::from_fn(authorize_rest))
        .layer(DefaultBodyLimit::max(body_limit))
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
}

fn make_router_with_limits<T>(
    server: RawServer<T>,
    body_limit: usize,
    publication_memory_bytes: usize,
    publication_body_timeout: Duration,
) -> Router
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    make_router_with_admission(
        server,
        body_limit,
        PublicationAdmission::new(publication_memory_bytes, publication_body_timeout)
            .expect("validated publication memory budget"),
    )
}

/// Create a new `axum::Router` with all the routes
pub fn make_app<T>(
    server: RawServer<T>,
    publication_memory_bytes: usize,
    publication_body_timeout: Duration,
) -> IntoMakeServiceWithConnectInfo<Router, SocketAddr>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    make_router_with_limits(
        server,
        MAX_PUBLICATION_BODY_BYTES,
        publication_memory_bytes,
        publication_body_timeout,
    )
    .into_make_service_with_connect_info::<SocketAddr>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InternalMetadata;
    use axum::body::Bytes;
    use distd_core::{
        chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
        chunks::ChunkAlgorithm,
        signature::{
            PublisherPublicKey, PublisherReleaseEnvelope, PublisherReleaseSequence,
            PublisherReleaseStatement, PublisherSignature,
        },
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tokio_stream::wrappers::ReceiverStream;
    use tower::ServiceExt;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct PublishTokenGuard(Option<std::ffi::OsString>);

    impl PublishTokenGuard {
        fn install(value: &str) -> Self {
            let previous = std::env::var_os("DISTD_PUBLISH_TOKEN");
            std::env::set_var("DISTD_PUBLISH_TOKEN", value);
            Self(previous)
        }
    }

    impl Drop for PublishTokenGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.0.take() {
                std::env::set_var("DISTD_PUBLISH_TOKEN", previous);
            } else {
                std::env::remove_var("DISTD_PUBLISH_TOKEN");
            }
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn multipart_body(payload: &[u8]) -> Vec<u8> {
        let mut body = b"--distd-test\r\nContent-Disposition: form-data; name=\"item\"; filename=\"item.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n".to_vec();
        body.extend_from_slice(payload);
        body.extend_from_slice(b"\r\n--distd-test--\r\n");
        body
    }

    fn request_with_body(
        body: Body,
        content_length: usize,
        bearer: Option<&str>,
        key: [u8; 32],
    ) -> Request<Body> {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/items?artifact_id=route-test&name=route-test&path=route.bin&chunking=fixed")
            .header(
                axum::http::header::CONTENT_TYPE,
                "multipart/form-data; boundary=distd-test",
            )
            .header(CONTENT_LENGTH, content_length.to_string())
            .header(PUBLISHER_VERSION_HEADER, "2")
            .header(PUBLISHER_SEQUENCE_HEADER, "1")
            .header(PUBLISHER_KEY_HEADER, hex(&key))
            .header(PUBLISHER_SIGNATURE_HEADER, "00".repeat(64));
        if let Some(bearer) = bearer {
            request = request.header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {bearer}"),
            );
        }
        request.body(body).unwrap()
    }

    fn request(body: Vec<u8>, bearer: Option<&str>, key: [u8; 32]) -> Request<Body> {
        let content_length = body.len();
        request_with_body(Body::from(body), content_length, bearer, key)
    }

    fn test_admission(max_bytes: usize) -> PublicationAdmission {
        PublicationAdmission::new(max_bytes, Duration::from_secs(60)).unwrap()
    }

    fn signed_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(PUBLISHER_VERSION_HEADER, HeaderValue::from_static("1"));
        headers.insert(
            PUBLISHER_KEY_HEADER,
            HeaderValue::from_str(&"12".repeat(32)).unwrap(),
        );
        headers.insert(
            PUBLISHER_SIGNATURE_HEADER,
            HeaderValue::from_str(&"34".repeat(64)).unwrap(),
        );
        headers
    }

    fn release_headers(sequence: &str) -> HeaderMap {
        let mut headers = signed_headers();
        headers.insert(PUBLISHER_VERSION_HEADER, HeaderValue::from_static("2"));
        headers.insert(
            PUBLISHER_SEQUENCE_HEADER,
            HeaderValue::from_str(sequence).unwrap(),
        );
        headers
    }

    #[test]
    fn publication_headers_are_exact_singletons() {
        let headers = signed_headers();
        let PublicationRequestAttestation::ReplayableV1(envelope) =
            publication_envelope(&headers).expect("valid headers")
        else {
            panic!("expected a V1 compatibility envelope");
        };
        assert_eq!(envelope.public_key().as_bytes(), &[0x12; 32]);
        assert_eq!(envelope.signature().as_bytes(), &[0x34; 64]);
        assert_eq!(
            publication_envelope(&HeaderMap::new()),
            Ok(PublicationRequestAttestation::Unsigned)
        );
        let PublicationRequestAttestation::ReleaseV2 { sequence, envelope } =
            publication_envelope(&release_headers("18446744073709551615"))
                .expect("valid V2 release headers")
        else {
            panic!("expected a V2 release envelope");
        };
        assert_eq!(sequence.get(), u64::MAX);
        assert_eq!(envelope.public_key().as_bytes(), &[0x12; 32]);

        for name in [
            PUBLISHER_VERSION_HEADER,
            PUBLISHER_KEY_HEADER,
            PUBLISHER_SIGNATURE_HEADER,
        ] {
            let mut missing = signed_headers();
            missing.remove(name);
            assert_eq!(publication_envelope(&missing), Err(StatusCode::BAD_REQUEST));

            let mut duplicate = signed_headers();
            duplicate.append(name, HeaderValue::from_static("1"));
            assert_eq!(
                publication_envelope(&duplicate),
                Err(StatusCode::BAD_REQUEST)
            );
        }
    }

    #[test]
    fn publication_headers_reject_aliases_and_malformed_values() {
        for version in ["0", "01", "2", "+1", " 1"] {
            let mut headers = signed_headers();
            headers.insert(
                PUBLISHER_VERSION_HEADER,
                HeaderValue::from_str(version).unwrap(),
            );
            assert_eq!(publication_envelope(&headers), Err(StatusCode::BAD_REQUEST));
        }

        for key in ["12".repeat(31), "12".repeat(33), "AB".repeat(32)] {
            let mut headers = signed_headers();
            headers.insert(PUBLISHER_KEY_HEADER, HeaderValue::from_str(&key).unwrap());
            assert_eq!(publication_envelope(&headers), Err(StatusCode::BAD_REQUEST));
        }
        for signature in ["34".repeat(63), "34".repeat(65), "CD".repeat(64)] {
            let mut headers = signed_headers();
            headers.insert(
                PUBLISHER_SIGNATURE_HEADER,
                HeaderValue::from_str(&signature).unwrap(),
            );
            assert_eq!(publication_envelope(&headers), Err(StatusCode::BAD_REQUEST));
        }
        for sequence in ["", "00", "01", "+1", " 1", "18446744073709551616"] {
            let mut headers = release_headers("1");
            if sequence.is_empty() {
                headers.remove(PUBLISHER_SEQUENCE_HEADER);
            } else {
                headers.insert(
                    PUBLISHER_SEQUENCE_HEADER,
                    HeaderValue::from_str(sequence).unwrap(),
                );
            }
            assert_eq!(publication_envelope(&headers), Err(StatusCode::BAD_REQUEST));
        }
        let mut duplicate_sequence = release_headers("1");
        duplicate_sequence.append(PUBLISHER_SEQUENCE_HEADER, HeaderValue::from_static("2"));
        assert_eq!(
            publication_envelope(&duplicate_sequence),
            Err(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn publication_failures_have_stable_non_secret_statuses() {
        assert_eq!(
            publication_error_status(&PublicationError::UntrustedPublisher.into()),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            publication_error_status(&PublicationError::AttestationConflict.into()),
            StatusCode::CONFLICT
        );
        assert_eq!(
            publication_error_status(&PublicationError::UnsignedForbidden.into()),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            publication_error_status(&ServerError::State(
                "metadata publication is read-only until restart".to_string()
            )),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn router_orders_bearer_publisher_and_body_limit_without_mutation() {
        let _environment = ENV_LOCK.lock().await;
        let _token = PublishTokenGuard::install("route-secret");
        let allowed_key = [0x41; 32];
        let directory = std::env::temp_dir().join(format!(
            "distd-rest-publication-order-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let durable_before = std::fs::read(&paths.current).unwrap();
        let server = RawServer::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_publisher_policy(crate::publication::PublisherPolicy::required([
                PublisherPublicKey::from_bytes(allowed_key),
            ]))
            .with_metadata_store(store);
        let storage = server.storage().clone();
        let metadata_before = server.metadata_snapshot().await;
        let observed_server = server.clone();
        let exact_body = multipart_body(b"x");
        let over_body = multipart_body(b"xy");
        assert_eq!(over_body.len(), exact_body.len() + 1);
        let admission = test_admission(MAX_PUBLICATION_BODY_BYTES);
        let held = Arc::clone(&admission.permits)
            .try_acquire_many_owned(MAX_PUBLICATION_BODY_BYTES as u32)
            .unwrap();
        let router = make_router_with_admission(server, exact_body.len(), admission.clone());

        let status = router
            .clone()
            .oneshot(request(over_body.clone(), None, [0x42; 32]))
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let status = router
            .clone()
            .oneshot(request(over_body.clone(), Some("route-secret"), [0x42; 32]))
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let status = router
            .clone()
            .oneshot(request(
                exact_body.clone(),
                Some("route-secret"),
                allowed_key,
            ))
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(storage.size(), 0);
        assert_eq!(observed_server.metadata_snapshot().await, metadata_before);
        assert_eq!(std::fs::read(&paths.current).unwrap(), durable_before);
        drop(held);
        assert_eq!(admission.available_bytes(), MAX_PUBLICATION_BODY_BYTES);

        let exact = router
            .clone()
            .oneshot(request(exact_body, Some("route-secret"), allowed_key))
            .await
            .unwrap()
            .status();
        assert_ne!(exact, StatusCode::PAYLOAD_TOO_LARGE);
        let over = router
            .oneshot(request(over_body, Some("route-secret"), allowed_key))
            .await
            .unwrap()
            .status();
        assert_eq!(over, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(storage.size(), 0);
        assert_eq!(observed_server.metadata_snapshot().await, metadata_before);
        assert_eq!(metadata_before, InternalMetadata::default());
        assert_eq!(std::fs::read(&paths.current).unwrap(), durable_before);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn publication_admission_is_after_auth_fail_fast_and_exactly_released() {
        let _environment = ENV_LOCK.lock().await;
        let _token = PublishTokenGuard::install("admission-secret");
        let allowed_key = [0x51; 32];
        let server = RawServer::new_ephemeral(HashMapStorage::default()).with_publisher_policy(
            crate::publication::PublisherPolicy::required([PublisherPublicKey::from_bytes(
                allowed_key,
            )]),
        );
        let storage = server.storage().clone();
        let observed = server.clone();
        let body = multipart_body(b"admitted");
        let admission = test_admission(body.len());
        let held = Arc::clone(&admission.permits)
            .try_acquire_many_owned(body.len() as u32)
            .unwrap();
        assert_eq!(admission.available_bytes(), 0);
        let router = make_router_with_admission(server, body.len() + 1, admission.clone());

        let unauthorized = router
            .clone()
            .oneshot(request(body.clone(), None, allowed_key))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(admission.available_bytes(), 0);

        let untrusted = router
            .clone()
            .oneshot(request(body.clone(), Some("admission-secret"), [0x52; 32]))
            .await
            .unwrap();
        assert_eq!(untrusted.status(), StatusCode::FORBIDDEN);
        assert_eq!(admission.available_bytes(), 0);

        let exhausted = router
            .clone()
            .oneshot(request(body.clone(), Some("admission-secret"), allowed_key))
            .await
            .unwrap();
        assert_eq!(exhausted.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(admission.available_bytes(), 0);

        let mut undeclared = request(body.clone(), Some("admission-secret"), allowed_key);
        undeclared.headers_mut().remove(CONTENT_LENGTH);
        let undeclared = router.clone().oneshot(undeclared).await.unwrap();
        assert_eq!(undeclared.status(), StatusCode::LENGTH_REQUIRED);

        for value in ["not-a-length", "18446744073709551616"] {
            let mut malformed = request(body.clone(), Some("admission-secret"), allowed_key);
            malformed
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from_str(value).unwrap());
            let malformed = router.clone().oneshot(malformed).await.unwrap();
            assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        }
        let mut duplicate_length = request(body.clone(), Some("admission-secret"), allowed_key);
        duplicate_length
            .headers_mut()
            .append(CONTENT_LENGTH, HeaderValue::from_static("1"));
        let duplicate_length = router.clone().oneshot(duplicate_length).await.unwrap();
        assert_eq!(duplicate_length.status(), StatusCode::BAD_REQUEST);

        let too_large_body = multipart_body(b"admitted-over-budget");
        let too_large = router
            .clone()
            .oneshot(request(
                too_large_body,
                Some("admission-secret"),
                allowed_key,
            ))
            .await
            .unwrap();
        assert_eq!(too_large.status(), StatusCode::PAYLOAD_TOO_LARGE);

        drop(held);
        assert_eq!(admission.available_bytes(), body.len());
        let ordinary_rejection = router
            .oneshot(request(body.clone(), Some("admission-secret"), allowed_key))
            .await
            .unwrap();
        assert_eq!(ordinary_rejection.status(), StatusCode::FORBIDDEN);
        assert_eq!(admission.available_bytes(), body.len());
        assert_eq!(storage.size(), 0);
        assert_eq!(
            observed.metadata_snapshot().await,
            InternalMetadata::default()
        );
    }

    #[tokio::test]
    async fn stalled_partial_multipart_times_out_before_storage_and_releases_admission() {
        let _environment = ENV_LOCK.lock().await;
        let _token = PublishTokenGuard::install("deadline-secret");
        let allowed_key = [0x61; 32];
        let directory = std::env::temp_dir().join(format!(
            "distd-rest-publication-deadline-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let durable_before = std::fs::read(&paths.current).unwrap();
        let server = RawServer::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_publisher_policy(crate::publication::PublisherPolicy::required([
                PublisherPublicKey::from_bytes(allowed_key),
            ]))
            .with_metadata_store(store);
        let storage = server.storage().clone();
        let observed = server.clone();
        let metadata_before = server.metadata_snapshot().await;

        let complete = multipart_body(b"never-completes");
        let partial_length = complete.len() - b"\r\n--distd-test--\r\n".len();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(Ok::<_, std::io::Error>(Bytes::copy_from_slice(
                &complete[..partial_length],
            )))
            .await
            .unwrap();
        let request = request_with_body(
            Body::from_stream(ReceiverStream::new(receiver)),
            complete.len(),
            Some("deadline-secret"),
            allowed_key,
        );
        let admission =
            PublicationAdmission::new(complete.len(), Duration::from_millis(20)).unwrap();
        let router = make_router_with_admission(server, complete.len() + 1, admission.clone());
        let response = tokio::time::timeout(Duration::from_secs(1), router.oneshot(request))
            .await
            .expect("stalled multipart must meet its configured deadline")
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(admission.available_bytes(), complete.len());
        assert_eq!(storage.size(), 0);
        assert_eq!(observed.metadata_snapshot().await, metadata_before);
        assert_eq!(std::fs::read(&paths.current).unwrap(), durable_before);

        drop(sender);
        drop(observed);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn dropped_publication_handler_releases_owned_admission() {
        let admission = test_admission(4_096);
        let router = Router::new()
            .route(
                "/items",
                axum::routing::post(|| async { std::future::pending::<StatusCode>().await }),
            )
            .layer(middleware::from_fn_with_state(
                admission.clone(),
                admit_publication_body,
            ));
        let request = Request::builder()
            .method(Method::POST)
            .uri("/items")
            .header(CONTENT_LENGTH, "4096")
            .body(Body::empty())
            .unwrap();
        let task = tokio::spawn(router.oneshot(request));
        while admission.available_bytes() != 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        assert_eq!(admission.available_bytes(), 4_096);
    }

    #[test]
    #[ignore = "release-only local publication admission reference"]
    fn publication_admission_reference_performance() {
        assert!(
            !cfg!(debug_assertions),
            "run the publication admission reference in release mode"
        );
        let admission = test_admission(MAX_PUBLICATION_BODY_BYTES);
        for _ in 0..1_000 {
            drop(
                Arc::clone(&admission.permits)
                    .try_acquire_many_owned(256 * 1024)
                    .unwrap(),
            );
        }
        let mut samples = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let started = std::time::Instant::now();
            let permit = Arc::clone(&admission.permits)
                .try_acquire_many_owned(256 * 1024)
                .unwrap();
            drop(permit);
            samples.push(started.elapsed().as_nanos());
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let p95 = sorted[(sorted.len() * 95).div_ceil(100) - 1];
        println!("publication_admission_raw_ns={samples:?}");
        println!("publication_admission_p95_ns={p95}");
        assert!(p95 <= 10_000, "publication admission p95 {p95}ns");
        assert_eq!(admission.available_bytes(), MAX_PUBLICATION_BODY_BYTES);
    }

    #[tokio::test]
    async fn valid_v2_rest_release_persists_exact_sequence_and_envelope() {
        let _environment = ENV_LOCK.lock().await;
        let _token = PublishTokenGuard::install("release-secret");
        let payload = b"rollback-protected REST release";
        let sequence = PublisherReleaseSequence::new(42);
        let storage = HashMapStorage::default();
        let item = storage
            .prepare_revision(
                "route-test".to_string(),
                "route-test".to_string(),
                "route.bin".into(),
                0,
                None,
                Bytes::from_static(payload),
                ChunkAlgorithm::Fixed256K,
            )
            .unwrap();
        let pair = Ed25519KeyPair::from_seed_unchecked(&[0x55; 32]).unwrap();
        let statement = PublisherReleaseStatement::from_item(&item, sequence).unwrap();
        let envelope = PublisherReleaseEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        );
        let server = RawServer::new_ephemeral(storage).with_publisher_policy(
            crate::publication::PublisherPolicy::required([envelope.public_key()]),
        );
        let observed = server.clone();
        let body = multipart_body(payload);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/items?artifact_id=route-test&name=route-test&path=route.bin&chunking=fixed")
            .header(
                axum::http::header::CONTENT_TYPE,
                "multipart/form-data; boundary=distd-test",
            )
            .header(CONTENT_LENGTH, body.len().to_string())
            .header(axum::http::header::AUTHORIZATION, "Bearer release-secret")
            .header(PUBLISHER_VERSION_HEADER, "2")
            .header(PUBLISHER_SEQUENCE_HEADER, sequence.get().to_string())
            .header(PUBLISHER_KEY_HEADER, hex(envelope.public_key().as_bytes()))
            .header(
                PUBLISHER_SIGNATURE_HEADER,
                hex(envelope.signature().as_bytes()),
            )
            .body(Body::from(body.clone()))
            .unwrap();
        let admission = test_admission(body.len() + 1);
        let response = make_router_with_admission(server, body.len() + 1, admission.clone())
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(admission.available_bytes(), body.len() + 1);
        let metadata = observed.metadata_snapshot().await;
        let published = &metadata.versions["route-test"][&0];
        assert_eq!(published.item().manifest.root_hash, item.manifest.root_hash);
        assert_eq!(
            published.attestation().release(),
            Some((sequence, &envelope))
        );
    }
}
