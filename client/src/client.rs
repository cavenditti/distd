use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use tokio::time::{sleep, Instant};
use uuid::Uuid;

use crate::{
    error::Client as ClientError, persistence::ClientState, server::Server, settings::Settings,
};

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, node::Node, ChunkStorage},
    hash::{hash, merge_hashes, Hash},
    item::{Item, Manifest},
    metadata::Item as ItemMetadata,
};

fn reconstruct_root_from_chunks(
    manifest: &Manifest,
    chunk_hashes: &[Hash],
    local_hashes: &[Hash],
    received_chunks: Vec<Vec<u8>>,
) -> Result<Arc<Node>, ClientError>
{
    if chunk_hashes.len() != manifest.chunk_count as usize {
        return Err(ClientError::TreeReconstruct);
    }

    let available_hashes: HashSet<Hash> = local_hashes.iter().copied().collect();
    let mut received_chunks: VecDeque<Vec<u8>> = received_chunks.into();

    let mut partials = Vec::with_capacity(chunk_hashes.len());
    for (index, expected_hash) in chunk_hashes.iter().copied().enumerate() {
        let node = if available_hashes.contains(&expected_hash) {
            let size = if index + 1 == chunk_hashes.len() {
                let full_chunks = chunk_hashes.len().saturating_sub(1) as u64;
                manifest.total_size - full_chunks * manifest.chunk_size as u64
            } else {
                manifest.chunk_size as u64
            };
            Arc::new(Node::Skipped {
                hash: expected_hash,
                size,
            })
        } else {
            let data = received_chunks.pop_front().ok_or(ClientError::TreeReconstruct)?;
            if hash(&data) != expected_hash {
                return Err(ClientError::TreeReconstruct);
            }
            Arc::new(Node::Stored {
                hash: expected_hash,
                data: Arc::new(data),
            })
        };
        partials.push(node);
    }

    if !received_chunks.is_empty() {
        return Err(ClientError::TreeReconstruct);
    }

    if partials.is_empty() {
        return Err(ClientError::TreeReconstruct);
    }

    while partials.len() > 1 {
        let mut next_level = Vec::with_capacity(partials.len().div_ceil(2));
        let mut index = 0;
        while index < partials.len() {
            if index + 1 < partials.len() {
                let left = partials[index].clone();
                let right = partials[index + 1].clone();
                let hash = merge_hashes(left.hash(), right.hash());
                let size = left.size() + right.size();
                let node = if matches!(left.as_ref(), Node::Skipped { .. })
                    && matches!(right.as_ref(), Node::Skipped { .. })
                {
                    Arc::new(Node::Skipped { hash, size })
                } else {
                    Arc::new(Node::Parent {
                        hash,
                        size,
                        left,
                        right,
                    })
                };
                next_level.push(node);
                index += 2;
            } else {
                next_level.push(partials[index].clone());
                index += 1;
            }
        }
        partials = next_level;
    }

    let root = partials.swap_remove(0);
    if root.hash() != &manifest.root_hash || root.size() != manifest.total_size {
        return Err(ClientError::TreeReconstruct);
    }

    Ok(root)
}

#[derive(Debug)]
pub struct RegisterError;

#[derive(Debug, Clone)]
pub struct Client<T>
where
    T: ChunkStorage,
{
    name: String,
    pub server: Server,
    pub storage: T,
    pub settings: Arc<Settings>,
    pub state: Arc<ClientState>,
}

impl<T> Client<T>
where
    T: ChunkStorage,
{
    pub async fn new(
        server_public_key: &[u8; 32],
        storage: T,
        settings: Settings,
        mut state: ClientState,
    ) -> Result<Self, ClientError> {
        let client_uuid = state
            .persistent
            .client_uuid
            .as_ref()
            .and_then(|uuid_str| Uuid::from_str(uuid_str).ok());

        let server = loop {
            match Server::new(
                &settings.server.url,
                &settings.client.name,
                client_uuid,
                server_public_key,
            )
            .await
            {
                Ok(server) => break server,
                Err(e) => {
                    const T: u64 = 5;
                    tracing::warn!("Error: '{e}', retrying in {T} seconds");
                    sleep(Duration::from_secs(T)).await;
                }
            }
        };

        state.persistent.client_uuid = Some(server.client_uuid().to_string());
        state.persistent.commit().unwrap();

        let items = server.metadata().await.items;
        let server_ids: Vec<&String> = items.keys().collect();
        let missing: Vec<&PathBuf> = settings
            .client
            .sync
            .iter()
            .filter(|p| {
                let aid = p.to_string_lossy().to_string();
                !server_ids.iter().any(|s| **s == aid)
            })
            .collect();
        if !missing.is_empty() {
            tracing::error!(
                "Some requested items could not be found on server: {}",
                missing
                    .iter()
                    .map(|p| format!("'{}'", p.to_string_lossy()))
                    .collect::<Vec<String>>()
                    .join(",")
            );
        }

        Ok(Self {
            name: String::from(&settings.client.name),
            server,
            storage,
            settings: Arc::new(settings),
            state: Arc::new(state),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Client<FsStorage> {
    fn local_diff_basis(&self, path: &Path) -> (Option<u32>, Vec<Hash>) {
        self.storage
            .item_for_path(path)
            .map(|item| {
                (
                    Some(item.metadata.revision),
                    item.chunks.iter().map(|chunk| chunk.hash).collect(),
                )
            })
            .unwrap_or_default()
    }

    async fn sync_transfer(
        &mut self,
        target: ItemMetadata,
        from: &[Hash],
    ) -> Result<Item, ClientError> {
        let (manifest, chunk_hashes, received_chunks) = self
            .server
            .sync_artifact(&target.artifact_id, from)
            .await?;

        let received_chunk_count = received_chunks.len();
        let payload_bytes = received_chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>();
        let root = reconstruct_root_from_chunks(&manifest, &chunk_hashes, from, received_chunks)?;
        let diff_stream = tokio_stream::iter(root.find_diff(&[]).map(|node| (*node).clone()));

        let item = self.storage
            .receive_item(
                target.name,
                target.path,
                target.revision,
                target.description,
                diff_stream,
            )
            .await
            .map_err(ClientError::Core)?;

        tracing::info!(
            "distd_sync_payload_bytes={payload_bytes} distd_sync_received_chunks={}",
            received_chunk_count
        );

        Ok(item)
    }

    pub async fn get(&mut self, target: &Path, path: &Path) -> Result<Item, ClientError> {
        tracing::debug!("sync: {target:?} {path:?}");

        let path = self.storage.path(path);
        let (_, from) = self.local_diff_basis(&path);

        let server_metadata = self.server.metadata().await;
        // Look up by artifact_id (target path string used as artifact_id)
        let target_str = target.to_string_lossy().to_string();
        let item_metadata = server_metadata
            .items
            .get(&target_str)
            .ok_or(ClientError::FileNotFound(target_str.clone()))?;

        tracing::info!(
            "Fetching item '{}' at '{}' (local chunks: {})",
            item_metadata.name,
            item_metadata.path.to_string_lossy(),
            from.len()
        );
        let now = Instant::now();

        let item = self
            .sync_transfer(item_metadata.clone(), &from)
            .await?;

        tracing::info!(
            "Got {} v{}, {} bytes after {:.4}s",
            item.metadata.name,
            item.metadata.revision,
            item.size(),
            now.elapsed().as_secs_f32()
        );

        Ok(item)
    }

    async fn update(&mut self, new_item_metadata: &ItemMetadata) -> Result<Item, ClientError> {
        tracing::info!(
            "Updating item '{}' at '{}'",
            new_item_metadata.name,
            new_item_metadata.path.to_string_lossy()
        );
        let now = Instant::now();

        let local_path = self.storage.path(&new_item_metadata.path);
        let (from_version, from) = self.local_diff_basis(&local_path);

        tracing::info!(
            "Requesting revision {} from local revision {:?} with {} known chunks",
            new_item_metadata.revision,
            from_version,
            from.len()
        );

        let item = self
            .sync_transfer(new_item_metadata.clone(), &from)
            .await?;

        tracing::info!(
            "Got {} v{}, {} bytes after {:.4}s",
            item.metadata.name,
            item.metadata.revision,
            item.size(),
            now.elapsed().as_secs_f32()
        );

        Ok(item)
    }

    /// Main client loop
    pub async fn client_loop(mut self) -> Result<(), ClientError> {
        tokio::spawn(self.server.clone().fetch_loop());

        let mut latest: HashMap<String, Hash> = HashMap::default();

        loop {
            tokio::time::sleep(self.server.timeout).await;
            let items = self.server.metadata().await.items;
            for path in &self.settings.client.sync.clone() {
                let artifact_id = path.to_string_lossy().to_string();
                if latest.get(&artifact_id)
                    == items
                        .get(&artifact_id)
                        .map(|i| &i.root.hash)
                {
                    continue;
                }

                tracing::debug!("Syncing '{artifact_id}'");
                let old_item = items.get(&artifact_id).ok_or(ClientError::Storage)?;
                let item = self.update(old_item).await?;
                latest.insert(artifact_id, *item.root());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use distd_core::chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage, Node};
    use distd_core::chunks::CHUNK_SIZE;
    use distd_core::hash::HashTreeCapable;
    use distd_core::item::Manifest;

    use super::reconstruct_root_from_chunks;

    #[test]
    fn reconstruct_root_from_chunks_matches_manifest() {
        let storage = HashMapStorage::default();
        let data: Vec<u8> = (0..(CHUNK_SIZE * 5 + 17))
            .map(|index| (index % 251) as u8)
            .collect();

        let root = storage.compute_tree(&data).unwrap();
        storage.try_fill_in(&root).unwrap();
        let manifest = Manifest::from_tree("artifact".into(), 1, &root);
        let chunk_hashes = root.flatten().unwrap();
        let received_chunks: Vec<Vec<u8>> = data
            .chunks(CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        let rebuilt = reconstruct_root_from_chunks(&manifest, &chunk_hashes, &[], received_chunks).unwrap();

        assert_eq!(rebuilt.hash(), root.hash());
        assert_eq!(rebuilt.size(), root.size());
        assert_eq!(rebuilt.clone_data().unwrap(), data);
    }

    #[test]
    fn reconstruct_root_supports_sparse_sync() {
        let storage = HashMapStorage::default();
        let v1: Vec<u8> = (0..(CHUNK_SIZE * 8))
            .map(|index| ((index * 3) % 251) as u8)
            .collect();
        let old_root = storage.compute_tree(&v1).unwrap();
        storage.try_fill_in(&old_root).unwrap();

        let mut v2 = v1.clone();
        for block_index in [1usize, 5usize] {
            let start = block_index * CHUNK_SIZE;
            let end = start + CHUNK_SIZE;
            for (offset, byte) in v2[start..end].iter_mut().enumerate() {
                *byte = byte.wrapping_add((block_index as u8).wrapping_mul(17)) ^ (offset as u8);
            }
        }

        let new_root = storage.compute_tree(&v2).unwrap();
        let manifest = Manifest::from_tree("artifact".into(), 2, &new_root);
        let chunk_hashes = new_root.flatten().unwrap();
        let old_hashes = old_root.flatten().unwrap();
        let received_chunks: Vec<Vec<u8>> = v2
            .chunks(CHUNK_SIZE)
            .enumerate()
            .filter(|(index, _)| matches!(*index, 1 | 5))
            .map(|(_, chunk)| chunk.to_vec())
            .collect();

        let rebuilt = reconstruct_root_from_chunks(&manifest, &chunk_hashes, &old_hashes, received_chunks).unwrap();
        let diff_nodes: Vec<Arc<Node>> = rebuilt.clone().find_diff(&[]).collect();
        let stored_bytes: u64 = diff_nodes
            .iter()
            .map(|node| match node.as_ref() {
                Node::Stored { data, .. } => data.len() as u64,
                Node::Parent { .. } | Node::Skipped { .. } => 0,
            })
            .sum();

        assert_eq!(stored_bytes, (CHUNK_SIZE * 2) as u64);
        assert!(diff_nodes.iter().any(|node| matches!(node.as_ref(), Node::Skipped { .. })));
        assert_eq!(rebuilt.hash(), new_root.hash());
        assert_eq!(rebuilt.size(), new_root.size());
    }
}

pub mod cli {
    use std::{env, path::PathBuf, str::FromStr};

    use distd_core::chunk_storage::fs_storage::FsStorage;

    use crate::client::Client;
    use crate::error::Client as ClientError;
    use crate::persistence::ClientState;
    use crate::settings::Settings;

    pub async fn main() -> Result<(), ClientError> {
        let settings = Settings::new("ClientSettings")?;

        tracing_subscriber::fmt()
            .with_target(true)
            //.compact()
            .with_max_level(tracing::Level::from_str(&settings.log.level).unwrap())
            .init();

        tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        tracing::debug!("Settings: {settings:?}");

        let cmd = std::env::args().nth(1).ok_or(ClientError::MissingCmd)?;
        let cmd_args = std::env::args().skip(2).collect::<Vec<String>>();

        tracing::debug!("Running \"{cmd}\" {cmd_args:?}");

        let state = ClientState::default();
        tracing::trace!("Client state initialized");

        let Ok(storage_root) = PathBuf::from_str(&settings.fsstorage.root);
        tracing::trace!("Storage root: {}", storage_root.to_string_lossy());
        let storage = FsStorage::new(storage_root);
        //let storage = HashMapStorage::default(); // use this for benchmarking in order to avoid potential fs-related bottlenecks
        tracing::trace!("Client storage initialized");
        let client = Client::new(&[0u8; 32], storage, settings, state).await?;
        tracing::trace!("Client initialized");

        match cmd.as_str() {
            "start" => client.client_loop().await,
            "get" => get(client, &cmd_args[..]).await,
            // TODO add "sync: to explicitly request syncing of items subscripted to?
            "publish" => todo!(),
            "subscribe" => todo!(),
            _ => {
                tracing::error!("Invalid command specified");
                Err(ClientError::InvalidCmd(cmd))
            }
        }
        .inspect_err(|e| tracing::error!("Fatal: {e}"))
    }

    async fn get(mut client: Client<FsStorage>, args: &[String]) -> Result<(), ClientError> {
        let first = args
            .first()
            .ok_or(ClientError::InvalidArgs(args.to_owned()))?;

        let (target, path) = match args.len() {
            1 => (first, first),
            2 => (
                first,
                args.get(1)
                    .ok_or(ClientError::InvalidArgs(args.to_owned()))?,
            ),
            _ => return Err(ClientError::InvalidArgs(args.to_owned())),
        };

        let Ok(path) = PathBuf::from_str(path);
        let Ok(target) = PathBuf::from_str(target.as_str());

        client.get(&target, &path).await.map(|_| ())
    }
}
