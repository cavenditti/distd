//! Metadata-only ("lazy") revision activation backed by `composefs` EROFS images.
//!
//! A lazy activation durably records an already verified revision for one activation
//! destination and projects it into a content-addressed EROFS metadata image without requiring
//! any payload chunk to be resident. File contents are hydrated later, on demand, into the CAS
//! blob store that the image's `trusted.overlay.redirect` xattrs point at.
//!
//! Layout below the activation namespace:
//! - `lazy/<destination_identity>.state`: enveloped [`FsLazyRevisionV1`] record, atomically
//!   replaced by every lazy activation of that destination.
//! - `composefs/<image_hash>.erofs`: immutable, content-addressed EROFS image.
//!
//! The image is a pure function of the recorded [`Item`] (default [`ComposefsOptions`], no
//! inline payloads), so reading a record back regenerates the image and compares it bit for bit
//! with the stored file.

use std::fs::{self, remove_file, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{fs_storage_root_identity, identity_filename, invalid_state, FsStorage};
use crate::chunk_storage::{prepared_tree, StorageError};
use crate::chunks::ChunkInfo;
use crate::composefs::{build_composefs_erofs, ComposefsImageInfo, ComposefsOptions};
use crate::durable_state::{
    read_bounded_file, read_enveloped_state, replace_enveloped_state, DurableStateKind,
    MAX_DURABLE_STATE_PAYLOAD,
};
use crate::error::{Error, InvalidParameter};
use crate::hash::{hash, Hash};
use crate::item::{Item, Manifest, Name as ItemName};

const FS_LAZY_REVISION_SCHEMA: u16 = 1;
const FS_LAZY_NAMESPACE: &str = "lazy";
const FS_COMPOSEFS_NAMESPACE: &str = "composefs";
const FS_COMPOSEFS_IMAGE_EXTENSION: &str = "erofs";
/// Generated metadata images stay far below this for the largest admissible manifest.
const MAX_COMPOSEFS_IMAGE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FsLazyRevisionV1 {
    root_identity: [u8; 32],
    destination_identity: [u8; 32],
    item: Item,
    image_hash: [u8; 32],
    image_len: u64,
}

/// A durably recorded metadata-only revision and its `composefs` metadata image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LazyRevision {
    /// The verified revision, bound to its canonical activation destination.
    pub item: Item,
    /// Content-addressed EROFS image (`.distd-activation-v1/composefs/<image_hash>.erofs`).
    pub image_path: PathBuf,
    /// BLAKE3 digest of the image bytes.
    pub image_hash: Hash,
    /// Parsed image layout, including every file's CAS blob redirect.
    pub image: ComposefsImageInfo,
}

impl FsStorage {
    /// Directory holding content-addressed `composefs` EROFS images
    /// (`.distd-activation-v1/composefs`).
    #[must_use]
    pub fn composefs_images_root(&self) -> PathBuf {
        self.activation_root().join(FS_COMPOSEFS_NAMESPACE)
    }

    /// Path of the content-addressed `composefs` image with digest `image_hash`.
    #[must_use]
    pub fn composefs_image_path(&self, image_hash: &Hash) -> PathBuf {
        self.composefs_images_root()
            .join(format!("{image_hash}.{FS_COMPOSEFS_IMAGE_EXTENSION}"))
    }

    /// Durably record a verified revision for `destination` without fetching any payload, and
    /// publish its `composefs` EROFS metadata image.
    ///
    /// `manifest` and `chunk_infos` must already have passed transport, publisher and rollback
    /// verification. The chunk list must reconstruct the manifest root. A later lazy activation
    /// of the same destination atomically replaces the record; eager activations of the
    /// destination are independent and unaffected.
    pub fn activate_lazy_revision(
        &self,
        name: ItemName,
        destination: &Path,
        revision: u32,
        description: Option<String>,
        manifest: &Manifest,
        chunk_infos: &[ChunkInfo],
    ) -> Result<LazyRevision, Error> {
        self.ensure_activation_not_poisoned()?;
        let destination = self.canonical_activation_destination(destination)?;
        let destination_identity = self.activation_destination_identity(&destination)?;
        let destination_lock = self.destination_lock(destination_identity);
        let _destination_guard = destination_lock.lock.lock().unwrap();
        self.ensure_activation_not_poisoned()?;

        if chunk_infos.is_empty() || chunk_infos.len() != manifest.chunk_count as usize {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }
        let root = prepared_tree(chunk_infos)?;
        if root.hash != manifest.root_hash || root.size != manifest.total_size {
            return Err(Error::Storage(StorageError::TreeReconstruct));
        }
        let item = Item::make_with_entries(
            name,
            destination,
            revision,
            description,
            root,
            chunk_infos.to_vec(),
            manifest.entries.clone(),
            manifest.chunk_algorithm,
        )?
        .with_artifact_id(manifest.artifact_id.clone());

        let (image_bytes, image) = build_composefs_erofs(&item, &ComposefsOptions::default())?;
        let image_hash = hash(&image_bytes);
        let image_path = self.publish_composefs_image(&image_hash, &image_bytes)?;

        let record = FsLazyRevisionV1 {
            root_identity: fs_storage_root_identity(&self.inner.read().unwrap().root)?,
            destination_identity,
            item,
            image_hash: *image_hash.as_bytes(),
            image_len: image_bytes.len() as u64,
        };
        let payload = bitcode::serialize(&record).map_err(InvalidParameter::from)?;
        let record_path = self
            .ensure_activation_namespace(&[FS_LAZY_NAMESPACE])?
            .join(identity_filename(&destination_identity));
        replace_enveloped_state(
            &record_path,
            DurableStateKind::FsLazyRevision,
            FS_LAZY_REVISION_SCHEMA,
            &payload,
            MAX_DURABLE_STATE_PAYLOAD,
        )?;

        Ok(LazyRevision {
            item: record.item,
            image_path,
            image_hash,
            image,
        })
    }

    /// Load and re-verify the lazy revision recorded for `destination`, if any.
    ///
    /// The stored image must match the image regenerated from the recorded revision exactly.
    pub fn lazy_revision(&self, destination: &Path) -> Result<Option<LazyRevision>, Error> {
        let destination = self.canonical_activation_destination(destination)?;
        let destination_identity = self.activation_destination_identity(&destination)?;
        let record_path = self
            .activation_root()
            .join(FS_LAZY_NAMESPACE)
            .join(identity_filename(&destination_identity));
        let payload = match read_enveloped_state(
            &record_path,
            DurableStateKind::FsLazyRevision,
            FS_LAZY_REVISION_SCHEMA,
            MAX_DURABLE_STATE_PAYLOAD,
        ) {
            Ok(payload) => payload,
            Err(crate::durable_state::DurableStateError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
        };
        let record: FsLazyRevisionV1 =
            bitcode::deserialize(&payload).map_err(InvalidParameter::from)?;
        if record.root_identity != fs_storage_root_identity(&self.inner.read().unwrap().root)? {
            return Err(invalid_state("lazy revision root identity mismatch"));
        }
        if record.destination_identity != destination_identity
            || record.item.metadata.path != destination
        {
            return Err(invalid_state("lazy revision destination mismatch"));
        }

        let (expected_bytes, image) =
            build_composefs_erofs(&record.item, &ComposefsOptions::default())?;
        let image_hash = Hash::from_bytes(record.image_hash);
        if hash(&expected_bytes) != image_hash || expected_bytes.len() as u64 != record.image_len {
            return Err(invalid_state(
                "lazy revision image identity differs from its revision",
            ));
        }
        let image_path = self.composefs_image_path(&image_hash);
        Self::verify_composefs_image(&image_path, &expected_bytes)?;

        Ok(Some(LazyRevision {
            item: record.item,
            image_path,
            image_hash,
            image,
        }))
    }

    fn ensure_activation_not_poisoned(&self) -> Result<(), Error> {
        if self
            .activation_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(invalid_state(
                "activation state is indeterminate and requires restart",
            ));
        }
        Ok(())
    }

    /// Publish `bytes` as an immutable, content-addressed image without ever replacing an
    /// existing file in place.
    fn publish_composefs_image(&self, image_hash: &Hash, bytes: &[u8]) -> Result<PathBuf, Error> {
        let parent = self.ensure_activation_namespace(&[FS_COMPOSEFS_NAMESPACE])?;
        let path = self.composefs_image_path(image_hash);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                Self::verify_composefs_image(&path, bytes)?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let staging = parent.join(format!(".{image_hash}.{}.tmp", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?;
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            drop(file);
            let _ = remove_file(&staging);
            return Err(error.into());
        }
        drop(file);
        match fs::hard_link(&staging, &path) {
            Ok(()) => remove_file(&staging)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                remove_file(&staging)?;
                Self::verify_composefs_image(&path, bytes)?;
            }
            Err(error) => {
                let _ = remove_file(&staging);
                return Err(error.into());
            }
        }
        #[cfg(unix)]
        File::open(&parent)?.sync_all()?;
        Ok(path)
    }

    fn verify_composefs_image(path: &Path, expected: &[u8]) -> Result<(), Error> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(invalid_state("composefs image is missing"))
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid_state("composefs image is not a regular file"));
        }
        if metadata.len() != expected.len() as u64
            || read_bounded_file(path, MAX_COMPOSEFS_IMAGE_BYTES)? != expected
        {
            return Err(invalid_state(
                "composefs image content differs from its identity",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::chunk_storage::ChunkStorage;
    use crate::chunks::ChunkAlgorithm;
    use crate::composefs::inspect_composefs_erofs;
    use crate::item::EntryKind;
    use crate::utils::testing::temp_path;

    fn directory_revision(storage: &FsStorage, revision: u32, tail: &[u8]) -> Item {
        storage
            .prepare_revision_from_files(
                "lazy-pkg".to_string(),
                "lazy-pkg".to_string(),
                PathBuf::from("lazy-pkg"),
                revision,
                None,
                vec![
                    (
                        PathBuf::from("bin/tool"),
                        Bytes::from(vec![0x7f; 3 * 4096 + 17]),
                    ),
                    (
                        PathBuf::from("share/doc/README"),
                        Bytes::from(tail.to_vec()),
                    ),
                ],
                ChunkAlgorithm::fixed_aligned(4096, 1),
            )
            .unwrap()
    }

    fn activate(storage: &FsStorage, item: &Item) -> Result<LazyRevision, Error> {
        storage.activate_lazy_revision(
            item.metadata.name.clone(),
            Path::new("lazy-pkg"),
            item.metadata.revision,
            item.metadata.description.clone(),
            &item.manifest,
            &item.chunks,
        )
    }

    #[test]
    fn lazy_activation_records_revision_and_image_without_payload() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let item = directory_revision(&storage, 1, b"lazy readme\n");

        let lazy = activate(&storage, &item).unwrap();

        assert_eq!(lazy.item.chunks, item.chunks);
        assert_eq!(lazy.item.manifest.entries, item.manifest.entries);
        assert_eq!(lazy.item.metadata.artifact_id, "lazy-pkg");
        assert!(lazy.image_path.starts_with(storage.composefs_images_root()));
        let on_disk = fs::read(&lazy.image_path).unwrap();
        assert_eq!(hash(&on_disk), lazy.image_hash);
        assert_eq!(inspect_composefs_erofs(&on_disk).unwrap(), lazy.image);
        let paths: Vec<_> = lazy
            .image
            .files
            .iter()
            .map(|file| file.relative_path.as_str())
            .collect();
        assert_eq!(paths, ["bin/tool", "share/doc/README"]);
        for file in &lazy.image.files {
            assert!(file.redirect_path.is_some());
            assert!(!storage.has_cas_blob(&file.blob_hash, file.size));
        }
        // No payload was stored, and no eager activation happened.
        assert!(storage.item_for_path(Path::new("lazy-pkg")).is_none());
        assert!(!root.join("lazy-pkg").exists());

        let reloaded = storage.lazy_revision(Path::new("lazy-pkg")).unwrap();
        assert_eq!(reloaded, Some(lazy.clone()));

        // Records survive reopening the storage root.
        drop(storage);
        let reopened = FsStorage::new(root).unwrap();
        assert_eq!(
            reopened.lazy_revision(Path::new("lazy-pkg")).unwrap(),
            Some(lazy)
        );
    }

    #[test]
    fn lazy_activation_replaces_record_and_keeps_images_content_addressed() {
        let storage = FsStorage::new(temp_path()).unwrap();
        assert_eq!(storage.lazy_revision(Path::new("lazy-pkg")).unwrap(), None);

        let first = activate(&storage, &directory_revision(&storage, 1, b"one\n")).unwrap();
        let again = activate(&storage, &directory_revision(&storage, 1, b"one\n")).unwrap();
        assert_eq!(first.image_hash, again.image_hash);

        let second = activate(&storage, &directory_revision(&storage, 2, b"two!\n")).unwrap();
        assert_ne!(first.image_hash, second.image_hash);
        assert!(first.image_path.exists());
        let current = storage
            .lazy_revision(Path::new("lazy-pkg"))
            .unwrap()
            .unwrap();
        assert_eq!(current.item.metadata.revision, 2);
        assert_eq!(current.image_hash, second.image_hash);
    }

    #[test]
    fn lazy_activation_rejects_chunks_that_do_not_reconstruct_the_manifest() {
        let storage = FsStorage::new(temp_path()).unwrap();
        let item = directory_revision(&storage, 1, b"readme\n");

        let mut truncated = item.chunks.clone();
        truncated.pop();
        assert!(storage
            .activate_lazy_revision(
                item.metadata.name.clone(),
                Path::new("lazy-pkg"),
                1,
                None,
                &item.manifest,
                &truncated,
            )
            .is_err());

        let mut forged = item.chunks.clone();
        forged[0].hash = hash(b"forged");
        assert!(storage
            .activate_lazy_revision(
                item.metadata.name.clone(),
                Path::new("lazy-pkg"),
                1,
                None,
                &item.manifest,
                &forged,
            )
            .is_err());
        assert_eq!(storage.lazy_revision(Path::new("lazy-pkg")).unwrap(), None);
    }

    #[test]
    fn lazy_revision_detects_tampered_or_missing_images() {
        let storage = FsStorage::new(temp_path()).unwrap();
        let lazy = activate(&storage, &directory_revision(&storage, 1, b"readme\n")).unwrap();

        let mut bytes = fs::read(&lazy.image_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&lazy.image_path, &bytes).unwrap();
        assert!(storage.lazy_revision(Path::new("lazy-pkg")).is_err());

        fs::remove_file(&lazy.image_path).unwrap();
        assert!(storage.lazy_revision(Path::new("lazy-pkg")).is_err());

        // Re-activating republishes the missing image.
        activate(&storage, &directory_revision(&storage, 1, b"readme\n")).unwrap();
        assert!(storage
            .lazy_revision(Path::new("lazy-pkg"))
            .unwrap()
            .is_some());
    }

    fn kinded_files() -> (
        Vec<(PathBuf, Bytes)>,
        std::collections::BTreeMap<PathBuf, EntryKind>,
    ) {
        (
            vec![
                (PathBuf::from("bin/hello"), Bytes::from(vec![0x7f; 6000])),
                (PathBuf::from("bin/hi"), Bytes::new()),
            ],
            std::collections::BTreeMap::from([
                (PathBuf::from("bin/hello"), EntryKind::Executable),
                (
                    PathBuf::from("bin/hi"),
                    EntryKind::Symlink {
                        target: "/nix/store/abc-hello/bin/hello".to_string(),
                    },
                ),
            ]),
        )
    }

    fn kinded_revision(storage: &FsStorage) -> Item {
        let (files, kinds) = kinded_files();
        storage
            .prepare_revision_from_entries(
                "kinded-pkg".to_string(),
                "kinded-pkg".to_string(),
                PathBuf::from("kinded-pkg"),
                1,
                None,
                files,
                kinds,
                ChunkAlgorithm::fixed_aligned(4096, 1),
            )
            .unwrap()
    }

    fn state_schema(storage: &FsStorage) -> u16 {
        let path = storage.inner.read().unwrap().persistance_path.clone();
        crate::durable_state::read_enveloped_state_any(
            &path,
            DurableStateKind::FsStorage,
            &[1, 2],
            MAX_DURABLE_STATE_PAYLOAD,
        )
        .unwrap()
        .0
    }

    #[test]
    fn kinded_revisions_persist_in_kinded_layouts_only() {
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let regular = directory_revision(&storage, 1, b"readme\n");
        storage
            .commit_revision_from_files(
                regular.clone(),
                vec![
                    (
                        PathBuf::from("bin/tool"),
                        Bytes::from(vec![0x7f; 3 * 4096 + 17]),
                    ),
                    (
                        PathBuf::from("share/doc/README"),
                        Bytes::from_static(b"readme\n"),
                    ),
                ],
            )
            .unwrap();
        assert_eq!(state_schema(&storage), 1);

        let kinded = kinded_revision(&storage);
        let (files, _) = kinded_files();
        let committed = storage
            .commit_revision_from_files(kinded.clone(), files)
            .unwrap();
        assert_eq!(committed.manifest.entries, kinded.manifest.entries);
        assert_eq!(state_schema(&storage), 2);
        let markers: Vec<_> = fs::read_dir(root.join(".distd-revisions-v1").join("items"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        let extensions: std::collections::BTreeSet<_> = markers
            .iter()
            .filter_map(|path| path.extension()?.to_str().map(str::to_owned))
            .collect();
        assert_eq!(extensions, ["item".to_string(), "kitem".to_string()].into());

        drop(storage);
        let reopened = FsStorage::new(root).unwrap();
        let reloaded = reopened.item_for_path(Path::new("kinded-pkg")).unwrap();
        assert_eq!(reloaded.manifest.entries, kinded.manifest.entries);
        assert_eq!(
            reopened
                .item_for_path(Path::new("lazy-pkg"))
                .unwrap()
                .manifest
                .entries,
            regular.manifest.entries
        );
    }

    #[test]
    fn lazy_activation_projects_entry_kinds() {
        let storage = FsStorage::new(temp_path()).unwrap();
        let item = kinded_revision(&storage);
        let lazy = storage
            .activate_lazy_revision(
                item.metadata.name.clone(),
                Path::new("kinded-pkg"),
                1,
                None,
                &item.manifest,
                &item.chunks,
            )
            .unwrap();
        assert_eq!(lazy.item.manifest.entries, item.manifest.entries);
        assert_eq!(lazy.image.files.len(), 1);
        assert!(lazy.image.files[0].executable);
        assert_eq!(lazy.image.symlinks.len(), 1);
        assert_eq!(
            lazy.image.symlinks[0].target,
            "/nix/store/abc-hello/bin/hello"
        );
        assert_eq!(
            storage.lazy_revision(Path::new("kinded-pkg")).unwrap(),
            Some(lazy)
        );
    }
}
