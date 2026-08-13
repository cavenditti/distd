//! Durable, overwrite-safe evidence primitives for the QUIC acceptance driver.

use ring::digest::SHA256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum EvidenceError {
    #[error("evidence I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("evidence JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("evidence path must be absolute: {0}")]
    RelativePath(PathBuf),
    #[error("evidence path has no normal leaf component: {0}")]
    InvalidLeaf(PathBuf),
    #[error("evidence path is not a safe relative path: {0}")]
    UnsafeRelativePath(PathBuf),
    #[error("evidence root already exists: {0}")]
    RootExists(PathBuf),
    #[error("evidence parent is not a directory: {0}")]
    ParentNotDirectory(PathBuf),
    #[error("expected a regular non-symlink file: {0}")]
    NotRegularFile(PathBuf),
    #[error("file changed while its identity was captured: {0}")]
    FileChanged(PathBuf),
    #[error("manifest path is outside its evidence root: {0}")]
    ManifestOutsideRoot(PathBuf),
    #[error("manifest has unsupported schema: {0}")]
    ManifestSchema(String),
    #[error("manifest encountered a symlink: {0}")]
    ManifestSymlink(PathBuf),
    #[error("manifest path is not valid UTF-8: {0}")]
    NonUtf8ManifestPath(PathBuf),
    #[error("manifest contains its own path: {0}")]
    ManifestSelfEntry(String),
    #[error("manifest contains a duplicate path: {0}")]
    ManifestDuplicate(String),
    #[error("manifest entry is not a safe relative path: {0}")]
    ManifestUnsafePath(String),
    #[error("manifest does not match the evidence tree: {0}")]
    ManifestMismatch(String),
}

fn io_error(path: &Path, source: io::Error) -> EvidenceError {
    EvidenceError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// An absent absolute path resolved through its already-existing canonical parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbsentRoot {
    pub canonical_parent: PathBuf,
    pub leaf: PathBuf,
    pub canonical_path: PathBuf,
}

/// Resolve an output path without creating it or following a final-component symlink.
pub fn canonical_absent_root(path: &Path) -> Result<AbsentRoot, EvidenceError> {
    if !path.is_absolute() {
        return Err(EvidenceError::RelativePath(path.to_path_buf()));
    }

    let leaf = match path.components().next_back() {
        Some(Component::Normal(leaf)) => PathBuf::from(leaf),
        _ => return Err(EvidenceError::InvalidLeaf(path.to_path_buf())),
    };
    let parent = path
        .parent()
        .ok_or_else(|| EvidenceError::InvalidLeaf(path.to_path_buf()))?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| io_error(parent, error))?;
    let parent_metadata =
        fs::metadata(&canonical_parent).map_err(|error| io_error(&canonical_parent, error))?;
    if !parent_metadata.is_dir() {
        return Err(EvidenceError::ParentNotDirectory(canonical_parent));
    }

    let canonical_path = canonical_parent.join(&leaf);
    match fs::symlink_metadata(&canonical_path) {
        Ok(_) => Err(EvidenceError::RootExists(canonical_path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(AbsentRoot {
            canonical_parent,
            leaf,
            canonical_path,
        }),
        Err(error) => Err(io_error(&canonical_path, error)),
    }
}

/// A validated absent root whose parent directory is held open as a capability.
///
/// Creating the root through this value never resolves `canonical_parent` again. If an attacker
/// renames that parent and installs a symlink at its former pathname, creation still occurs in the
/// originally validated directory inode.
#[derive(Debug)]
pub struct PinnedAbsentRoot {
    absent: AbsentRoot,
    parent: RootHandle,
}

impl PinnedAbsentRoot {
    /// Validate an absent absolute root and pin its canonical parent directory.
    pub fn new(path: &Path) -> Result<Self, EvidenceError> {
        if !path.is_absolute() {
            return Err(EvidenceError::RelativePath(path.to_path_buf()));
        }
        let leaf = match path.components().next_back() {
            Some(Component::Normal(leaf)) => PathBuf::from(leaf),
            _ => return Err(EvidenceError::InvalidLeaf(path.to_path_buf())),
        };
        let requested_parent = path
            .parent()
            .ok_or_else(|| EvidenceError::InvalidLeaf(path.to_path_buf()))?;
        let canonical_parent = fs::canonicalize(requested_parent)
            .map_err(|error| io_error(requested_parent, error))?;
        let parent = RootHandle::open(&canonical_parent)?;
        let canonical_path = canonical_parent.join(&leaf);
        if entry_exists_at(&parent.directory, leaf.as_os_str(), &canonical_path)? {
            return Err(EvidenceError::RootExists(canonical_path));
        }
        Ok(Self {
            absent: AbsentRoot {
                canonical_parent,
                leaf,
                canonical_path,
            },
            parent,
        })
    }

    #[must_use]
    pub fn absent(&self) -> &AbsentRoot {
        &self.absent
    }

    /// Create an immutable file at the validated absent leaf through the pinned parent.
    pub fn atomic_create_new_synced(self, bytes: &[u8]) -> Result<(), EvidenceError> {
        atomic_create_new_synced_at(
            &self.parent.directory,
            self.absent.leaf.as_os_str(),
            &self.absent.canonical_path,
            bytes,
        )
    }

    pub fn write_json_create_new<T: Serialize>(self, value: &T) -> Result<(), EvidenceError> {
        self.atomic_create_new_synced(&canonical_json_line(value)?)
    }

    /// Atomically reserve the absent leaf as a directory and return its pinned handle.
    pub fn create_root(self) -> Result<RootHandle, EvidenceError> {
        let display_path = self.absent.canonical_path.clone();
        let leaf = self.absent.leaf.as_os_str();
        let leaf_c = c_name(leaf, &display_path)?;
        let result =
            unsafe { libc::mkdirat(self.parent.directory.as_raw_fd(), leaf_c.as_ptr(), 0o700) };
        if result != 0 {
            return Err(io_error(&display_path, io::Error::last_os_error()));
        }
        self.parent.sync()?;

        let directory = open_directory_at(&self.parent.directory, leaf, &display_path)?;
        let opened = directory
            .metadata()
            .map_err(|error| io_error(&display_path, error))?;
        let current = metadata_at(
            &self.parent.directory,
            leaf,
            &display_path,
            libc::AT_SYMLINK_NOFOLLOW,
        )?;
        let current_device = u64::try_from(current.st_dev)
            .map_err(|_| EvidenceError::FileChanged(display_path.clone()))?;
        if !opened.is_dir()
            || (current.st_mode & libc::S_IFMT) != libc::S_IFDIR
            || opened.dev() != current_device
            || opened.ino() != current.st_ino
        {
            return Err(EvidenceError::FileChanged(display_path));
        }
        Ok(RootHandle {
            directory,
            display_path,
        })
    }
}

/// Pin an absent evidence root's validated parent directory.
pub fn pin_absent_root(path: &Path) -> Result<PinnedAbsentRoot, EvidenceError> {
    PinnedAbsentRoot::new(path)
}

/// A descriptor-pinned evidence directory.
///
/// All relative operations walk from this descriptor with `openat(2)` and `O_NOFOLLOW`, so
/// renaming the root or replacing any pathname ancestor with a symlink cannot redirect them.
#[derive(Debug)]
pub struct RootHandle {
    directory: File,
    display_path: PathBuf,
}

impl RootHandle {
    /// Open and pin an existing directory, rejecting a final-component symlink.
    pub fn open(path: &Path) -> Result<Self, EvidenceError> {
        if !path.is_absolute() {
            return Err(EvidenceError::RelativePath(path.to_path_buf()));
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|error| io_error(path, error))?;
        let opened = directory
            .metadata()
            .map_err(|error| io_error(path, error))?;
        if !opened.is_dir() {
            return Err(EvidenceError::ParentNotDirectory(path.to_path_buf()));
        }

        // Resolve a diagnostic path only after the authoritative descriptor is open. Reopen that
        // canonical spelling without following symlinks and prove both descriptors name the same
        // inode, detecting a concurrent rename/replacement during acquisition.
        let canonical = fs::canonicalize(path).map_err(|error| io_error(path, error))?;
        let canonical_directory = open_absolute_directory_nofollow(&canonical)?;
        let canonical_metadata = canonical_directory
            .metadata()
            .map_err(|error| io_error(&canonical, error))?;
        if opened.dev() != canonical_metadata.dev() || opened.ino() != canonical_metadata.ino() {
            return Err(EvidenceError::FileChanged(path.to_path_buf()));
        }
        Ok(Self {
            directory,
            display_path: canonical,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.display_path
    }

    pub fn sync(&self) -> Result<(), EvidenceError> {
        self.directory
            .sync_all()
            .map_err(|error| io_error(&self.display_path, error))
    }

    pub fn mode(&self) -> Result<u32, EvidenceError> {
        self.directory
            .metadata()
            .map(|metadata| metadata.mode() & 0o7777)
            .map_err(|error| io_error(&self.display_path, error))
    }

    pub fn owner_uid(&self) -> Result<u32, EvidenceError> {
        self.directory
            .metadata()
            .map(|metadata| metadata.uid())
            .map_err(|error| io_error(&self.display_path, error))
    }

    /// Change and verify this pinned directory's mode without resolving its pathname again.
    pub fn set_mode(&self, mode: u32) -> Result<(), EvidenceError> {
        self.directory
            .set_permissions(fs::Permissions::from_mode(mode))
            .map_err(|error| io_error(&self.display_path, error))?;
        let metadata = self
            .directory
            .metadata()
            .map_err(|error| io_error(&self.display_path, error))?;
        if !metadata.is_dir() || metadata.mode() & 0o7777 != mode {
            return Err(EvidenceError::FileChanged(self.display_path.clone()));
        }
        self.directory
            .sync_all()
            .map_err(|error| io_error(&self.display_path, error))
    }

    /// Change and verify a pinned root-relative directory mode through no-follow descriptors.
    pub fn set_directory_mode(&self, relative: &Path, mode: u32) -> Result<(), EvidenceError> {
        validate_safe_relative(relative)?;
        let mut directory = self
            .directory
            .try_clone()
            .map_err(|error| io_error(&self.display_path, error))?;
        let mut display = self.display_path.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                unreachable!("validated relative path contains only normal components");
            };
            display.push(component);
            directory = open_directory_at(&directory, component, &display)?;
        }
        directory
            .set_permissions(fs::Permissions::from_mode(mode))
            .map_err(|error| io_error(&display, error))?;
        let metadata = directory
            .metadata()
            .map_err(|error| io_error(&display, error))?;
        if !metadata.is_dir() || metadata.mode() & 0o7777 != mode {
            return Err(EvidenceError::FileChanged(display));
        }
        directory
            .sync_all()
            .map_err(|error| io_error(&display, error))?;
        self.sync()
    }

    pub fn directory_mode_owner(&self, relative: &Path) -> Result<(u32, u32), EvidenceError> {
        validate_safe_relative(relative)?;
        let mut directory = self
            .directory
            .try_clone()
            .map_err(|error| io_error(&self.display_path, error))?;
        let mut display = self.display_path.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                unreachable!("validated relative path contains only normal components");
            };
            display.push(component);
            directory = open_directory_at(&directory, component, &display)?;
        }
        directory
            .metadata()
            .map(|metadata| (metadata.mode() & 0o7777, metadata.uid()))
            .map_err(|error| io_error(&display, error))
    }

    /// Return every regular-file and directory path beneath this pinned root.
    ///
    /// Paths are UTF-8, slash-separated, root-relative, and sorted. The root itself is omitted.
    pub fn collect_tree_paths(
        &self,
    ) -> Result<(BTreeSet<String>, BTreeSet<String>), EvidenceError> {
        let mut files = BTreeSet::new();
        let mut directories = BTreeSet::new();
        self.collect_tree_path_directory(
            &self.directory,
            Path::new(""),
            &mut files,
            &mut directories,
        )?;
        Ok((files, directories))
    }

    fn collect_tree_path_directory(
        &self,
        directory: &File,
        relative_directory: &Path,
        files: &mut BTreeSet<String>,
        directories: &mut BTreeSet<String>,
    ) -> Result<(), EvidenceError> {
        let display_directory = self.display_path.join(relative_directory);
        for leaf in read_directory_names(directory, &display_directory)? {
            let relative = relative_directory.join(&leaf);
            let display_path = self.display_path.join(&relative);
            let metadata = metadata_at(directory, &leaf, &display_path, libc::AT_SYMLINK_NOFOLLOW)?;
            let text = relative
                .to_str()
                .ok_or_else(|| EvidenceError::NonUtf8ManifestPath(relative.clone()))?
                .replace(std::path::MAIN_SEPARATOR, "/");
            match metadata.st_mode & libc::S_IFMT {
                libc::S_IFREG => {
                    files.insert(text);
                }
                libc::S_IFDIR => {
                    directories.insert(text);
                    let child = open_directory_at(directory, &leaf, &display_path)?;
                    self.collect_tree_path_directory(&child, &relative, files, directories)?;
                }
                libc::S_IFLNK => return Err(EvidenceError::ManifestSymlink(display_path)),
                _ => return Err(EvidenceError::NotRegularFile(display_path)),
            }
        }
        Ok(())
    }

    /// Check one root-relative entry without following its leaf or any parent symlink.
    pub fn entry_exists(&self, relative: &Path) -> Result<bool, EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        entry_exists_at(&parent, &leaf, &display_path)
    }

    /// Check whether one root-relative entry is a regular non-symlink file.
    pub fn entry_is_regular_file(&self, relative: &Path) -> Result<bool, EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        match metadata_at(&parent, &leaf, &display_path, libc::AT_SYMLINK_NOFOLLOW) {
            Ok(metadata) => Ok((metadata.st_mode & libc::S_IFMT) == libc::S_IFREG),
            Err(EvidenceError::Io { source, .. })
                if source.raw_os_error() == Some(libc::ENOENT) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Create one root-relative directory. Every existing parent is opened without symlink
    /// following, and the created directory is synced through its pinned descriptor.
    pub fn create_dir(&self, relative: &Path) -> Result<(), EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        let leaf_c = c_name(&leaf, &display_path)?;
        if unsafe { libc::mkdirat(parent.as_raw_fd(), leaf_c.as_ptr(), 0o700) } != 0 {
            return Err(io_error(&display_path, io::Error::last_os_error()));
        }
        parent
            .sync_all()
            .map_err(|error| io_error(&display_path, error))?;
        let created = open_directory_at(&parent, &leaf, &display_path)?;
        created
            .sync_all()
            .map_err(|error| io_error(&display_path, error))
    }

    /// Create and return a new regular file through the pinned directory tree.
    ///
    /// This is intended for bounded process-output drains: pass clones to child stdout/stderr,
    /// then `sync_all` the returned file after the child and drain tasks finish. The directory
    /// entry is create-new and its pinned parent is synced before this returns.
    pub fn create_new_file(&self, relative: &Path) -> Result<File, EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        let file = open_file_at(
            &parent,
            &leaf,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
            &display_path,
        )?;
        parent
            .sync_all()
            .map_err(|error| io_error(&display_path, error))?;
        Ok(file)
    }

    /// Descriptor-relative create-new publication with a same-directory staging inode.
    pub fn atomic_create_new_synced(
        &self,
        relative: &Path,
        bytes: &[u8],
    ) -> Result<(), EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        atomic_create_new_synced_at(&parent, &leaf, &display_path, bytes)
    }

    pub fn write_json_create_new<T: Serialize>(
        &self,
        relative: &Path,
        value: &T,
    ) -> Result<(), EvidenceError> {
        self.atomic_create_new_synced(relative, &canonical_json_line(value)?)
    }

    pub fn create_terminal<T: Serialize>(
        &self,
        relative: &Path,
        value: &T,
    ) -> Result<(), EvidenceError> {
        self.write_json_create_new(relative, value)
    }

    /// Append a diagnostic line through a pinned parent and no-follow leaf open.
    pub fn append_jsonl_event<T: Serialize>(
        &self,
        relative: &Path,
        value: &T,
    ) -> Result<(), EvidenceError> {
        let bytes = canonical_json_line(value)?;
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        let mut file = open_file_at(
            &parent,
            &leaf,
            libc::O_WRONLY | libc::O_APPEND | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
            &display_path,
        )?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io_error(&display_path, io::Error::last_os_error()));
        }
        let result = file
            .write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| io_error(&display_path, error));
        let unlock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        result?;
        if unlock_result != 0 {
            return Err(io_error(&display_path, io::Error::last_os_error()));
        }
        parent
            .sync_all()
            .map_err(|error| io_error(&display_path, error))
    }

    /// Read and hash a root-relative regular file without following any path-component symlink.
    pub fn stable_file_bytes(
        &self,
        relative: &Path,
    ) -> Result<(FileIdentity, Vec<u8>), EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        stable_file_bytes_at(&parent, &leaf, &display_path)
    }

    pub fn stable_file_identity(&self, relative: &Path) -> Result<FileIdentity, EvidenceError> {
        self.stable_file_bytes(relative)
            .map(|(identity, _)| identity)
    }

    /// Hash, verify, retain, and rewind one root-relative regular file descriptor.
    pub fn open_stable_file(&self, relative: &Path) -> Result<(FileIdentity, File), EvidenceError> {
        let (parent, leaf, display_path) = self.open_relative_parent(relative)?;
        let (identity, _, file) = stable_file_capture_at(&parent, &leaf, &display_path)?;
        Ok((identity, file))
    }

    /// Create a manifest by walking only descriptor-relative, no-follow directory handles.
    pub fn create_manifest_with_exclusions(
        &self,
        manifest_relative: &Path,
        excluded_paths: &[&str],
    ) -> Result<EvidenceManifest, EvidenceError> {
        validate_safe_relative(manifest_relative)?;
        let excluded = validate_excluded_paths(excluded_paths.iter().copied())?;
        let entries = self.collect_manifest_entries(manifest_relative, &excluded)?;
        let manifest = EvidenceManifest {
            schema: "distd.quic.fetch.evidence-manifest.v1".to_owned(),
            excluded_paths: excluded_paths
                .iter()
                .map(|path| (*path).to_string())
                .collect(),
            entries,
        };
        self.write_json_create_new(manifest_relative, &manifest)?;
        Ok(manifest)
    }

    /// Verify a manifest while retaining the same pinned root for the manifest read and walk.
    pub fn verify_manifest(
        &self,
        manifest_relative: &Path,
    ) -> Result<EvidenceManifest, EvidenceError> {
        validate_safe_relative(manifest_relative)?;
        let manifest_relative_text = manifest_relative
            .to_str()
            .ok_or_else(|| EvidenceError::NonUtf8ManifestPath(manifest_relative.to_path_buf()))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        let (_, bytes) = self.stable_file_bytes(manifest_relative)?;
        let manifest: EvidenceManifest = serde_json::from_slice(&bytes)?;
        if canonical_json_line(&manifest)? != bytes {
            return Err(EvidenceError::ManifestMismatch(
                "manifest bytes are not canonical JSON".to_string(),
            ));
        }
        if manifest.schema != "distd.quic.fetch.evidence-manifest.v1" {
            return Err(EvidenceError::ManifestSchema(manifest.schema));
        }
        let excluded = validate_excluded_paths(manifest.excluded_paths.iter().map(String::as_str))?;
        validate_declared_manifest_entries(&manifest, &manifest_relative_text)?;
        let actual = self.collect_manifest_entries(manifest_relative, &excluded)?;
        ensure_manifest_matches(&manifest, &actual)?;
        Ok(manifest)
    }

    fn collect_manifest_entries(
        &self,
        manifest_relative: &Path,
        excluded_relative: &BTreeSet<PathBuf>,
    ) -> Result<Vec<ManifestEntry>, EvidenceError> {
        let mut output = Vec::new();
        self.collect_manifest_directory(
            &self.directory,
            Path::new(""),
            manifest_relative,
            excluded_relative,
            &mut output,
        )?;
        output.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Ok(output)
    }

    fn collect_manifest_directory(
        &self,
        directory: &File,
        relative_directory: &Path,
        manifest_relative: &Path,
        additionally_excluded: &BTreeSet<PathBuf>,
        output: &mut Vec<ManifestEntry>,
    ) -> Result<(), EvidenceError> {
        let display_directory = self.display_path.join(relative_directory);
        for leaf in read_directory_names(directory, &display_directory)? {
            let relative = relative_directory.join(&leaf);
            if relative == manifest_relative || additionally_excluded.contains(&relative) {
                continue;
            }
            let display_path = self.display_path.join(&relative);
            let metadata = metadata_at(directory, &leaf, &display_path, libc::AT_SYMLINK_NOFOLLOW)?;
            let kind = metadata.st_mode & libc::S_IFMT;
            if kind == libc::S_IFLNK {
                return Err(EvidenceError::ManifestSymlink(display_path));
            }
            if kind == libc::S_IFDIR {
                let child = open_directory_at(directory, &leaf, &display_path)?;
                self.collect_manifest_directory(
                    &child,
                    &relative,
                    manifest_relative,
                    additionally_excluded,
                    output,
                )?;
            } else if kind == libc::S_IFREG {
                let identity = stable_file_bytes_at(directory, &leaf, &display_path)?.0;
                let relative_path = relative
                    .to_str()
                    .ok_or_else(|| EvidenceError::NonUtf8ManifestPath(relative.clone()))?
                    .replace(std::path::MAIN_SEPARATOR, "/");
                output.push(ManifestEntry {
                    relative_path,
                    sha256: identity.sha256,
                    length: identity.length,
                    device: identity.device,
                    inode: identity.inode,
                    modified_seconds: identity.modified_seconds,
                    modified_nanoseconds: identity.modified_nanoseconds,
                });
            } else {
                return Err(EvidenceError::NotRegularFile(display_path));
            }
        }
        Ok(())
    }

    fn open_relative_parent(
        &self,
        relative: &Path,
    ) -> Result<(File, OsString, PathBuf), EvidenceError> {
        validate_safe_relative(relative)?;
        let mut components: Vec<OsString> = relative
            .components()
            .map(|component| match component {
                Component::Normal(value) => value.to_os_string(),
                _ => unreachable!("validated relative path contains only normal components"),
            })
            .collect();
        let leaf = components
            .pop()
            .ok_or_else(|| EvidenceError::UnsafeRelativePath(relative.to_path_buf()))?;
        let mut directory = self
            .directory
            .try_clone()
            .map_err(|error| io_error(&self.display_path, error))?;
        let mut display = self.display_path.clone();
        for component in components {
            display.push(&component);
            directory = open_directory_at(&directory, &component, &display)?;
        }
        Ok((directory, leaf, self.display_path.join(relative)))
    }
}

fn validate_safe_relative(relative: &Path) -> Result<(), EvidenceError> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(EvidenceError::UnsafeRelativePath(relative.to_path_buf()));
    }
    Ok(())
}

fn c_name(name: &OsStr, display_path: &Path) -> Result<CString, EvidenceError> {
    CString::new(name.as_bytes()).map_err(|_| {
        io_error(
            display_path,
            io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"),
        )
    })
}

fn open_file_at(
    directory: &File,
    leaf: &OsStr,
    flags: libc::c_int,
    mode: libc::mode_t,
    display_path: &Path,
) -> Result<File, EvidenceError> {
    let leaf = c_name(leaf, display_path)?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            leaf.as_ptr(),
            flags,
            libc::c_uint::from(mode),
        )
    };
    if descriptor < 0 {
        return Err(io_error(display_path, io::Error::last_os_error()));
    }
    // SAFETY: `openat` returned a new owned descriptor above.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn open_directory_at(
    directory: &File,
    leaf: &OsStr,
    display_path: &Path,
) -> Result<File, EvidenceError> {
    open_file_at(
        directory,
        leaf,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
        display_path,
    )
}

fn open_absolute_directory_nofollow(path: &Path) -> Result<File, EvidenceError> {
    if !path.is_absolute() {
        return Err(EvidenceError::RelativePath(path.to_path_buf()));
    }
    let mut directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(Path::new("/"))
        .map_err(|error| io_error(Path::new("/"), error))?;
    let mut display = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(value) => {
                display.push(value);
                directory = open_directory_at(&directory, value, &display)?;
            }
            _ => return Err(EvidenceError::UnsafeRelativePath(path.to_path_buf())),
        }
    }
    Ok(directory)
}

fn metadata_at(
    directory: &File,
    leaf: &OsStr,
    display_path: &Path,
    flags: libc::c_int,
) -> Result<libc::stat, EvidenceError> {
    let leaf = c_name(leaf, display_path)?;
    // SAFETY: `stat` is plain data and `fstatat` initializes it on success.
    let mut metadata = unsafe { std::mem::zeroed::<libc::stat>() };
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            leaf.as_ptr(),
            &raw mut metadata,
            flags,
        )
    } != 0
    {
        return Err(io_error(display_path, io::Error::last_os_error()));
    }
    Ok(metadata)
}

fn entry_exists_at(
    directory: &File,
    leaf: &OsStr,
    display_path: &Path,
) -> Result<bool, EvidenceError> {
    match metadata_at(directory, leaf, display_path, libc::AT_SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(EvidenceError::Io { source, .. }) if source.raw_os_error() == Some(libc::ENOENT) => {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn atomic_create_new_synced_at(
    parent: &File,
    leaf: &OsStr,
    display_path: &Path,
    bytes: &[u8],
) -> Result<(), EvidenceError> {
    let mut temporary_name = None;
    let mut temporary_file = None;
    for _ in 0..128 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = OsString::from(format!(
            ".{}.{}.{}.partial",
            leaf.to_string_lossy(),
            std::process::id(),
            sequence
        ));
        match open_file_at(
            parent,
            &candidate,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
            &display_path.with_file_name(&candidate),
        ) {
            Ok(file) => {
                temporary_name = Some(candidate);
                temporary_file = Some(file);
                break;
            }
            Err(EvidenceError::Io { source, .. })
                if source.raw_os_error() == Some(libc::EEXIST) => {}
            Err(error) => return Err(error),
        }
    }
    let temporary_name = temporary_name.ok_or_else(|| {
        io_error(
            display_path,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "unable to reserve an evidence staging name",
            ),
        )
    })?;
    let Some(mut temporary_file) = temporary_file else {
        return Err(io_error(
            display_path,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "evidence staging file was not reserved",
            ),
        ));
    };
    let temporary_display = display_path.with_file_name(&temporary_name);
    let temporary_c = c_name(&temporary_name, &temporary_display)?;
    let leaf_c = c_name(leaf, display_path)?;

    let result = (|| {
        temporary_file
            .write_all(bytes)
            .map_err(|error| io_error(&temporary_display, error))?;
        temporary_file
            .sync_all()
            .map_err(|error| io_error(&temporary_display, error))?;
        let staged = temporary_file
            .metadata()
            .map_err(|error| io_error(&temporary_display, error))?;
        if unsafe {
            libc::linkat(
                parent.as_raw_fd(),
                temporary_c.as_ptr(),
                parent.as_raw_fd(),
                leaf_c.as_ptr(),
                0,
            )
        } != 0
        {
            return Err(io_error(display_path, io::Error::last_os_error()));
        }
        let target = metadata_at(parent, leaf, display_path, libc::AT_SYMLINK_NOFOLLOW)?;
        let target_device = u64::try_from(target.st_dev)
            .map_err(|_| EvidenceError::FileChanged(display_path.to_path_buf()))?;
        let target_length = u64::try_from(target.st_size)
            .map_err(|_| EvidenceError::FileChanged(display_path.to_path_buf()))?;
        if (target.st_mode & libc::S_IFMT) != libc::S_IFREG
            || staged.dev() != target_device
            || staged.ino() != target.st_ino
            || staged.len() != target_length
        {
            return Err(EvidenceError::FileChanged(display_path.to_path_buf()));
        }
        parent
            .sync_all()
            .map_err(|error| io_error(display_path, error))?;
        // The linked target is authoritative after its inode and parent are synced. Staging-name
        // cleanup is best effort and cannot change that committed target.
        let _ = unsafe { libc::unlinkat(parent.as_raw_fd(), temporary_c.as_ptr(), 0) };
        let _ = parent.sync_all();
        Ok(())
    })();

    if result.is_err() {
        let _ = unsafe { libc::unlinkat(parent.as_raw_fd(), temporary_c.as_ptr(), 0) };
    }
    result
}

fn stable_file_bytes_at(
    parent: &File,
    leaf: &OsStr,
    display_path: &Path,
) -> Result<(FileIdentity, Vec<u8>), EvidenceError> {
    let (identity, bytes, _) = stable_file_capture_at(parent, leaf, display_path)?;
    Ok((identity, bytes))
}

fn stable_file_capture_at(
    parent: &File,
    leaf: &OsStr,
    display_path: &Path,
) -> Result<(FileIdentity, Vec<u8>, File), EvidenceError> {
    let link_metadata = metadata_at(parent, leaf, display_path, libc::AT_SYMLINK_NOFOLLOW)?;
    if (link_metadata.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(EvidenceError::NotRegularFile(display_path.to_path_buf()));
    }
    let mut file = open_file_at(
        parent,
        leaf,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
        display_path,
    )
    .map_err(|_| EvidenceError::FileChanged(display_path.to_path_buf()))?;
    let before = file
        .metadata()
        .map_err(|error| io_error(display_path, error))?;
    if !before.is_file() {
        return Err(EvidenceError::NotRegularFile(display_path.to_path_buf()));
    }
    let before_identity = MetadataIdentity::from(&before);
    let capacity = usize::try_from(before.len()).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error(display_path, error))?;
    let sha256 = sha256_bytes(&bytes);
    let after = file
        .metadata()
        .map_err(|error| io_error(display_path, error))?;
    let after_identity = MetadataIdentity::from(&after);
    if before_identity != after_identity {
        return Err(EvidenceError::FileChanged(display_path.to_path_buf()));
    }

    // Reopen the leaf through the same pinned parent, proving the directory entry still names the
    // descriptor that was hashed. A symlink/replacement race fails closed here.
    let current = open_file_at(
        parent,
        leaf,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
        display_path,
    )
    .map_err(|_| EvidenceError::FileChanged(display_path.to_path_buf()))?;
    let current_metadata = current
        .metadata()
        .map_err(|_| EvidenceError::FileChanged(display_path.to_path_buf()))?;
    if MetadataIdentity::from(&current_metadata) != after_identity {
        return Err(EvidenceError::FileChanged(display_path.to_path_buf()));
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(display_path, error))?;

    Ok((
        FileIdentity {
            // This is a diagnostic spelling. The descriptor and identity fields, not this path,
            // remain authoritative if an ancestor is renamed after the root is pinned.
            canonical_path: display_path.to_path_buf(),
            sha256,
            device: before_identity.device,
            inode: before_identity.inode,
            length: before_identity.length,
            modified_seconds: before_identity.modified_seconds,
            modified_nanoseconds: before_identity.modified_nanoseconds,
            mode: before_identity.mode,
            owner_uid: before_identity.owner_uid,
        },
        bytes,
        file,
    ))
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: this wrapper exclusively owns the stream returned by `fdopendir`.
            let _ = unsafe { libc::closedir(self.0) };
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: libc returns the calling thread's errno pointer.
    unsafe { libc::__errno_location() }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: libc returns the calling thread's errno pointer.
    unsafe { libc::__error() }
}

fn read_directory_names(
    directory: &File,
    display_path: &Path,
) -> Result<Vec<OsString>, EvidenceError> {
    let current = c".";
    let duplicate = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            current.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if duplicate < 0 {
        return Err(io_error(display_path, io::Error::last_os_error()));
    }
    let raw_stream = unsafe { libc::fdopendir(duplicate) };
    if raw_stream.is_null() {
        // `fdopendir` assumes ownership only on success.
        let _ = unsafe { libc::close(duplicate) };
        return Err(io_error(display_path, io::Error::last_os_error()));
    }
    let stream = DirectoryStream(raw_stream);
    let mut names = Vec::new();
    loop {
        // SAFETY: errno is thread-local and the stream remains alive for this loop.
        unsafe { *errno_location() = 0 };
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let errno = unsafe { *errno_location() };
            if errno != 0 {
                return Err(io_error(display_path, io::Error::from_raw_os_error(errno)));
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        names.push(OsString::from_vec(name.to_vec()));
    }
    names.sort();
    Ok(names)
}

fn validate_declared_manifest_entries(
    manifest: &EvidenceManifest,
    manifest_relative_text: &str,
) -> Result<(), EvidenceError> {
    let mut declared = BTreeSet::new();
    for entry in &manifest.entries {
        if entry.relative_path == manifest_relative_text {
            return Err(EvidenceError::ManifestSelfEntry(
                entry.relative_path.clone(),
            ));
        }
        let relative = Path::new(&entry.relative_path);
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(EvidenceError::ManifestUnsafePath(
                entry.relative_path.clone(),
            ));
        }
        if !declared.insert(entry.relative_path.clone()) {
            return Err(EvidenceError::ManifestDuplicate(
                entry.relative_path.clone(),
            ));
        }
    }
    Ok(())
}

fn ensure_manifest_matches(
    manifest: &EvidenceManifest,
    actual: &[ManifestEntry],
) -> Result<(), EvidenceError> {
    if manifest.entries == actual {
        return Ok(());
    }
    let expected_paths: BTreeSet<_> = manifest
        .entries
        .iter()
        .map(|entry| entry.relative_path.as_str())
        .collect();
    let actual_paths: BTreeSet<_> = actual
        .iter()
        .map(|entry| entry.relative_path.as_str())
        .collect();
    let missing: Vec<_> = expected_paths.difference(&actual_paths).copied().collect();
    let extra: Vec<_> = actual_paths.difference(&expected_paths).copied().collect();
    let detail = if !missing.is_empty() || !extra.is_empty() {
        format!("missing={missing:?}, extra={extra:?}")
    } else {
        "one or more file hashes or identities changed".to_owned()
    };
    Err(EvidenceError::ManifestMismatch(detail))
}

/// Serialize as compact JSON with recursively sorted object keys and one trailing newline.
pub fn canonical_json_line<T: Serialize>(value: &T) -> Result<Vec<u8>, EvidenceError> {
    let value = serde_json::to_value(value)?;
    let canonical = canonicalize_json(value);
    let mut bytes = serde_json::to_vec(&canonical)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut values: Vec<_> = values.into_iter().collect();
            values.sort_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

/// Create a fully-written target atomically, refusing to overwrite an existing target.
///
/// A same-directory temporary inode is written and synced first. A hard link then publishes
/// the requested name atomically with create-new semantics.
pub fn atomic_create_new_synced(path: &Path, bytes: &[u8]) -> Result<(), EvidenceError> {
    let (parent, leaf) = pin_parent_for_path(path)?;
    atomic_create_new_synced_at(&parent.directory, &leaf, path, bytes)
}

pub fn write_json_create_new<T: Serialize>(path: &Path, value: &T) -> Result<(), EvidenceError> {
    atomic_create_new_synced(path, &canonical_json_line(value)?)
}

/// Create an immutable terminal receipt. A pre-existing terminal is never replaced.
pub fn create_terminal<T: Serialize>(path: &Path, value: &T) -> Result<(), EvidenceError> {
    write_json_create_new(path, value)
}

/// Append a non-authoritative diagnostic event under an exclusive advisory lock.
///
/// State transitions and receipts must use [`create_terminal`] or per-sequence create-new files;
/// append-only files are reserved for diagnostics because a process interruption can truncate the
/// last record.
pub fn append_jsonl_event<T: Serialize>(path: &Path, value: &T) -> Result<(), EvidenceError> {
    let (parent, leaf) = pin_parent_for_path(path)?;
    parent.append_jsonl_event(Path::new(&leaf), value)
}

pub fn sync_directory(path: &Path) -> Result<(), EvidenceError> {
    RootHandle::open(path)?.sync()
}

fn pin_parent_for_path(path: &Path) -> Result<(RootHandle, OsString), EvidenceError> {
    let leaf = match path.components().next_back() {
        Some(Component::Normal(leaf)) => leaf.to_os_string(),
        _ => return Err(EvidenceError::InvalidLeaf(path.to_path_buf())),
    };
    let parent = path
        .parent()
        .ok_or_else(|| EvidenceError::InvalidLeaf(path.to_path_buf()))?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| io_error(parent, error))?;
    Ok((RootHandle::open(&canonical_parent)?, leaf))
}

#[must_use]
pub fn sha256_bytes(bytes: &[u8]) -> String {
    lowercase_hex(ring::digest::digest(&SHA256, bytes).as_ref())
}

pub fn sha256_file(path: &Path) -> Result<String, EvidenceError> {
    stable_file_identity(path).map(|identity| identity.sha256)
}

fn lowercase_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIdentity {
    pub canonical_path: PathBuf,
    pub sha256: String,
    pub device: u64,
    pub inode: u64,
    pub length: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
    pub mode: u32,
    pub owner_uid: u32,
}

impl FileIdentity {
    #[must_use]
    pub fn same_file(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    mode: u32,
    owner_uid: u32,
}

impl From<&fs::Metadata> for MetadataIdentity {
    fn from(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
            mode: metadata.mode(),
            owner_uid: metadata.uid(),
        }
    }
}

/// Hash a regular file and prove its identity did not change across the read.
pub fn stable_file_identity(path: &Path) -> Result<FileIdentity, EvidenceError> {
    stable_file_bytes(path).map(|(identity, _)| identity)
}

/// Hash, verify, retain, and rewind one regular file descriptor.
///
/// The returned identity and descriptor refer to the same inode. The pathname entry is reopened
/// through its pinned parent after hashing, so a concurrent replacement fails closed.
pub fn open_stable_file(path: &Path) -> Result<(FileIdentity, File), EvidenceError> {
    let (parent, leaf) = pin_parent_for_path(path)?;
    let display_path = parent.path().join(&leaf);
    let (identity, _, file) = stable_file_capture_at(&parent.directory, &leaf, &display_path)
        .map_err(|error| remap_file_error_path(error, path))?;
    Ok((identity, file))
}

/// Read and hash one regular file through a single `O_NOFOLLOW` descriptor,
/// proving both descriptor and pathname identity stayed stable.
pub fn stable_file_bytes(path: &Path) -> Result<(FileIdentity, Vec<u8>), EvidenceError> {
    let (parent, leaf) = pin_parent_for_path(path)?;
    let display_path = parent.path().join(&leaf);
    stable_file_bytes_at(&parent.directory, &leaf, &display_path)
        .map_err(|error| remap_file_error_path(error, path))
}

fn remap_file_error_path(error: EvidenceError, requested: &Path) -> EvidenceError {
    match error {
        EvidenceError::NotRegularFile(_) => EvidenceError::NotRegularFile(requested.to_path_buf()),
        EvidenceError::FileChanged(_) => EvidenceError::FileChanged(requested.to_path_buf()),
        other => other,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub relative_path: String,
    pub sha256: String,
    pub length: u64,
    pub device: u64,
    pub inode: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceManifest {
    pub schema: String,
    /// Safe root-relative files intentionally published after this manifest.
    pub excluded_paths: Vec<String>,
    pub entries: Vec<ManifestEntry>,
}

/// Build and create a manifest that intentionally excludes its own path.
pub fn create_manifest_without_self(
    root: &Path,
    manifest_path: &Path,
) -> Result<EvidenceManifest, EvidenceError> {
    create_manifest_with_exclusions(root, manifest_path, &[])
}

/// Build and create a manifest while reserving exact safe relative paths for
/// later atomic publication (normally only `terminal.json`).
pub fn create_manifest_with_exclusions(
    root: &Path,
    manifest_path: &Path,
    excluded_paths: &[&str],
) -> Result<EvidenceManifest, EvidenceError> {
    let manifest_relative = manifest_relative_path(root, manifest_path)?;
    RootHandle::open(root)?.create_manifest_with_exclusions(&manifest_relative, excluded_paths)
}

/// Independently rehash and restat every manifest entry, rejecting tree drift.
pub fn verify_manifest(
    root: &Path,
    manifest_path: &Path,
) -> Result<EvidenceManifest, EvidenceError> {
    let manifest_relative = manifest_relative_path(root, manifest_path)?;
    RootHandle::open(root)?.verify_manifest(&manifest_relative)
}

fn manifest_relative_path(root: &Path, manifest_path: &Path) -> Result<PathBuf, EvidenceError> {
    if !root.is_absolute()
        || !manifest_path.is_absolute()
        || manifest_path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(EvidenceError::ManifestOutsideRoot(
            manifest_path.to_path_buf(),
        ));
    }
    let relative = manifest_path
        .strip_prefix(root)
        .map_err(|_| EvidenceError::ManifestOutsideRoot(manifest_path.to_path_buf()))?
        .to_path_buf();
    if validate_safe_relative(&relative).is_err() {
        return Err(EvidenceError::ManifestOutsideRoot(
            manifest_path.to_path_buf(),
        ));
    }
    Ok(relative)
}

fn validate_excluded_paths<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<PathBuf>, EvidenceError> {
    let mut validated = BTreeSet::new();
    for path in paths {
        let relative = Path::new(path);
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(EvidenceError::ManifestUnsafePath(path.to_string()));
        }
        if !validated.insert(relative.to_path_buf()) {
            return Err(EvidenceError::ManifestDuplicate(path.to_string()));
        }
    }
    Ok(validated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::symlink;
    use tempfile::tempdir;

    #[test]
    fn canonical_json_is_compact_sorted_and_newline_terminated() {
        let encoded = canonical_json_line(&json!({
            "z": 1,
            "a": {"y": 2, "b": 3},
            "m": [0, {"d": 4, "c": 5}]
        }))
        .unwrap();
        assert_eq!(
            encoded,
            br#"{"a":{"b":3,"y":2},"m":[0,{"c":5,"d":4}],"z":1}
"#
        );
    }

    #[test]
    fn atomic_create_new_never_replaces_existing_target() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("terminal.json");
        atomic_create_new_synced(&path, b"first\n").unwrap();
        let error = atomic_create_new_synced(&path, b"second\n").unwrap_err();
        assert!(matches!(error, EvidenceError::Io { .. }));
        assert_eq!(fs::read(path).unwrap(), b"first\n");
    }

    #[test]
    fn jsonl_events_append_complete_canonical_lines() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("events.jsonl");
        append_jsonl_event(&path, &json!({"b": 2, "a": 1})).unwrap();
        append_jsonl_event(&path, &json!({"event": "done"})).unwrap();
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            "{\"a\":1,\"b\":2}\n{\"event\":\"done\"}\n"
        );
    }

    #[test]
    fn absent_root_uses_canonical_parent_and_does_not_create_leaf() {
        let temporary = tempdir().unwrap();
        let parent = fs::canonicalize(temporary.path()).unwrap();
        let requested = parent.join("new-evidence");
        let absent = canonical_absent_root(&requested).unwrap();
        assert_eq!(absent.canonical_parent, parent);
        assert_eq!(absent.leaf, PathBuf::from("new-evidence"));
        assert_eq!(absent.canonical_path, requested);
        assert!(!absent.canonical_path.exists());
    }

    #[test]
    fn pinned_absent_root_survives_parent_rename_and_symlink_replacement() {
        let temporary = tempdir().unwrap();
        let parent = temporary.path().join("validated-parent");
        let moved_parent = temporary.path().join("moved-parent");
        let attacker = temporary.path().join("attacker");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&attacker).unwrap();

        let requested = parent.join("evidence");
        let pinned = pin_absent_root(&requested).unwrap();
        fs::rename(&parent, &moved_parent).unwrap();
        symlink(&attacker, &parent).unwrap();

        let root = pinned.create_root().unwrap();
        root.atomic_create_new_synced(Path::new("receipt"), b"pinned\n")
            .unwrap();

        assert_eq!(
            fs::read(moved_parent.join("evidence/receipt")).unwrap(),
            b"pinned\n"
        );
        assert!(!attacker.join("evidence").exists());
    }

    #[test]
    fn root_handle_writes_and_walks_pinned_tree_after_root_symlink_swap() {
        let temporary = tempdir().unwrap();
        let requested = temporary.path().join("evidence");
        let moved = temporary.path().join("moved-evidence");
        let attacker = temporary.path().join("attacker");
        fs::create_dir(&attacker).unwrap();
        fs::write(attacker.join("decoy"), b"outside").unwrap();

        let root = pin_absent_root(&requested).unwrap().create_root().unwrap();
        root.create_dir(Path::new("runs")).unwrap();
        root.atomic_create_new_synced(Path::new("payload"), b"trusted")
            .unwrap();
        fs::rename(&requested, &moved).unwrap();
        symlink(&attacker, &requested).unwrap();

        root.create_dir(Path::new("runs/0001")).unwrap();
        let mut output = root
            .create_new_file(Path::new("runs/0001/stdout.log"))
            .unwrap();
        output.write_all(b"bounded output").unwrap();
        output.sync_all().unwrap();
        drop(output);
        let manifest = root
            .create_manifest_with_exclusions(Path::new("manifest.json"), &[])
            .unwrap();
        assert_eq!(
            manifest
                .entries
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["payload", "runs/0001/stdout.log"]
        );
        assert_eq!(
            root.verify_manifest(Path::new("manifest.json")).unwrap(),
            manifest
        );

        assert_eq!(fs::read(moved.join("payload")).unwrap(), b"trusted");
        assert!(moved.join("manifest.json").is_file());
        assert_eq!(fs::read(attacker.join("decoy")).unwrap(), b"outside");
        assert!(!attacker.join("payload").exists());
        assert!(!attacker.join("manifest.json").exists());
        assert!(!attacker.join("runs").exists());
    }

    #[test]
    fn stable_identity_records_hash_and_rejects_symlink() {
        let temporary = tempdir().unwrap();
        let regular = temporary.path().join("binary");
        fs::write(&regular, b"stable bytes").unwrap();
        let identity = stable_file_identity(&regular).unwrap();
        assert_eq!(identity.sha256, sha256_bytes(b"stable bytes"));
        assert_eq!(identity.length, 12);

        let linked = temporary.path().join("linked");
        symlink(&regular, &linked).unwrap();
        let linked_error = stable_file_identity(&linked).unwrap_err();
        assert!(
            matches!(
                linked_error,
                EvidenceError::NotRegularFile(ref path) if path == &linked
            ),
            "unexpected symlink error: {linked_error:?}"
        );
    }

    #[test]
    fn open_stable_file_retains_hashed_inode_after_path_replacement() {
        let temporary = tempdir().unwrap();
        let executable = temporary.path().join("executable");
        let moved = temporary.path().join("verified-executable");
        fs::write(&executable, b"verified bytes").unwrap();
        let (identity, mut file) = open_stable_file(&executable).unwrap();

        fs::rename(&executable, &moved).unwrap();
        fs::write(&executable, b"attacker bytes").unwrap();
        let mut retained = Vec::new();
        file.read_to_end(&mut retained).unwrap();

        assert_eq!(identity.sha256, sha256_bytes(b"verified bytes"));
        assert_eq!(retained, b"verified bytes");
        assert_eq!(fs::read(executable).unwrap(), b"attacker bytes");
    }

    #[test]
    fn manifest_is_sorted_and_does_not_hash_itself() {
        let temporary = tempdir().unwrap();
        let nested = temporary.path().join("nested");
        fs::create_dir(&nested).unwrap();
        fs::write(temporary.path().join("z.txt"), b"z").unwrap();
        fs::write(nested.join("a.txt"), b"a").unwrap();
        let manifest_path = temporary.path().join("evidence-manifest.json");

        let manifest = create_manifest_without_self(temporary.path(), &manifest_path).unwrap();
        let paths: Vec<_> = manifest
            .entries
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect();
        assert_eq!(paths, vec!["nested/a.txt", "z.txt"]);
        assert!(manifest_path.is_file());
        assert!(manifest
            .entries
            .iter()
            .all(|entry| entry.relative_path != "evidence-manifest.json"));
        assert_eq!(
            verify_manifest(temporary.path(), &manifest_path).unwrap(),
            manifest
        );
    }

    #[test]
    fn manifest_verification_rejects_hash_drift_and_extras() {
        let temporary = tempdir().unwrap();
        let payload = temporary.path().join("payload");
        fs::write(&payload, b"original").unwrap();
        let manifest_path = temporary.path().join("manifest.json");
        create_manifest_without_self(temporary.path(), &manifest_path).unwrap();

        fs::write(&payload, b"changed").unwrap();
        assert!(matches!(
            verify_manifest(temporary.path(), &manifest_path),
            Err(EvidenceError::ManifestMismatch(_))
        ));

        fs::write(&payload, b"original").unwrap();
        fs::write(temporary.path().join("extra"), b"extra").unwrap();
        assert!(matches!(
            verify_manifest(temporary.path(), &manifest_path),
            Err(EvidenceError::ManifestMismatch(_))
        ));
    }

    #[test]
    fn manifest_creation_rejects_parent_escape_before_writing() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("root");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("payload"), b"payload").unwrap();
        let escaped = root.join("..").join("escaped-manifest.json");
        assert!(matches!(
            create_manifest_without_self(&root, &escaped),
            Err(EvidenceError::ManifestOutsideRoot(path)) if path == escaped
        ));
        assert!(!temporary.path().join("escaped-manifest.json").exists());
    }

    #[test]
    fn root_handle_manifest_rejects_parent_escape_before_writing() {
        let temporary = tempdir().unwrap();
        let root_path = temporary.path().join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("payload"), b"payload").unwrap();
        let root = RootHandle::open(&root_path).unwrap();

        assert!(matches!(
            root.create_manifest_with_exclusions(Path::new("../escaped-manifest.json"), &[]),
            Err(EvidenceError::UnsafeRelativePath(path))
                if path == PathBuf::from("../escaped-manifest.json")
        ));
        assert!(!temporary.path().join("escaped-manifest.json").exists());
    }

    #[test]
    fn verifier_rejects_declared_self_entry() {
        let temporary = tempdir().unwrap();
        let manifest_path = temporary.path().join("manifest.json");
        let manifest = EvidenceManifest {
            schema: "distd.quic.fetch.evidence-manifest.v1".to_owned(),
            excluded_paths: Vec::new(),
            entries: vec![ManifestEntry {
                relative_path: "manifest.json".to_owned(),
                sha256: "0".repeat(64),
                length: 0,
                device: 0,
                inode: 0,
                modified_seconds: 0,
                modified_nanoseconds: 0,
            }],
        };
        write_json_create_new(&manifest_path, &manifest).unwrap();
        assert!(matches!(
            verify_manifest(temporary.path(), &manifest_path),
            Err(EvidenceError::ManifestSelfEntry(path)) if path == "manifest.json"
        ));
    }

    #[test]
    fn diagnostic_append_refuses_symlink_target() {
        let temporary = tempdir().unwrap();
        let target = temporary.path().join("target");
        fs::write(&target, b"untouched").unwrap();
        let linked = temporary.path().join("events.jsonl");
        symlink(&target, &linked).unwrap();
        assert!(append_jsonl_event(&linked, &json!({"event": 1})).is_err());
        assert_eq!(fs::read(target).unwrap(), b"untouched");
    }
}
