//! Deterministic `composefs` (EROFS v1 + OverlayFS metacopy/redirect) metadata image generator.
//!
//! Projects a verified [`Item`] revision manifest into a compact, read-only EROFS v1 image where
//! every regular file inode either:
//! - carries `trusted.overlay.redirect = "/<2-hex-prefix>/<file_blob_hash>"` and
//!   `trusted.overlay.metacopy` extended attributes pointing into `FsStorage`'s native CAS blob
//!   store (`.distd-activation-v1/blobs`), or
//! - stores its payload inline (`EROFS_INODE_FLAT_INLINE`) when small inline file bytes are
//!   provided.
//!
//! Executable entries get the executable permission bits, and symbolic links become `S_IFLNK`
//! inodes whose signed target is stored inline (or in one data block when it cannot share the
//! inode's block).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::chunk_storage::fs_storage::FsStorage;
use crate::chunk_storage::validate_revision_metadata;
use crate::chunks::ChunkInfo;
use crate::error::{Error, InvalidParameter};
use crate::hash::Hash;
use crate::item::{EntryKind, Item};

/// EROFS v1 superblock magic (`0xe0f5e1e2`).
pub const EROFS_SUPER_MAGIC_V1: u32 = 0xe0f5_e1e2;
/// Byte offset of the EROFS superblock from the start of the image.
pub const EROFS_SUPER_OFFSET: usize = 1024;
/// Size of the EROFS superblock in bytes.
pub const EROFS_SUPER_SIZE: usize = 128;
/// Block size (4096 bytes, `blkszbits = 12`).
pub const EROFS_BLOCK_SIZE: usize = 4096;
/// Size of an EROFS NID slot in bytes.
pub const EROFS_SLOT_SIZE: usize = 32;
/// Size of an extended EROFS inode (`erofs_inode_extended`) in bytes.
pub const EROFS_INODE_EXTENDED_SIZE: usize = 64;

const EROFS_INODE_LAYOUT_PLAIN: u16 = 0;
const EROFS_INODE_LAYOUT_INLINE: u16 = 2;
const EROFS_NULL_ADDR: u32 = 0xffff_ffff;

const EROFS_FT_REG_FILE: u8 = 1;
const EROFS_FT_DIR: u8 = 2;
const EROFS_FT_SYMLINK: u8 = 7;

/// `EROFS_XATTR_INDEX_TRUSTED` (`"trusted."` prefix).
pub const EROFS_XATTR_INDEX_TRUSTED: u8 = 4;
const XATTR_OVERLAY_REDIRECT_SUFFIX: &[u8] = b"overlay.redirect";
const XATTR_OVERLAY_METACOPY_SUFFIX: &[u8] = b"overlay.metacopy";

const S_IFMT: u16 = 0o170000;
const S_IFDIR: u16 = 0o040000;
const S_IFREG: u16 = 0o100000;
const S_IFLNK: u16 = 0o120000;

/// Options controlling `composefs` EROFS image generation.
#[derive(Clone, Debug)]
pub struct ComposefsOptions {
    /// File mode bits (low 12 bits) for regular files (default `0o444`).
    pub file_permissions: u16,
    /// File mode bits (low 12 bits) for executable files (default `0o555`).
    pub executable_permissions: u16,
    /// Directory mode bits (low 12 bits) for directories (default `0o555`).
    pub dir_permissions: u16,
    /// Deterministic mtime seconds since UNIX_EPOCH (default `0`).
    pub mtime_sec: u64,
    /// Maximum file size to inline directly into the EROFS inode tail when inline bytes are
    /// supplied (default `256`).
    pub max_inline_bytes: usize,
    /// Optional map from relative file path (or `""` for single-file items) to raw bytes for
    /// inlining small files directly into the EROFS image.
    pub inline_files: BTreeMap<String, Vec<u8>>,
}

impl Default for ComposefsOptions {
    fn default() -> Self {
        Self {
            file_permissions: 0o444,
            executable_permissions: 0o555,
            dir_permissions: 0o555,
            mtime_sec: 0,
            max_inline_bytes: 256,
            inline_files: BTreeMap::new(),
        }
    }
}

/// Summary of a materialized file entry inside a `composefs` EROFS image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComposefsFileProjection {
    /// Relative path inside the image (`""` when the root itself is a single regular file wrapped
    /// under its destination filename).
    pub relative_path: String,
    /// Logical file size in bytes.
    pub size: u64,
    /// Content-addressed whole-file blob hash in `FsStorage`.
    pub blob_hash: Hash,
    /// OverlayFS redirect path (`/<2-hex-prefix>/<blob_hash>`) when backed by the CAS blob store,
    /// or `None` when inlined directly in the EROFS image.
    pub redirect_path: Option<String>,
    /// Inline payload stored inside the EROFS inode, if inlined.
    pub inline_data: Option<Vec<u8>>,
    /// Whether the inode carries executable permission bits.
    pub executable: bool,
    /// Assigned EROFS 32-byte slot NID.
    pub nid: u64,
}

/// Summary of a symbolic link inside a `composefs` EROFS image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComposefsSymlinkProjection {
    /// Relative path inside the image.
    pub relative_path: String,
    /// Link target, exactly as signed in the manifest.
    pub target: String,
    /// Assigned EROFS 32-byte slot NID.
    pub nid: u64,
}

/// Parsed metadata and projections from a generated `composefs` EROFS image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComposefsImageInfo {
    /// Root directory EROFS NID.
    pub root_nid: u16,
    /// Total number of inodes in the image.
    pub total_inodes: u64,
    /// Total number of 4 KiB blocks in the image.
    pub total_blocks: u32,
    /// Deterministic volume UUID derived from the revision root hash.
    pub uuid: [u8; 16],
    /// Projected regular files in canonical path order.
    pub files: Vec<ComposefsFileProjection>,
    /// Projected symbolic links in canonical path order.
    pub symlinks: Vec<ComposefsSymlinkProjection>,
}

#[derive(Debug)]
struct DirBuilderNode {
    subdirs: BTreeMap<String, DirBuilderNode>,
    files: BTreeMap<String, NodeSpec>,
}

impl DirBuilderNode {
    fn new() -> Self {
        Self {
            subdirs: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct FileSpec {
    relative_path: String,
    size: u64,
    blob_hash: Hash,
    redirect_path: Option<String>,
    inline_data: Option<Vec<u8>>,
    executable: bool,
}

#[derive(Clone, Debug)]
struct SymlinkSpec {
    relative_path: String,
    target: String,
}

#[derive(Clone, Debug)]
enum NodeSpec {
    File(FileSpec),
    Symlink(SymlinkSpec),
}

#[derive(Clone, Debug)]
enum PlannedNodeKind {
    Directory {
        children: Vec<(String, usize, u8)>,
        nlink: u32,
    },
    File(FileSpec),
    /// A symbolic link whose target does not fit inline and needs one data block.
    SymlinkBlock(SymlinkSpec),
    SymlinkInline(SymlinkSpec),
}

#[derive(Clone, Debug)]
struct PlannedNode {
    ino: u32,
    kind: PlannedNodeKind,
    encoded_inode_and_xattrs: Vec<u8>,
    inline_tail: Vec<u8>,
    nid: u64,
    inode_offset: usize,
    data_start_block: u32,
    data_size: u64,
}

fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

fn encode_trusted_xattr_entry(name_suffix: &[u8], value: &[u8]) -> Result<Vec<u8>, Error> {
    let name_len = u8::try_from(name_suffix.len()).map_err(InvalidParameter::from)?;
    let value_len = u16::try_from(value.len()).map_err(InvalidParameter::from)?;
    let raw_len = 4 + name_suffix.len() + value.len();
    let padded_len = align_up(raw_len, 4);
    let mut buf = Vec::with_capacity(padded_len);
    buf.push(name_len);
    buf.push(EROFS_XATTR_INDEX_TRUSTED);
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(name_suffix);
    buf.extend_from_slice(value);
    buf.resize(padded_len, 0);
    Ok(buf)
}

fn encode_redirect_xattr_body(redirect_path: &str) -> Result<(u16, Vec<u8>), Error> {
    let mut body = vec![0u8; 12]; // erofs_xattr_ibody_header
    let redirect_entry =
        encode_trusted_xattr_entry(XATTR_OVERLAY_REDIRECT_SUFFIX, redirect_path.as_bytes())?;
    let metacopy_entry = encode_trusted_xattr_entry(XATTR_OVERLAY_METACOPY_SUFFIX, b"")?;
    body.extend_from_slice(&redirect_entry);
    body.extend_from_slice(&metacopy_entry);
    debug_assert_eq!((body.len() - 12) % 4, 0);
    let icount = u16::try_from(1 + (body.len() - 12) / 4).map_err(InvalidParameter::from)?;
    Ok((icount, body))
}

#[allow(clippy::too_many_arguments)]
fn encode_extended_inode(
    layout: u16,
    xattr_icount: u16,
    mode: u16,
    nlink: u32,
    size: u64,
    u_blkaddr: u32,
    ino: u32,
    mtime_sec: u64,
) -> [u8; EROFS_INODE_EXTENDED_SIZE] {
    let mut buf = [0u8; EROFS_INODE_EXTENDED_SIZE];
    let i_format = 1u16 | ((layout & 0x7) << 1);
    buf[0..2].copy_from_slice(&i_format.to_le_bytes());
    buf[2..4].copy_from_slice(&xattr_icount.to_le_bytes());
    buf[4..6].copy_from_slice(&mode.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
    buf[8..12].copy_from_slice(&nlink.to_le_bytes());
    buf[12..20].copy_from_slice(&size.to_le_bytes());
    buf[20..24].copy_from_slice(&0u32.to_le_bytes());
    buf[24..28].copy_from_slice(&u_blkaddr.to_le_bytes());
    buf[28..32].copy_from_slice(&ino.to_le_bytes());
    buf[32..36].copy_from_slice(&0u32.to_le_bytes()); // uid
    buf[36..40].copy_from_slice(&0u32.to_le_bytes()); // gid
    buf[40..48].copy_from_slice(&mtime_sec.to_le_bytes());
    buf[48..52].copy_from_slice(&0u32.to_le_bytes()); // mtime_nsec
    buf
}

fn encode_directory_blocks(
    self_nid: u64,
    parent_nid: u64,
    children: &[(String, u64, u8)],
) -> Result<Vec<u8>, Error> {
    let mut all_entries: Vec<(&[u8], u64, u8)> = Vec::with_capacity(2 + children.len());
    all_entries.push((b".", self_nid, EROFS_FT_DIR));
    all_entries.push((b"..", parent_nid, EROFS_FT_DIR));
    let mut sorted_children = children.to_vec();
    sorted_children.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    for (name, nid, ft) in &sorted_children {
        all_entries.push((name.as_bytes(), *nid, *ft));
    }

    let mut blocks = Vec::new();
    let mut idx = 0usize;
    while idx < all_entries.len() {
        let start_idx = idx;
        let mut header_bytes = 0usize;
        let mut names_bytes = 0usize;
        while idx < all_entries.len() {
            let name_len = all_entries[idx].0.len();
            if name_len == 0 || name_len > 255 {
                return Err(Error::Other(
                    "composefs directory entry name must be 1..=255 bytes".to_string(),
                ));
            }
            if header_bytes + 12 + names_bytes + name_len > EROFS_BLOCK_SIZE {
                break;
            }
            header_bytes += 12;
            names_bytes += name_len;
            idx += 1;
        }
        if idx == start_idx {
            return Err(Error::Other(
                "composefs directory entry exceeds EROFS block size".to_string(),
            ));
        }

        let mut block = vec![0u8; EROFS_BLOCK_SIZE];
        let mut name_offset = header_bytes;
        for (slot, (name, nid, file_type)) in all_entries[start_idx..idx].iter().enumerate() {
            let dirent_off = slot * 12;
            let nameoff_u16 = u16::try_from(name_offset).map_err(InvalidParameter::from)?;
            block[dirent_off..dirent_off + 8].copy_from_slice(&nid.to_le_bytes());
            block[dirent_off + 8..dirent_off + 10].copy_from_slice(&nameoff_u16.to_le_bytes());
            block[dirent_off + 10] = *file_type;
            block[dirent_off + 11] = 0;
            block[name_offset..name_offset + name.len()].copy_from_slice(name);
            name_offset += name.len();
        }
        blocks.extend_from_slice(&block);
    }

    Ok(blocks)
}

fn insert_file_into_tree(
    root: &mut DirBuilderNode,
    relative_path: &Path,
    spec: NodeSpec,
) -> Result<(), Error> {
    let canonical = crate::utils::path::validate_relative(relative_path)?;
    let components: Vec<String> = canonical
        .components()
        .map(|comp| {
            comp.as_os_str()
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::Other("composefs path must be valid UTF-8".to_string()))
        })
        .collect::<Result<_, _>>()?;
    if components.is_empty() {
        return Err(Error::Other(
            "composefs entry path must not be empty".to_string(),
        ));
    }
    let mut current = root;
    for dir_name in &components[..components.len() - 1] {
        if current.files.contains_key(dir_name) {
            return Err(Error::Other(format!(
                "composefs path conflict at component '{dir_name}'"
            )));
        }
        current = current
            .subdirs
            .entry(dir_name.clone())
            .or_insert_with(DirBuilderNode::new);
    }
    let file_name = components.last().unwrap().clone();
    if current.subdirs.contains_key(&file_name) || current.files.contains_key(&file_name) {
        return Err(Error::Other(format!(
            "composefs duplicate entry '{file_name}'"
        )));
    }
    current.files.insert(file_name, spec);
    Ok(())
}

fn flatten_tree(
    node: &DirBuilderNode,
    options: &ComposefsOptions,
    nodes: &mut Vec<PlannedNode>,
) -> Result<usize, Error> {
    let dir_index = nodes.len();
    let ino = u32::try_from(dir_index + 1).map_err(InvalidParameter::from)?;
    let nlink = u32::try_from(2 + node.subdirs.len()).map_err(InvalidParameter::from)?;
    nodes.push(PlannedNode {
        ino,
        kind: PlannedNodeKind::Directory {
            children: Vec::new(),
            nlink,
        },
        encoded_inode_and_xattrs: vec![0u8; EROFS_INODE_EXTENDED_SIZE],
        inline_tail: Vec::new(),
        nid: 0,
        inode_offset: 0,
        data_start_block: 0,
        data_size: 0,
    });

    let mut children = Vec::with_capacity(node.subdirs.len() + node.files.len());
    for (name, subdir) in &node.subdirs {
        let child_idx = flatten_tree(subdir, options, nodes)?;
        children.push((name.clone(), child_idx, EROFS_FT_DIR));
    }
    for (name, spec) in &node.files {
        let spec = match spec {
            NodeSpec::File(spec) => spec,
            NodeSpec::Symlink(link) => {
                let link_idx = plan_symlink(link, options, nodes)?;
                children.push((name.clone(), link_idx, EROFS_FT_SYMLINK));
                continue;
            }
        };
        let file_idx = nodes.len();
        let file_ino = u32::try_from(file_idx + 1).map_err(InvalidParameter::from)?;
        let (xattr_icount, xattr_body, inline_tail) = if let Some(inline_bytes) = &spec.inline_data
        {
            (0u16, Vec::new(), inline_bytes.clone())
        } else {
            let redirect = spec.redirect_path.as_deref().ok_or_else(|| {
                Error::Other("non-inline composefs file requires redirect path".to_string())
            })?;
            let (icount, body) = encode_redirect_xattr_body(redirect)?;
            (icount, body, Vec::new())
        };
        let mut encoded = Vec::with_capacity(EROFS_INODE_EXTENDED_SIZE + xattr_body.len());
        encoded.resize(EROFS_INODE_EXTENDED_SIZE, 0);
        encoded.extend_from_slice(&xattr_body);
        nodes.push(PlannedNode {
            ino: file_ino,
            kind: PlannedNodeKind::File(spec.clone()),
            encoded_inode_and_xattrs: encoded,
            inline_tail,
            nid: 0,
            inode_offset: 0,
            data_start_block: EROFS_NULL_ADDR,
            data_size: spec.size,
        });
        // Pre-encode file inode now since its fields do not depend on block addresses.
        let layout = if spec.inline_data.is_some() {
            EROFS_INODE_LAYOUT_INLINE
        } else {
            EROFS_INODE_LAYOUT_PLAIN
        };
        let permissions = if spec.executable {
            options.executable_permissions
        } else {
            options.file_permissions
        };
        let mode = S_IFREG | (permissions & 0o7777);
        let inode_bytes = encode_extended_inode(
            layout,
            xattr_icount,
            mode,
            1,
            spec.size,
            EROFS_NULL_ADDR,
            file_ino,
            options.mtime_sec,
        );
        nodes[file_idx].encoded_inode_and_xattrs[..EROFS_INODE_EXTENDED_SIZE]
            .copy_from_slice(&inode_bytes);
        children.push((name.clone(), file_idx, EROFS_FT_REG_FILE));
    }

    if let PlannedNodeKind::Directory { children: slot, .. } = &mut nodes[dir_index].kind {
        *slot = children;
    }
    Ok(dir_index)
}

/// Plan a symbolic-link inode. Its inode is final unless the target needs a data block, whose
/// address is filled in once directory and link data blocks are laid out.
fn plan_symlink(
    link: &SymlinkSpec,
    options: &ComposefsOptions,
    nodes: &mut Vec<PlannedNode>,
) -> Result<usize, Error> {
    let link_idx = nodes.len();
    let ino = u32::try_from(link_idx + 1).map_err(InvalidParameter::from)?;
    let target = link.target.as_bytes();
    let inline = EROFS_INODE_EXTENDED_SIZE + target.len() <= EROFS_BLOCK_SIZE;
    let (layout, inline_tail, kind) = if inline {
        (
            EROFS_INODE_LAYOUT_INLINE,
            target.to_vec(),
            PlannedNodeKind::SymlinkInline(link.clone()),
        )
    } else {
        (
            EROFS_INODE_LAYOUT_PLAIN,
            Vec::new(),
            PlannedNodeKind::SymlinkBlock(link.clone()),
        )
    };
    let inode_bytes = encode_extended_inode(
        layout,
        0,
        S_IFLNK | 0o777,
        1,
        target.len() as u64,
        EROFS_NULL_ADDR,
        ino,
        options.mtime_sec,
    );
    nodes.push(PlannedNode {
        ino,
        kind,
        encoded_inode_and_xattrs: inode_bytes.to_vec(),
        inline_tail,
        nid: 0,
        inode_offset: 0,
        data_start_block: EROFS_NULL_ADDR,
        data_size: target.len() as u64,
    });
    Ok(link_idx)
}

fn build_file_spec(
    relative_path: String,
    size: u64,
    file_chunks: &[ChunkInfo],
    executable: bool,
    options: &ComposefsOptions,
) -> Result<FileSpec, Error> {
    let blob_hash = FsStorage::file_blob_hash(file_chunks)?;
    if size <= options.max_inline_bytes as u64 {
        if let Some(inline_bytes) = options.inline_files.get(&relative_path) {
            if inline_bytes.len() as u64 != size {
                return Err(Error::Other(format!(
                    "composefs inline payload for '{relative_path}' does not match declared size"
                )));
            }
            return Ok(FileSpec {
                relative_path,
                size,
                blob_hash,
                redirect_path: None,
                inline_data: Some(inline_bytes.clone()),
                executable,
            });
        }
    }
    let rel_blob = FsStorage::cas_blob_relative_path(&blob_hash);
    let redirect_path = format!("/{}", rel_blob.to_string_lossy().replace('\\', "/"));
    Ok(FileSpec {
        relative_path,
        size,
        blob_hash,
        redirect_path: Some(redirect_path),
        inline_data: None,
        executable,
    })
}

/// Build a deterministic `composefs` EROFS v1 metadata image from a verified [`Item`].
pub fn build_composefs_erofs(
    item: &Item,
    options: &ComposefsOptions,
) -> Result<(Vec<u8>, ComposefsImageInfo), Error> {
    validate_revision_metadata(item)?;

    let mut root_tree = DirBuilderNode::new();
    if item.manifest.entries.is_empty() {
        let file_name = item
            .metadata
            .path
            .file_name()
            .and_then(|os| os.to_str())
            .ok_or_else(|| {
                Error::Other("single-file revision path must have a UTF-8 file name".to_string())
            })?
            .to_string();
        let spec = build_file_spec(
            file_name.clone(),
            item.manifest.total_size,
            &item.chunks,
            false,
            options,
        )?;
        insert_file_into_tree(&mut root_tree, Path::new(&file_name), NodeSpec::File(spec))?;
    } else {
        for entry in &item.manifest.entries {
            let start = entry.chunk_range.0 as usize;
            let end = entry.chunk_range.1 as usize;
            let file_chunks = item.chunks.get(start..end).ok_or_else(|| {
                Error::Other("manifest entry chunk range out of bounds".to_string())
            })?;
            let spec = match &entry.kind {
                EntryKind::Symlink { target } => NodeSpec::Symlink(SymlinkSpec {
                    relative_path: entry.relative_path.clone(),
                    target: target.clone(),
                }),
                EntryKind::Regular | EntryKind::Executable => NodeSpec::File(build_file_spec(
                    entry.relative_path.clone(),
                    entry.size,
                    file_chunks,
                    entry.kind == EntryKind::Executable,
                    options,
                )?),
            };
            insert_file_into_tree(&mut root_tree, Path::new(&entry.relative_path), spec)?;
        }
    }

    let mut nodes = Vec::new();
    let root_idx = flatten_tree(&root_tree, options, &mut nodes)?;
    debug_assert_eq!(root_idx, 0);

    // Reserve bytes 0..2048 in block 0 for boot sector (0..1024) + superblock (1024..2048),
    // so the root inode starts at byte offset 2048 (`nid = 64`).
    let mut cursor = 2048usize;
    for node in &mut nodes {
        let total_node_bytes = node.encoded_inode_and_xattrs.len() + node.inline_tail.len();
        if !node.inline_tail.is_empty() {
            let block_rem = EROFS_BLOCK_SIZE - (cursor % EROFS_BLOCK_SIZE);
            if total_node_bytes > block_rem {
                cursor = align_up(cursor, EROFS_BLOCK_SIZE);
            }
        }
        node.inode_offset = cursor;
        node.nid = u64::try_from(cursor / EROFS_SLOT_SIZE).map_err(InvalidParameter::from)?;
        cursor = align_up(cursor + total_node_bytes, EROFS_SLOT_SIZE);
    }

    let metadata_end = align_up(cursor, EROFS_BLOCK_SIZE);
    let mut current_data_block =
        u32::try_from(metadata_end / EROFS_BLOCK_SIZE).map_err(InvalidParameter::from)?;
    let mut dir_data_payloads: Vec<(usize, Vec<u8>)> = Vec::new();

    // Compute parent NIDs for directories.
    let mut parent_nids = vec![nodes[0].nid; nodes.len()];
    for idx in 0..nodes.len() {
        let self_nid = nodes[idx].nid;
        if let PlannedNodeKind::Directory { children, .. } = &nodes[idx].kind {
            for &(_, child_idx, ft) in children {
                if ft == EROFS_FT_DIR {
                    parent_nids[child_idx] = self_nid;
                }
            }
        }
    }

    for idx in 0..nodes.len() {
        let self_nid = nodes[idx].nid;
        let parent_nid = parent_nids[idx];
        let dir_info = match &nodes[idx].kind {
            PlannedNodeKind::Directory { children, nlink } => {
                let resolved: Vec<(String, u64, u8)> = children
                    .iter()
                    .map(|(name, child_idx, ft)| (name.clone(), nodes[*child_idx].nid, *ft))
                    .collect();
                Some((resolved, *nlink))
            }
            PlannedNodeKind::SymlinkBlock(link) => {
                let mut block = link.target.as_bytes().to_vec();
                block.resize(EROFS_BLOCK_SIZE, 0);
                let start_block = current_data_block;
                current_data_block = current_data_block
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("composefs block count overflow".to_string()))?;
                nodes[idx].data_start_block = start_block;
                nodes[idx].encoded_inode_and_xattrs[24..28]
                    .copy_from_slice(&start_block.to_le_bytes());
                dir_data_payloads.push((
                    usize::try_from(start_block).map_err(InvalidParameter::from)?
                        * EROFS_BLOCK_SIZE,
                    block,
                ));
                None
            }
            PlannedNodeKind::File(_) | PlannedNodeKind::SymlinkInline(_) => None,
        };
        if let Some((resolved_children, nlink)) = dir_info {
            let dir_blocks = encode_directory_blocks(self_nid, parent_nid, &resolved_children)?;
            let block_count = u32::try_from(dir_blocks.len() / EROFS_BLOCK_SIZE)
                .map_err(InvalidParameter::from)?;
            let dir_size = dir_blocks.len() as u64;
            let start_block = current_data_block;
            current_data_block = current_data_block
                .checked_add(block_count)
                .ok_or_else(|| Error::Other("composefs block count overflow".to_string()))?;
            nodes[idx].data_start_block = start_block;
            nodes[idx].data_size = dir_size;
            let mode = S_IFDIR | (options.dir_permissions & 0o7777);
            let inode_bytes = encode_extended_inode(
                EROFS_INODE_LAYOUT_PLAIN,
                0,
                mode,
                nlink,
                dir_size,
                start_block,
                nodes[idx].ino,
                options.mtime_sec,
            );
            nodes[idx].encoded_inode_and_xattrs[..EROFS_INODE_EXTENDED_SIZE]
                .copy_from_slice(&inode_bytes);
            dir_data_payloads.push((
                usize::try_from(start_block).map_err(InvalidParameter::from)? * EROFS_BLOCK_SIZE,
                dir_blocks,
            ));
        }
    }

    let total_blocks = current_data_block;
    let total_bytes =
        usize::try_from(total_blocks).map_err(InvalidParameter::from)? * EROFS_BLOCK_SIZE;
    let mut image = vec![0u8; total_bytes];

    // Write all inodes + inline xattrs + inline tails.
    let mut projections = Vec::new();
    let mut symlinks = Vec::new();
    for node in &nodes {
        let start = node.inode_offset;
        let end_meta = start + node.encoded_inode_and_xattrs.len();
        image[start..end_meta].copy_from_slice(&node.encoded_inode_and_xattrs);
        if !node.inline_tail.is_empty() {
            image[end_meta..end_meta + node.inline_tail.len()].copy_from_slice(&node.inline_tail);
        }
        match &node.kind {
            PlannedNodeKind::File(spec) => projections.push(ComposefsFileProjection {
                relative_path: spec.relative_path.clone(),
                size: spec.size,
                blob_hash: spec.blob_hash,
                redirect_path: spec.redirect_path.clone(),
                inline_data: spec.inline_data.clone(),
                executable: spec.executable,
                nid: node.nid,
            }),
            PlannedNodeKind::SymlinkInline(link) | PlannedNodeKind::SymlinkBlock(link) => {
                symlinks.push(ComposefsSymlinkProjection {
                    relative_path: link.relative_path.clone(),
                    target: link.target.clone(),
                    nid: node.nid,
                });
            }
            PlannedNodeKind::Directory { .. } => {}
        }
    }

    // Write directory data blocks.
    for (offset, dir_bytes) in dir_data_payloads {
        image[offset..offset + dir_bytes.len()].copy_from_slice(&dir_bytes);
    }

    // Write EROFS v1 superblock at offset 1024.
    let root_nid = u16::try_from(nodes[0].nid).map_err(InvalidParameter::from)?;
    let total_inodes = nodes.len() as u64;
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(&item.metadata.root.hash.as_bytes()[..16]);

    let sb = &mut image[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + EROFS_SUPER_SIZE];
    sb[0..4].copy_from_slice(&EROFS_SUPER_MAGIC_V1.to_le_bytes());
    sb[4..8].copy_from_slice(&0u32.to_le_bytes()); // checksum (0 when feature_compat == 0)
    sb[8..12].copy_from_slice(&0u32.to_le_bytes()); // feature_compat
    sb[12] = 12; // blkszbits = 12 (4096)
    sb[13] = 0; // sb_extslots
    sb[14..16].copy_from_slice(&root_nid.to_le_bytes());
    sb[16..24].copy_from_slice(&total_inodes.to_le_bytes());
    sb[24..32].copy_from_slice(&options.mtime_sec.to_le_bytes());
    sb[32..36].copy_from_slice(&0u32.to_le_bytes()); // build_time_nsec
    sb[36..40].copy_from_slice(&total_blocks.to_le_bytes());
    sb[40..44].copy_from_slice(&0u32.to_le_bytes()); // meta_blkaddr = 0
    sb[44..48].copy_from_slice(&0u32.to_le_bytes()); // xattr_blkaddr = 0
    sb[48..64].copy_from_slice(&uuid);
    sb[64..80].copy_from_slice(b"distd-composefs\0");

    projections.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    symlinks.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    let info = ComposefsImageInfo {
        root_nid,
        total_inodes,
        total_blocks,
        uuid,
        files: projections,
        symlinks,
    };
    Ok((image, info))
}

/// Parse and verify a `composefs` EROFS v1 metadata image, returning its superblock and file
/// projections in canonical traversal order.
pub fn inspect_composefs_erofs(image: &[u8]) -> Result<ComposefsImageInfo, Error> {
    if image.len() < EROFS_BLOCK_SIZE || image.len() % EROFS_BLOCK_SIZE != 0 {
        return Err(Error::Other(
            "composefs EROFS image size is not a positive multiple of 4096 bytes".to_string(),
        ));
    }
    let sb = &image[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + EROFS_SUPER_SIZE];
    let magic = u32::from_le_bytes(sb[0..4].try_into().unwrap());
    if magic != EROFS_SUPER_MAGIC_V1 {
        return Err(Error::Other(format!(
            "invalid EROFS superblock magic: 0x{magic:08x}"
        )));
    }
    let blkszbits = sb[12];
    if blkszbits != 12 {
        return Err(Error::Other(format!(
            "unsupported EROFS block size bits: {blkszbits}"
        )));
    }
    let root_nid = u16::from_le_bytes(sb[14..16].try_into().unwrap());
    let total_inodes = u64::from_le_bytes(sb[16..24].try_into().unwrap());
    let total_blocks = u32::from_le_bytes(sb[36..40].try_into().unwrap());
    let meta_blkaddr = u32::from_le_bytes(sb[40..44].try_into().unwrap());
    if usize::try_from(total_blocks).map_err(InvalidParameter::from)? * EROFS_BLOCK_SIZE
        != image.len()
    {
        return Err(Error::Other(
            "EROFS superblock block count does not match image length".to_string(),
        ));
    }
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(&sb[48..64]);

    let mut files = Vec::new();
    let mut symlinks = Vec::new();
    walk_erofs_directory(
        image,
        meta_blkaddr,
        u64::from(root_nid),
        "",
        &mut files,
        &mut symlinks,
        0,
    )?;
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    symlinks.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

    Ok(ComposefsImageInfo {
        root_nid,
        total_inodes,
        total_blocks,
        uuid,
        files,
        symlinks,
    })
}

fn walk_erofs_directory(
    image: &[u8],
    meta_blkaddr: u32,
    dir_nid: u64,
    prefix: &str,
    files: &mut Vec<ComposefsFileProjection>,
    symlinks: &mut Vec<ComposefsSymlinkProjection>,
    depth: usize,
) -> Result<(), Error> {
    if depth > 64 {
        return Err(Error::Other(
            "EROFS directory recursion limit exceeded".to_string(),
        ));
    }
    let inode_offset = usize::try_from(meta_blkaddr).map_err(InvalidParameter::from)?
        * EROFS_BLOCK_SIZE
        + usize::try_from(dir_nid).map_err(InvalidParameter::from)? * EROFS_SLOT_SIZE;
    if inode_offset + EROFS_INODE_EXTENDED_SIZE > image.len() {
        return Err(Error::Other(
            "EROFS directory NID out of bounds".to_string(),
        ));
    }
    let inode = &image[inode_offset..inode_offset + EROFS_INODE_EXTENDED_SIZE];
    let i_format = u16::from_le_bytes(inode[0..2].try_into().unwrap());
    if i_format & 1 == 0 {
        return Err(Error::Other(
            "expected extended 64-byte EROFS inode".to_string(),
        ));
    }
    let i_mode = u16::from_le_bytes(inode[4..6].try_into().unwrap());
    if i_mode & S_IFMT != S_IFDIR {
        return Err(Error::Other("expected EROFS directory inode".to_string()));
    }
    let i_size = u64::from_le_bytes(inode[12..20].try_into().unwrap());
    let raw_blkaddr = u32::from_le_bytes(inode[24..28].try_into().unwrap());
    let data_offset =
        usize::try_from(raw_blkaddr).map_err(InvalidParameter::from)? * EROFS_BLOCK_SIZE;
    let data_len = usize::try_from(i_size).map_err(InvalidParameter::from)?;
    if data_offset + data_len > image.len() || data_len % EROFS_BLOCK_SIZE != 0 {
        return Err(Error::Other(
            "EROFS directory data blocks out of bounds".to_string(),
        ));
    }

    for block in image[data_offset..data_offset + data_len].chunks_exact(EROFS_BLOCK_SIZE) {
        let first_nameoff = u16::from_le_bytes(block[8..10].try_into().unwrap()) as usize;
        if first_nameoff < 12 || first_nameoff % 12 != 0 || first_nameoff > EROFS_BLOCK_SIZE {
            return Err(Error::Other("invalid EROFS dirent nameoff".to_string()));
        }
        let dirent_count = first_nameoff / 12;
        for idx in 0..dirent_count {
            let d_off = idx * 12;
            let child_nid = u64::from_le_bytes(block[d_off..d_off + 8].try_into().unwrap());
            let name_start =
                u16::from_le_bytes(block[d_off + 8..d_off + 10].try_into().unwrap()) as usize;
            let file_type = block[d_off + 10];
            let name_end = if idx + 1 < dirent_count {
                u16::from_le_bytes(
                    block[(idx + 1) * 12 + 8..(idx + 1) * 12 + 10]
                        .try_into()
                        .unwrap(),
                ) as usize
            } else {
                let mut end = name_start;
                while end < EROFS_BLOCK_SIZE && block[end] != 0 {
                    end += 1;
                }
                end
            };
            if name_start > name_end || name_end > EROFS_BLOCK_SIZE {
                return Err(Error::Other(
                    "EROFS dirent name slice out of bounds".to_string(),
                ));
            }
            let name = std::str::from_utf8(&block[name_start..name_end])
                .map_err(|_| Error::Other("EROFS dirent name is not valid UTF-8".to_string()))?;
            if name == "." || name == ".." {
                continue;
            }
            let rel_path = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}/{name}")
            };
            match file_type {
                EROFS_FT_DIR => {
                    walk_erofs_directory(
                        image,
                        meta_blkaddr,
                        child_nid,
                        &rel_path,
                        files,
                        symlinks,
                        depth + 1,
                    )?;
                }
                EROFS_FT_REG_FILE => {
                    let proj = read_erofs_file_inode(image, meta_blkaddr, child_nid, rel_path)?;
                    files.push(proj);
                }
                EROFS_FT_SYMLINK => {
                    symlinks.push(read_erofs_symlink_inode(
                        image,
                        meta_blkaddr,
                        child_nid,
                        rel_path,
                    )?);
                }
                other => {
                    return Err(Error::Other(format!(
                        "unsupported EROFS dirent file type: {other}"
                    )))
                }
            }
        }
    }
    Ok(())
}

fn read_erofs_file_inode(
    image: &[u8],
    meta_blkaddr: u32,
    nid: u64,
    relative_path: String,
) -> Result<ComposefsFileProjection, Error> {
    let inode_offset = usize::try_from(meta_blkaddr).map_err(InvalidParameter::from)?
        * EROFS_BLOCK_SIZE
        + usize::try_from(nid).map_err(InvalidParameter::from)? * EROFS_SLOT_SIZE;
    if inode_offset + EROFS_INODE_EXTENDED_SIZE > image.len() {
        return Err(Error::Other("EROFS file NID out of bounds".to_string()));
    }
    let inode = &image[inode_offset..inode_offset + EROFS_INODE_EXTENDED_SIZE];
    let i_format = u16::from_le_bytes(inode[0..2].try_into().unwrap());
    let layout = (i_format >> 1) & 0x7;
    let xattr_icount = u16::from_le_bytes(inode[2..4].try_into().unwrap());
    let i_mode = u16::from_le_bytes(inode[4..6].try_into().unwrap());
    if i_mode & S_IFMT != S_IFREG {
        return Err(Error::Other(
            "expected EROFS regular file inode".to_string(),
        ));
    }
    let size = u64::from_le_bytes(inode[12..20].try_into().unwrap());
    let xattr_bytes = if xattr_icount == 0 {
        0usize
    } else {
        12 + usize::from(xattr_icount - 1) * 4
    };
    let xattr_start = inode_offset + EROFS_INODE_EXTENDED_SIZE;
    let xattr_end = xattr_start + xattr_bytes;
    if xattr_end > image.len() {
        return Err(Error::Other(
            "EROFS inline xattrs out of bounds".to_string(),
        ));
    }

    let mut redirect_path = None;
    if xattr_bytes >= 12 {
        let mut cursor = xattr_start + 12;
        while cursor + 4 <= xattr_end {
            let name_len = usize::from(image[cursor]);
            let name_index = image[cursor + 1];
            let value_len = usize::from(u16::from_le_bytes(
                image[cursor + 2..cursor + 4].try_into().unwrap(),
            ));
            let entry_end = cursor + 4 + name_len + value_len;
            if entry_end > xattr_end {
                return Err(Error::Other("EROFS xattr entry out of bounds".to_string()));
            }
            let name = &image[cursor + 4..cursor + 4 + name_len];
            let value = &image[cursor + 4 + name_len..entry_end];
            if name_index == EROFS_XATTR_INDEX_TRUSTED && name == XATTR_OVERLAY_REDIRECT_SUFFIX {
                let s = std::str::from_utf8(value).map_err(|_| {
                    Error::Other("EROFS overlay.redirect is not valid UTF-8".to_string())
                })?;
                redirect_path = Some(s.to_string());
            }
            cursor = align_up(entry_end, 4);
        }
    }

    let (blob_hash, inline_data) = if layout == EROFS_INODE_LAYOUT_INLINE {
        let len = usize::try_from(size).map_err(InvalidParameter::from)?;
        if xattr_end + len > image.len() {
            return Err(Error::Other(
                "EROFS inline file data out of bounds".to_string(),
            ));
        }
        let inline_bytes = image[xattr_end..xattr_end + len].to_vec();
        let chunk = ChunkInfo {
            hash: crate::hash::hash(&inline_bytes),
            size,
        };
        let blob_hash = FsStorage::file_blob_hash(&[chunk])?;
        (blob_hash, Some(inline_bytes))
    } else {
        let redirect = redirect_path.as_deref().ok_or_else(|| {
            Error::Other("EROFS plain file inode missing trusted.overlay.redirect".to_string())
        })?;
        let hex_digest = redirect
            .rsplit('/')
            .next()
            .ok_or_else(|| Error::Other("invalid overlay.redirect path".to_string()))?;
        let blob_hash: Hash = hex_digest
            .parse()
            .map_err(|_| Error::Other("invalid blob hash in overlay.redirect".to_string()))?;
        (blob_hash, None)
    };

    Ok(ComposefsFileProjection {
        relative_path,
        size,
        blob_hash,
        redirect_path,
        inline_data,
        executable: i_mode & 0o111 != 0,
        nid,
    })
}

fn read_erofs_symlink_inode(
    image: &[u8],
    meta_blkaddr: u32,
    nid: u64,
    relative_path: String,
) -> Result<ComposefsSymlinkProjection, Error> {
    let inode_offset = usize::try_from(meta_blkaddr).map_err(InvalidParameter::from)?
        * EROFS_BLOCK_SIZE
        + usize::try_from(nid).map_err(InvalidParameter::from)? * EROFS_SLOT_SIZE;
    if inode_offset + EROFS_INODE_EXTENDED_SIZE > image.len() {
        return Err(Error::Other("EROFS symlink NID out of bounds".to_string()));
    }
    let inode = &image[inode_offset..inode_offset + EROFS_INODE_EXTENDED_SIZE];
    let i_format = u16::from_le_bytes(inode[0..2].try_into().unwrap());
    let layout = (i_format >> 1) & 0x7;
    let xattr_icount = u16::from_le_bytes(inode[2..4].try_into().unwrap());
    let i_mode = u16::from_le_bytes(inode[4..6].try_into().unwrap());
    if i_mode & S_IFMT != S_IFLNK || xattr_icount != 0 {
        return Err(Error::Other("expected EROFS symlink inode".to_string()));
    }
    let size = usize::try_from(u64::from_le_bytes(inode[12..20].try_into().unwrap()))
        .map_err(InvalidParameter::from)?;
    let data_offset = match layout {
        EROFS_INODE_LAYOUT_INLINE => inode_offset + EROFS_INODE_EXTENDED_SIZE,
        EROFS_INODE_LAYOUT_PLAIN => {
            let blkaddr = u32::from_le_bytes(inode[24..28].try_into().unwrap());
            usize::try_from(blkaddr).map_err(InvalidParameter::from)? * EROFS_BLOCK_SIZE
        }
        _ => return Err(Error::Other("unsupported EROFS symlink layout".to_string())),
    };
    if size == 0 || size >= EROFS_BLOCK_SIZE || data_offset + size > image.len() {
        return Err(Error::Other(
            "EROFS symlink target out of bounds".to_string(),
        ));
    }
    let target = std::str::from_utf8(&image[data_offset..data_offset + size])
        .map_err(|_| Error::Other("EROFS symlink target is not valid UTF-8".to_string()))?
        .to_string();
    Ok(ComposefsSymlinkProjection {
        relative_path,
        target,
        nid,
    })
}

impl FsStorage {
    /// Generate a deterministic `composefs` EROFS v1 metadata image for `item` and write it to
    /// `destination`.
    ///
    /// Every regular file in the generated image references its content-addressed backing file in
    /// [`FsStorage::cas_blobs_root`] via `trusted.overlay.redirect = "/<prefix>/<blob_hash>"` (or
    /// is inlined directly when configured in `options`).
    pub fn write_composefs_image(
        &self,
        item: &Item,
        destination: &Path,
        options: &ComposefsOptions,
    ) -> Result<ComposefsImageInfo, Error> {
        let (bytes, info) = build_composefs_erofs(item, options)?;
        if let Some(parent) = destination.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(destination, &bytes)?;
        Ok(info)
    }

    /// Return the lowerdir mount specification (`<erofs_mount>:<cas_blobs_root>`) for mounting
    /// a `composefs` OverlayFS view backed by this storage's CAS blob store.
    #[must_use]
    pub fn composefs_overlay_lowerdir(&self, erofs_mount_point: &Path) -> PathBuf {
        let mut combined = erofs_mount_point.as_os_str().to_os_string();
        combined.push(":");
        combined.push(self.cas_blobs_root().as_os_str());
        PathBuf::from(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_storage::hashmap_storage::HashMapStorage;
    use crate::chunk_storage::ChunkStorage;
    use crate::chunks::ChunkAlgorithm;
    use crate::item::{EntryKind, FileEntry};
    use crate::utils::testing::temp_path;
    use std::collections::BTreeMap;

    #[test]
    fn composefs_erofs_projects_directory_manifest_with_cas_blob_redirects_and_inline_files() {
        let source = HashMapStorage::default();
        let pkg_config = b"prefix=/nix/store/demo\nName: demo\nVersion: 1.0\n";
        let lib_binary = vec![0x7fu8; 16384];
        let header_file = b"#pragma once\nint demo(void);\n";

        let mut combined = Vec::new();
        combined.extend_from_slice(header_file);
        combined.extend_from_slice(&lib_binary);
        combined.extend_from_slice(pkg_config);

        let algo = ChunkAlgorithm::fixed_aligned(4096, 1);
        let (hdr_root, hdr_chunks) = source.chunk_tree(header_file, algo).unwrap();
        let (lib_root, lib_chunks) = source.chunk_tree(&lib_binary, algo).unwrap();
        let (pc_root, pc_chunks) = source.chunk_tree(pkg_config, algo).unwrap();
        let _ = (hdr_root, lib_root, pc_root);

        let mut all_chunks = Vec::new();
        let hdr_start = 0u32;
        all_chunks.extend_from_slice(&hdr_chunks);
        let hdr_end = u32::try_from(all_chunks.len()).unwrap();

        let lib_start = hdr_end;
        all_chunks.extend_from_slice(&lib_chunks);
        let lib_end = u32::try_from(all_chunks.len()).unwrap();

        let pc_start = lib_end;
        all_chunks.extend_from_slice(&pc_chunks);
        let pc_end = u32::try_from(all_chunks.len()).unwrap();

        let entries = vec![
            FileEntry {
                relative_path: "include/demo.h".to_string(),
                size: header_file.len() as u64,
                chunk_range: (hdr_start, hdr_end),
                kind: EntryKind::Regular,
            },
            FileEntry {
                relative_path: "lib/libdemo.so".to_string(),
                size: lib_binary.len() as u64,
                chunk_range: (lib_start, lib_end),
                kind: EntryKind::Regular,
            },
            FileEntry {
                relative_path: "lib/pkgconfig/demo.pc".to_string(),
                size: pkg_config.len() as u64,
                chunk_range: (pc_start, pc_end),
                kind: EntryKind::Regular,
            },
        ];

        // Compute tree root over all_chunks using HashMapStorage
        let mut partials: Vec<std::sync::Arc<crate::chunk_storage::Node>> = all_chunks
            .iter()
            .map(|c| {
                std::sync::Arc::new(crate::chunk_storage::Node::Skipped {
                    hash: c.hash,
                    size: c.size,
                })
            })
            .collect();
        while partials.len() > 1 {
            let count = partials.len();
            let mut next = Vec::with_capacity((count + 1) / 2);
            let mut i = 0;
            while i + 1 < count {
                next.push(
                    source
                        .link(partials[i].clone(), partials[i + 1].clone())
                        .unwrap(),
                );
                i += 2;
            }
            if i < count {
                next.push(partials[i].clone());
            }
            partials = next;
        }
        let root_info = partials[0].chunk_info();

        let item = Item::make_with_entries(
            "demo-pkg".to_string(),
            PathBuf::from("demo-pkg"),
            1,
            Some("composefs test package".to_string()),
            root_info,
            all_chunks.clone(),
            entries,
            algo,
        )
        .unwrap();

        let mut options = ComposefsOptions::default();
        options
            .inline_files
            .insert("lib/pkgconfig/demo.pc".to_string(), pkg_config.to_vec());

        let (image1, info1) = build_composefs_erofs(&item, &options).unwrap();
        let (image2, info2) = build_composefs_erofs(&item, &options).unwrap();
        // Image generation must be bit-for-bit deterministic
        assert_eq!(image1, image2);
        assert_eq!(info1, info2);

        let inspected = inspect_composefs_erofs(&image1).unwrap();
        assert_eq!(inspected, info1);
        assert_eq!(inspected.files.len(), 3);

        // 1. include/demo.h -> redirected to CAS blob
        let expected_hdr_hash = FsStorage::file_blob_hash(&hdr_chunks).unwrap();
        let expected_hdr_redirect = format!(
            "/{}",
            FsStorage::cas_blob_relative_path(&expected_hdr_hash)
                .to_string_lossy()
                .replace('\\', "/")
        );
        assert_eq!(inspected.files[0].relative_path, "include/demo.h");
        assert_eq!(inspected.files[0].size, header_file.len() as u64);
        assert_eq!(inspected.files[0].blob_hash, expected_hdr_hash);
        assert_eq!(
            inspected.files[0].redirect_path.as_deref(),
            Some(expected_hdr_redirect.as_str())
        );
        assert!(inspected.files[0].inline_data.is_none());

        // 2. lib/libdemo.so -> redirected to CAS blob
        let expected_lib_hash = FsStorage::file_blob_hash(&lib_chunks).unwrap();
        let expected_lib_redirect = format!(
            "/{}",
            FsStorage::cas_blob_relative_path(&expected_lib_hash)
                .to_string_lossy()
                .replace('\\', "/")
        );
        assert_eq!(inspected.files[1].relative_path, "lib/libdemo.so");
        assert_eq!(inspected.files[1].size, lib_binary.len() as u64);
        assert_eq!(inspected.files[1].blob_hash, expected_lib_hash);
        assert_eq!(
            inspected.files[1].redirect_path.as_deref(),
            Some(expected_lib_redirect.as_str())
        );

        // 3. lib/pkgconfig/demo.pc -> inlined directly into EROFS image
        let expected_pc_hash = FsStorage::file_blob_hash(&pc_chunks).unwrap();
        assert_eq!(inspected.files[2].relative_path, "lib/pkgconfig/demo.pc");
        assert_eq!(inspected.files[2].size, pkg_config.len() as u64);
        assert_eq!(inspected.files[2].blob_hash, expected_pc_hash);
        assert!(inspected.files[2].redirect_path.is_none());
        assert_eq!(
            inspected.files[2].inline_data.as_deref(),
            Some(pkg_config.as_slice())
        );

        // Verify FsStorage::write_composefs_image writes a valid inspectable image on disk
        let root = temp_path();
        let storage = FsStorage::new(root.clone()).unwrap();
        let erofs_path = root.join("demo-pkg.erofs");
        let written_info = storage
            .write_composefs_image(&item, &erofs_path, &options)
            .unwrap();
        assert_eq!(written_info, info1);
        let disk_bytes = std::fs::read(&erofs_path).unwrap();
        assert_eq!(inspect_composefs_erofs(&disk_bytes).unwrap(), info1);
    }

    #[test]
    fn composefs_erofs_projects_executable_files_and_symlinks() {
        let storage = HashMapStorage::default();
        let long_target = format!("/nix/store/{}", "x".repeat(4060));
        let item = storage
            .prepare_revision_from_entries(
                "nix-pkg".to_string(),
                "nix-pkg".to_string(),
                PathBuf::from("nix-pkg"),
                1,
                None,
                vec![
                    (
                        PathBuf::from("bin/hello"),
                        bytes::Bytes::from(vec![0x7f; 5000]),
                    ),
                    (PathBuf::from("bin/hi"), bytes::Bytes::new()),
                    (PathBuf::from("lib/long"), bytes::Bytes::new()),
                    (
                        PathBuf::from("share/doc"),
                        bytes::Bytes::from_static(b"plain\n"),
                    ),
                ],
                BTreeMap::from([
                    (PathBuf::from("bin/hello"), EntryKind::Executable),
                    (
                        PathBuf::from("bin/hi"),
                        EntryKind::Symlink {
                            target: "hello".to_string(),
                        },
                    ),
                    (
                        PathBuf::from("lib/long"),
                        EntryKind::Symlink {
                            target: long_target.clone(),
                        },
                    ),
                ]),
                ChunkAlgorithm::fixed_aligned(4096, 1),
            )
            .unwrap();

        let (image, info) = build_composefs_erofs(&item, &ComposefsOptions::default()).unwrap();
        assert_eq!(
            build_composefs_erofs(&item, &ComposefsOptions::default()).unwrap(),
            (image.clone(), info.clone())
        );
        assert_eq!(inspect_composefs_erofs(&image).unwrap(), info);

        let files: Vec<_> = info
            .files
            .iter()
            .map(|file| (file.relative_path.as_str(), file.executable))
            .collect();
        assert_eq!(files, [("bin/hello", true), ("share/doc", false)]);
        assert!(info.files.iter().all(|file| file.redirect_path.is_some()));
        let links: Vec<_> = info
            .symlinks
            .iter()
            .map(|link| (link.relative_path.as_str(), link.target.as_str()))
            .collect();
        assert_eq!(
            links,
            [("bin/hi", "hello"), ("lib/long", long_target.as_str())]
        );
    }
}
