//! common chunks and hash-tree data structs

use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::InvalidParameter;
use crate::hash::Hash;
use crate::proto;

/// Chunk size in bytes
/// It may useful to increase this in order to print hash tree when debugging
//pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;
pub const CHUNK_SIZE: usize = 256 * 1024;
pub const CHUNK_SIZE_U64: u64 = CHUNK_SIZE as u64;
pub const FASTCDC_SEED: u64 = 0x1234_5678_9ABC_DEF0;
pub const FASTCDC_NORMALIZATION_LEVEL: u32 = 2;

/// Owned chunk
pub type OwnedChunk = Vec<u8>;

//pub type Size = u64;

/// Node in an hash-tree
/// It may be a parent node with two children or a leaf node with owned data
pub trait HashTreeNode {
    /// Get hash of node
    fn hash(&self) -> &Hash;

    /// Compute sum size in bytes of all descending chunks
    fn size(&self) -> u64;

    /// Get contained data, returns None if is not Stored
    fn stored_data(&self) -> Option<&OwnedChunk>;

    /// Get mutable contained data, returns None if is not Stored
    fn stored_data_mut(&mut self) -> Option<&mut OwnedChunk>;

    /// Get owned contained data, returns None if is not `Stored`
    fn stored_data_owned(self) -> Option<OwnedChunk>;

    /// Get contained data, returns None if is not Parent
    fn children(&self) -> Option<(&Self, &Self)>;

    /// Get diff sub-tree: required tree to reconstruct current node if one has the `hashes`
    #[must_use]
    fn find_diff(&self, hashes: &[Hash]) -> Self;

    /// Flatten the tree into an iterator on chunks
    fn flatten_iter(self) -> Box<dyn Iterator<Item = Vec<u8>>>;

    /// Wheter the subtree has any missing node
    ///
    /// A default implementation si provided to skip this if the type cannot have missing nodes
    fn is_complete(&self) -> bool {
        true
    }
}

/// Seralizable view of an hash tree node, only contains size and hash
#[derive(Debug, Clone, Copy, Serialize, Deserialize, std::hash::Hash, PartialEq, Eq)]
pub struct ChunkInfo {
    // progressive unique id provided by the storage ??
    //pub id: u64,
    // Chunk size
    pub size: u64,
    // Chunk hash
    pub hash: Hash,
}

impl ChunkInfo {
    #[allow(dead_code)]
    fn is_leaf(&self) -> bool {
        self.size == CHUNK_SIZE as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkingPolicy {
    Auto,
    Fixed256K,
    Fixed1M,
    FastCdc,
    Image,
    Archive,
}

impl FromStr for ChunkingPolicy {
    type Err = InvalidParameter;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "fixed" | "fixed-256k" | "fixed_256k" => Ok(Self::Fixed256K),
            "fixed-1m" | "fixed_1m" | "fixed1m" => Ok(Self::Fixed1M),
            "fastcdc" | "cdc" | "generic" => Ok(Self::FastCdc),
            "image" | "images" | "os-image" | "os_image" => Ok(Self::Image),
            "archive" | "archives" => Ok(Self::Archive),
            got => Err(InvalidParameter::Generic {
                expected: "one of auto, fixed-256k, fixed-1m, fastcdc, image, archive".to_string(),
                got: got.to_string(),
            }),
        }
    }
}

/// Chunking algorithm identifier, stored in manifests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, std::hash::Hash)]
pub enum ChunkAlgorithm {
    /// Legacy fixed-size 256 KiB chunks.
    Fixed256K,
    /// Fixed-size aligned chunks.
    FixedAligned {
        chunk_size: u32,
        alignment: u32,
        header_size: u32,
        profile_version: u32,
    },
    /// Generic FastCDC-style content-defined chunking.
    FastCdc {
        min_size: u32,
        avg_size: u32,
        max_size: u32,
        normalization_level: u32,
        seed: u64,
        profile_version: u32,
    },
    /// Image-oriented FastCDC profile with stable alignment.
    BlockAlignedFastCdc {
        min_size: u32,
        avg_size: u32,
        max_size: u32,
        alignment: u32,
        zero_run_cutoff: u32,
        normalization_level: u32,
        seed: u64,
        profile_version: u32,
    },
    /// Archive-oriented FastCDC profile that prefers record boundaries.
    ArchiveAwareFastCdc {
        min_size: u32,
        avg_size: u32,
        max_size: u32,
        record_size: u32,
        alignment: u32,
        normalization_level: u32,
        seed: u64,
        profile_version: u32,
    },
}

impl Default for ChunkAlgorithm {
    fn default() -> Self {
        Self::Fixed256K
    }
}

impl ChunkAlgorithm {
    #[must_use]
    pub fn fixed_1m() -> Self {
        Self::FixedAligned {
            chunk_size: 1024 * 1024,
            alignment: 4096,
            header_size: 0,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn fastcdc_default() -> Self {
        Self::FastCdc {
            min_size: 64 * 1024,
            avg_size: 256 * 1024,
            max_size: 1024 * 1024,
            normalization_level: FASTCDC_NORMALIZATION_LEVEL,
            seed: FASTCDC_SEED,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn image_default() -> Self {
        Self::BlockAlignedFastCdc {
            min_size: 256 * 1024,
            avg_size: 1024 * 1024,
            max_size: 4 * 1024 * 1024,
            alignment: 4096,
            zero_run_cutoff: 64 * 1024,
            normalization_level: FASTCDC_NORMALIZATION_LEVEL,
            seed: FASTCDC_SEED,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn archive_default() -> Self {
        Self::ArchiveAwareFastCdc {
            min_size: 64 * 1024,
            avg_size: 256 * 1024,
            max_size: 1024 * 1024,
            record_size: 512,
            alignment: 512,
            normalization_level: FASTCDC_NORMALIZATION_LEVEL,
            seed: FASTCDC_SEED,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn nominal_chunk_size(self) -> u32 {
        match self {
            Self::Fixed256K => CHUNK_SIZE as u32,
            Self::FixedAligned { chunk_size, .. } => chunk_size,
            Self::FastCdc { avg_size, .. }
            | Self::BlockAlignedFastCdc { avg_size, .. }
            | Self::ArchiveAwareFastCdc { avg_size, .. } => avg_size,
        }
    }

    #[must_use]
    pub fn from_policy(policy: ChunkingPolicy, path: &Path, sample: &[u8], multi_file: bool) -> Self {
        match policy {
            ChunkingPolicy::Auto => detect_chunking_algorithm(path, sample, multi_file),
            ChunkingPolicy::Fixed256K => Self::Fixed256K,
            ChunkingPolicy::Fixed1M => Self::fixed_1m(),
            ChunkingPolicy::FastCdc => Self::fastcdc_default(),
            ChunkingPolicy::Image => Self::image_default(),
            ChunkingPolicy::Archive => Self::archive_default(),
        }
    }

    #[must_use]
    pub fn to_proto(self) -> proto::ChunkAlgorithmProfile {
        use proto::chunk_algorithm_profile::Kind;

        match self {
            Self::Fixed256K => proto::ChunkAlgorithmProfile {
                kind: Kind::FixedAligned as i32,
                profile_version: 0,
                nominal_chunk_size: CHUNK_SIZE as u32,
                min_size: 0,
                avg_size: 0,
                max_size: 0,
                alignment: 1,
                header_size: 0,
                normalization_level: 0,
                zero_run_cutoff: 0,
                record_size: 0,
                seed: 0,
            },
            Self::FixedAligned {
                chunk_size,
                alignment,
                header_size,
                profile_version,
            } => proto::ChunkAlgorithmProfile {
                kind: Kind::FixedAligned as i32,
                profile_version,
                nominal_chunk_size: chunk_size,
                min_size: 0,
                avg_size: 0,
                max_size: 0,
                alignment,
                header_size,
                normalization_level: 0,
                zero_run_cutoff: 0,
                record_size: 0,
                seed: 0,
            },
            Self::FastCdc {
                min_size,
                avg_size,
                max_size,
                normalization_level,
                seed,
                profile_version,
            } => proto::ChunkAlgorithmProfile {
                kind: Kind::FastCdc as i32,
                profile_version,
                nominal_chunk_size: avg_size,
                min_size,
                avg_size,
                max_size,
                alignment: 1,
                header_size: 0,
                normalization_level,
                zero_run_cutoff: 0,
                record_size: 0,
                seed,
            },
            Self::BlockAlignedFastCdc {
                min_size,
                avg_size,
                max_size,
                alignment,
                zero_run_cutoff,
                normalization_level,
                seed,
                profile_version,
            } => proto::ChunkAlgorithmProfile {
                kind: Kind::BlockAlignedFastCdc as i32,
                profile_version,
                nominal_chunk_size: avg_size,
                min_size,
                avg_size,
                max_size,
                alignment,
                header_size: 0,
                normalization_level,
                zero_run_cutoff,
                record_size: 0,
                seed,
            },
            Self::ArchiveAwareFastCdc {
                min_size,
                avg_size,
                max_size,
                record_size,
                alignment,
                normalization_level,
                seed,
                profile_version,
            } => proto::ChunkAlgorithmProfile {
                kind: Kind::ArchiveAwareFastCdc as i32,
                profile_version,
                nominal_chunk_size: avg_size,
                min_size,
                avg_size,
                max_size,
                alignment,
                header_size: 0,
                normalization_level,
                zero_run_cutoff: 0,
                record_size,
                seed,
            },
        }
    }

    #[must_use]
    pub fn from_proto(profile: Option<&proto::ChunkAlgorithmProfile>) -> Self {
        use proto::chunk_algorithm_profile::Kind;

        let Some(profile) = profile else {
            return Self::default();
        };

        match Kind::try_from(profile.kind).unwrap_or(Kind::Unspecified) {
            Kind::FixedAligned => {
                if profile.profile_version == 0 && profile.nominal_chunk_size == CHUNK_SIZE as u32 {
                    Self::Fixed256K
                } else {
                    Self::FixedAligned {
                        chunk_size: profile.nominal_chunk_size.max(1),
                        alignment: profile.alignment.max(1),
                        header_size: profile.header_size,
                        profile_version: profile.profile_version.max(1),
                    }
                }
            }
            Kind::FastCdc => Self::FastCdc {
                min_size: profile.min_size.max(1),
                avg_size: profile.avg_size.max(1),
                max_size: profile.max_size.max(profile.avg_size.max(1)),
                normalization_level: profile.normalization_level,
                seed: profile.seed,
                profile_version: profile.profile_version.max(1),
            },
            Kind::BlockAlignedFastCdc => Self::BlockAlignedFastCdc {
                min_size: profile.min_size.max(1),
                avg_size: profile.avg_size.max(1),
                max_size: profile.max_size.max(profile.avg_size.max(1)),
                alignment: profile.alignment.max(1),
                zero_run_cutoff: profile.zero_run_cutoff,
                normalization_level: profile.normalization_level,
                seed: profile.seed,
                profile_version: profile.profile_version.max(1),
            },
            Kind::ArchiveAwareFastCdc => Self::ArchiveAwareFastCdc {
                min_size: profile.min_size.max(1),
                avg_size: profile.avg_size.max(1),
                max_size: profile.max_size.max(profile.avg_size.max(1)),
                record_size: profile.record_size.max(1),
                alignment: profile.alignment.max(1),
                normalization_level: profile.normalization_level,
                seed: profile.seed,
                profile_version: profile.profile_version.max(1),
            },
            Kind::Unspecified => Self::default(),
        }
    }

    #[must_use]
    pub fn chunk_boundaries(self, data: &[u8]) -> Vec<(usize, usize)> {
        match self {
            Self::Fixed256K => FixedChunker::default().chunk_boundaries(data),
            Self::FixedAligned {
                chunk_size,
                alignment,
                header_size,
                ..
            } => FixedChunker {
                chunk_size: chunk_size as usize,
                alignment: alignment as usize,
                header_size: header_size as usize,
            }
            .chunk_boundaries(data),
            Self::FastCdc {
                min_size,
                avg_size,
                max_size,
                normalization_level,
                seed,
                ..
            } => ContentDefinedChunker {
                min_size: min_size as usize,
                avg_size: avg_size as usize,
                max_size: max_size as usize,
                alignment: 1,
                record_size: None,
                zero_run_cutoff: None,
                normalization_level,
                seed,
            }
            .chunk_boundaries(data),
            Self::BlockAlignedFastCdc {
                min_size,
                avg_size,
                max_size,
                alignment,
                zero_run_cutoff,
                normalization_level,
                seed,
                ..
            } => ContentDefinedChunker {
                min_size: min_size as usize,
                avg_size: avg_size as usize,
                max_size: max_size as usize,
                alignment: alignment as usize,
                record_size: None,
                zero_run_cutoff: Some(zero_run_cutoff as usize),
                normalization_level,
                seed,
            }
            .chunk_boundaries(data),
            Self::ArchiveAwareFastCdc {
                min_size,
                avg_size,
                max_size,
                record_size,
                alignment,
                normalization_level,
                seed,
                ..
            } => ContentDefinedChunker {
                min_size: min_size as usize,
                avg_size: avg_size as usize,
                max_size: max_size as usize,
                alignment: alignment as usize,
                record_size: Some(record_size as usize),
                zero_run_cutoff: None,
                normalization_level,
                seed,
            }
            .chunk_boundaries(data),
        }
    }
}

#[must_use]
pub fn detect_chunking_algorithm(path: &Path, sample: &[u8], multi_file: bool) -> ChunkAlgorithm {
    if multi_file {
        return ChunkAlgorithm::fastcdc_default();
    }
    if looks_like_image(path, sample) {
        return ChunkAlgorithm::image_default();
    }
    if looks_like_archive(path, sample) {
        return ChunkAlgorithm::archive_default();
    }
    if looks_like_precompressed(path, sample) {
        return ChunkAlgorithm::fixed_1m();
    }
    ChunkAlgorithm::fastcdc_default()
}

fn looks_like_image(path: &Path, sample: &[u8]) -> bool {
    const IMAGE_EXTENSIONS: &[&str] = &[
        "img", "raw", "qcow", "qcow2", "vmdk", "vhd", "vhdx", "iso", "erofs",
        "squashfs",
    ];
    matches_extension(path, IMAGE_EXTENSIONS)
        || sample.starts_with(b"QFI\xfb")
        || sample.starts_with(b"hsqs")
        || zero_density(sample) >= 0.60
}

fn looks_like_archive(path: &Path, sample: &[u8]) -> bool {
    const ARCHIVE_EXTENSIONS: &[&str] = &["tar", "cpio", "catar", "caidx", "caibx"];
    matches_extension(path, ARCHIVE_EXTENSIONS) || sample.get(257..262) == Some(&b"ustar"[..])
}

fn looks_like_precompressed(path: &Path, sample: &[u8]) -> bool {
    const COMPRESSED_EXTENSIONS: &[&str] = &[
        "gz", "xz", "zst", "zip", "7z", "rar", "bz2", "jpg", "jpeg", "png", "gif",
        "webp", "mp4", "mkv", "webm", "mp3", "flac", "pdf",
    ];
    matches_extension(path, COMPRESSED_EXTENSIONS)
        || sample.starts_with(&[0x1f, 0x8b])
        || sample.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00])
        || sample.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
        || sample.starts_with(b"PK\x03\x04")
}

fn matches_extension(path: &Path, known: &[&str]) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            known.iter().any(|candidate| ext == *candidate)
        })
        .unwrap_or(false)
}

fn zero_density(sample: &[u8]) -> f32 {
    if sample.is_empty() {
        return 0.0;
    }
    let zeros = sample.iter().filter(|byte| **byte == 0).count();
    zeros as f32 / sample.len() as f32
}

/// Trait for splitting data into chunks.
pub trait Chunker {
    /// Split `data` into a vector of byte slices (boundaries).
    /// Returns a vector of (offset, length) pairs.
    fn chunk_boundaries(&self, data: &[u8]) -> Vec<(usize, usize)>;
}

/// Fixed-size chunker.
pub struct FixedChunker {
    pub chunk_size: usize,
    pub alignment: usize,
    pub header_size: usize,
}

impl Default for FixedChunker {
    fn default() -> Self {
        Self {
            chunk_size: CHUNK_SIZE,
            alignment: 1,
            header_size: 0,
        }
    }
}

impl Chunker for FixedChunker {
    fn chunk_boundaries(&self, data: &[u8]) -> Vec<(usize, usize)> {
        let chunk_size = self.chunk_size.max(1);
        let mut boundaries = Vec::with_capacity(data.len() / chunk_size + 1);
        let mut offset = 0;

        if self.header_size > 0 && !data.is_empty() {
            let header_end = self.header_size.min(data.len());
            boundaries.push((0, header_end));
            offset = header_end;
        }

        while offset < data.len() {
            let desired_end = (offset + chunk_size).min(data.len());
            let end = snap_boundary(desired_end, offset, data.len(), self.alignment.max(1));
            boundaries.push((offset, end - offset));
            offset = end;
        }

        if boundaries.is_empty() {
            boundaries.push((0, 0));
        }
        boundaries
    }
}

/// Content-defined chunker using a simple Gear-hash rolling window.
/// This provides better dedup on shifted/edited binaries.
pub struct ContentDefinedChunker {
    pub min_size: usize,
    pub avg_size: usize,
    pub max_size: usize,
    pub alignment: usize,
    pub record_size: Option<usize>,
    pub zero_run_cutoff: Option<usize>,
    pub normalization_level: u32,
    pub seed: u64,
}

impl Default for ContentDefinedChunker {
    fn default() -> Self {
        Self {
            min_size: 64 * 1024,
            avg_size: 256 * 1024,
            max_size: 1024 * 1024,
            alignment: 1,
            record_size: None,
            zero_run_cutoff: None,
            normalization_level: FASTCDC_NORMALIZATION_LEVEL,
            seed: FASTCDC_SEED,
        }
    }
}

impl Chunker for ContentDefinedChunker {
    fn chunk_boundaries(&self, data: &[u8]) -> Vec<(usize, usize)> {
        if data.is_empty() {
            return vec![(0, 0)];
        }

        let mut boundaries = Vec::new();
        let mut offset = 0;
        let gear = gear_table(self.seed);
        let mask = self.avg_size.next_power_of_two() as u64 - 1;
        let center = self.avg_size.min(self.max_size).max(self.min_size);
        let normalization_window = (center / 4).max(self.min_size / 2);

        while offset < data.len() {
            let chunk_start = offset;
            let min_end = (offset + self.min_size).min(data.len());
            let max_end = (offset + self.max_size).min(data.len());
            offset = min_end;

            let mut hash = 0u64;
            let mut selected_end = max_end;

            while offset < max_end {
                if let Some(cutoff) = self.zero_run_cutoff {
                    if is_long_zero_run(data, offset, cutoff) {
                        selected_end = offset;
                        break;
                    }
                }

                hash = (hash << 1).wrapping_add(gear[data[offset] as usize]);
                offset += 1;
                if hash & mask == 0 {
                    selected_end = offset;
                    if self.normalization_level > 0
                        && offset < chunk_start + center.saturating_sub(normalization_window)
                    {
                        continue;
                    }
                    break;
                }
            }

            if offset >= max_end {
                selected_end = max_end;
            }

            if let Some(record_size) = self.record_size {
                selected_end = snap_to_record_boundary(
                    selected_end,
                    chunk_start,
                    max_end,
                    self.min_size,
                    record_size,
                );
            }

            selected_end = snap_boundary(selected_end, chunk_start, max_end, self.alignment.max(1));
            offset = selected_end.max(chunk_start + 1).min(max_end);
            boundaries.push((chunk_start, offset - chunk_start));
        }

        boundaries
    }
}

/// Deterministic gear hash table (seeded from a fixed value).
fn gear_table(seed: u64) -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut state = seed;
    for entry in table.iter_mut() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *entry = state;
    }
    table
}

fn snap_boundary(end: usize, start: usize, max_end: usize, alignment: usize) -> usize {
    if alignment <= 1 || end >= max_end {
        return end.max(start + 1).min(max_end);
    }
    let snapped_down = end - (end % alignment);
    if snapped_down > start {
        snapped_down.min(max_end)
    } else {
        end.max(start + 1).min(max_end)
    }
}

fn snap_to_record_boundary(
    end: usize,
    start: usize,
    max_end: usize,
    min_size: usize,
    record_size: usize,
) -> usize {
    if record_size <= 1 || end >= max_end {
        return end;
    }
    let min_end = start.saturating_add(min_size);
    let snapped_down = end - (end % record_size);
    if snapped_down >= min_end {
        snapped_down.min(max_end)
    } else {
        end
    }
}

fn is_long_zero_run(data: &[u8], offset: usize, cutoff: usize) -> bool {
    cutoff > 0
        && offset + cutoff <= data.len()
        && data[offset..offset + cutoff].iter().all(|byte| *byte == 0)
}
