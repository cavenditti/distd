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
    Iso,
    Oci,
    Package,
    Tar,
    TarGzip,
    TarZstd,
    Gzip,
    Zstd,
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
            "iso" | "iso9660" => Ok(Self::Iso),
            "oci" | "oci-image" | "oci_image" | "container" => Ok(Self::Oci),
            "package" | "pkg" | "packages" => Ok(Self::Package),
            "tar" => Ok(Self::Tar),
            "tar-gz" | "tar.gz" | "tgz" | "tar-gzip" => Ok(Self::TarGzip),
            "tar-zst" | "tar.zst" | "tar-zstd" | "tar.zstd" | "tzst" => Ok(Self::TarZstd),
            "gzip" | "gz" => Ok(Self::Gzip),
            "zstd" | "zst" => Ok(Self::Zstd),
            got => Err(InvalidParameter::Generic {
                expected: "one of auto, fixed-256k, fixed-1m, fastcdc, image, archive, iso, oci, package, tar, tar.gz, tar.zst, gzip, zstd".to_string(),
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
    pub fn fixed_aligned(chunk_size: u32, alignment: u32) -> Self {
        Self::FixedAligned {
            chunk_size,
            alignment,
            header_size: 0,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn fixed_aligned_with_header(chunk_size: u32, alignment: u32, header_size: u32) -> Self {
        Self::FixedAligned {
            chunk_size,
            alignment,
            header_size,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn fixed_1m() -> Self {
        Self::fixed_aligned(1024 * 1024, 4096)
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
    pub fn iso_default() -> Self {
        Self::BlockAlignedFastCdc {
            min_size: 128 * 1024,
            avg_size: 1024 * 1024,
            max_size: 4 * 1024 * 1024,
            alignment: 2048,
            zero_run_cutoff: 128 * 1024,
            normalization_level: FASTCDC_NORMALIZATION_LEVEL,
            seed: FASTCDC_SEED,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn oci_layer_default() -> Self {
        Self::ArchiveAwareFastCdc {
            min_size: 128 * 1024,
            avg_size: 512 * 1024,
            max_size: 2 * 1024 * 1024,
            record_size: 512,
            alignment: 4096,
            normalization_level: FASTCDC_NORMALIZATION_LEVEL,
            seed: FASTCDC_SEED,
            profile_version: 1,
        }
    }

    #[must_use]
    pub fn gzip_default() -> Self {
        Self::fixed_aligned(1024 * 1024, 32 * 1024)
    }

    #[must_use]
    pub fn zstd_default() -> Self {
        Self::fixed_aligned(1024 * 1024, 128 * 1024)
    }

    #[must_use]
    pub fn xz_default() -> Self {
        Self::fixed_aligned(2 * 1024 * 1024, 64 * 1024)
    }

    #[must_use]
    pub fn tar_gzip_default() -> Self {
        Self::fixed_aligned(1024 * 1024, 32 * 1024)
    }

    #[must_use]
    pub fn tar_zstd_default() -> Self {
        Self::fixed_aligned(1024 * 1024, 128 * 1024)
    }

    #[must_use]
    pub fn package_default() -> Self {
        Self::fixed_aligned_with_header(1024 * 1024, 4096, 4096)
    }

    #[must_use]
    pub fn compressed_image_default() -> Self {
        Self::fixed_aligned(1024 * 1024, 128 * 1024)
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
    pub fn from_policy(
        policy: ChunkingPolicy,
        path: &Path,
        sample: &[u8],
        multi_file: bool,
    ) -> Self {
        match policy {
            ChunkingPolicy::Auto => detect_chunking_algorithm(path, sample, multi_file),
            ChunkingPolicy::Fixed256K => Self::Fixed256K,
            ChunkingPolicy::Fixed1M => Self::fixed_1m(),
            ChunkingPolicy::FastCdc => Self::fastcdc_default(),
            ChunkingPolicy::Image => Self::image_default(),
            ChunkingPolicy::Archive => Self::archive_default(),
            ChunkingPolicy::Iso => Self::iso_default(),
            ChunkingPolicy::Oci => Self::oci_layer_default(),
            ChunkingPolicy::Package => Self::package_default(),
            ChunkingPolicy::Tar => Self::archive_default(),
            ChunkingPolicy::TarGzip => Self::tar_gzip_default(),
            ChunkingPolicy::TarZstd => Self::tar_zstd_default(),
            ChunkingPolicy::Gzip => Self::gzip_default(),
            ChunkingPolicy::Zstd => Self::zstd_default(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedFormat {
    RawDiskImage,
    Iso9660,
    Qcow2,
    Squashfs,
    Erofs,
    Tar,
    Cpio,
    TarGzip,
    TarZstd,
    TarXz,
    TarBzip2,
    Gzip,
    Zstd,
    Xz,
    Zip,
    OciLayerTar,
    OciLayerTarGzip,
    OciLayerTarZstd,
    Deb,
    Rpm,
    Apk,
    ArchPkgTarZstd,
    ArchPkgTarXz,
    Unknown,
}

#[must_use]
pub fn detect_chunking_algorithm(path: &Path, sample: &[u8], multi_file: bool) -> ChunkAlgorithm {
    if multi_file {
        return ChunkAlgorithm::fastcdc_default();
    }
    chunk_algorithm_for_format(detect_format(path, sample), sample)
}

#[must_use]
pub fn path_likely_precompressed(path: &Path) -> bool {
    matches!(
        detect_format(path, &[]),
        DetectedFormat::Squashfs
            | DetectedFormat::Erofs
            | DetectedFormat::TarGzip
            | DetectedFormat::OciLayerTarGzip
            | DetectedFormat::Apk
            | DetectedFormat::Gzip
            | DetectedFormat::TarZstd
            | DetectedFormat::OciLayerTarZstd
            | DetectedFormat::ArchPkgTarZstd
            | DetectedFormat::Zstd
            | DetectedFormat::TarXz
            | DetectedFormat::ArchPkgTarXz
            | DetectedFormat::Xz
            | DetectedFormat::TarBzip2
            | DetectedFormat::Zip
            | DetectedFormat::Deb
            | DetectedFormat::Rpm
    )
}

#[must_use]
pub fn chunk_algorithm_likely_precompressed(algorithm: ChunkAlgorithm) -> bool {
    algorithm == ChunkAlgorithm::gzip_default()
        || algorithm == ChunkAlgorithm::zstd_default()
        || algorithm == ChunkAlgorithm::xz_default()
        || algorithm == ChunkAlgorithm::tar_gzip_default()
        || algorithm == ChunkAlgorithm::tar_zstd_default()
        || algorithm == ChunkAlgorithm::compressed_image_default()
        || algorithm == ChunkAlgorithm::package_default()
        || algorithm == ChunkAlgorithm::fixed_aligned(1024 * 1024, 100 * 1024)
        || algorithm == ChunkAlgorithm::fixed_aligned(1024 * 1024, 4096)
}

fn chunk_algorithm_for_format(format: DetectedFormat, sample: &[u8]) -> ChunkAlgorithm {
    match format {
        DetectedFormat::RawDiskImage | DetectedFormat::Qcow2 => ChunkAlgorithm::image_default(),
        DetectedFormat::Iso9660 => ChunkAlgorithm::iso_default(),
        DetectedFormat::Squashfs | DetectedFormat::Erofs => {
            ChunkAlgorithm::compressed_image_default()
        }
        DetectedFormat::Tar | DetectedFormat::Cpio => ChunkAlgorithm::archive_default(),
        DetectedFormat::OciLayerTar => ChunkAlgorithm::oci_layer_default(),
        DetectedFormat::TarGzip
        | DetectedFormat::OciLayerTarGzip
        | DetectedFormat::Apk
        | DetectedFormat::Gzip => ChunkAlgorithm::tar_gzip_default(),
        DetectedFormat::TarZstd
        | DetectedFormat::OciLayerTarZstd
        | DetectedFormat::ArchPkgTarZstd
        | DetectedFormat::Zstd => ChunkAlgorithm::tar_zstd_default(),
        DetectedFormat::TarXz | DetectedFormat::ArchPkgTarXz | DetectedFormat::Xz => {
            ChunkAlgorithm::xz_default()
        }
        DetectedFormat::TarBzip2 => ChunkAlgorithm::fixed_aligned(1024 * 1024, 100 * 1024),
        DetectedFormat::Zip => ChunkAlgorithm::fixed_aligned(1024 * 1024, 4096),
        DetectedFormat::Deb | DetectedFormat::Rpm => ChunkAlgorithm::package_default(),
        DetectedFormat::Unknown => {
            if zero_density(sample) >= 0.60 {
                ChunkAlgorithm::image_default()
            } else {
                ChunkAlgorithm::fastcdc_default()
            }
        }
    }
}

fn detect_format(path: &Path, sample: &[u8]) -> DetectedFormat {
    let lower_path = path.to_string_lossy().to_ascii_lowercase();

    if looks_like_oci_layer(path) {
        if matches_extension_chain(path, &["tar", "gz"])
            || matches_extension_chain(path, &["tgz"])
            || is_gzip(sample)
        {
            return DetectedFormat::OciLayerTarGzip;
        }
        if matches_extension_chain(path, &["tar", "zst"])
            || matches_extension_chain(path, &["tar", "zstd"])
            || is_zstd(sample)
        {
            return DetectedFormat::OciLayerTarZstd;
        }
        if matches_extension_chain(path, &["tar"]) || is_tar(sample) {
            return DetectedFormat::OciLayerTar;
        }
    }

    if matches_extension_chain(path, &["pkg", "tar", "zst"]) {
        return DetectedFormat::ArchPkgTarZstd;
    }
    if matches_extension_chain(path, &["pkg", "tar", "xz"]) {
        return DetectedFormat::ArchPkgTarXz;
    }
    if matches_extension_chain(path, &["tar", "gz"]) || matches_extension_chain(path, &["tgz"]) {
        return DetectedFormat::TarGzip;
    }
    if matches_extension_chain(path, &["tar", "zst"])
        || matches_extension_chain(path, &["tar", "zstd"])
        || matches_extension_chain(path, &["tzst"])
    {
        return DetectedFormat::TarZstd;
    }
    if matches_extension_chain(path, &["tar", "xz"]) || matches_extension_chain(path, &["txz"]) {
        return DetectedFormat::TarXz;
    }
    if matches_extension_chain(path, &["tar", "bz2"]) || matches_extension_chain(path, &["tbz2"]) {
        return DetectedFormat::TarBzip2;
    }
    if matches_extension_chain(path, &["tar"]) || matches_extension_chain(path, &["cpio"]) {
        return if matches_extension_chain(path, &["cpio"]) {
            DetectedFormat::Cpio
        } else {
            DetectedFormat::Tar
        };
    }
    if matches_extension_chain(path, &["deb"]) {
        return DetectedFormat::Deb;
    }
    if matches_extension_chain(path, &["rpm"]) {
        return DetectedFormat::Rpm;
    }
    if matches_extension_chain(path, &["apk"]) {
        return DetectedFormat::Apk;
    }
    if matches_extension(path, &["img", "raw", "vmdk", "vhd", "vhdx"]) {
        return DetectedFormat::RawDiskImage;
    }
    if matches_extension(path, &["iso"]) {
        return DetectedFormat::Iso9660;
    }
    if matches_extension(path, &["qcow", "qcow2"]) {
        return DetectedFormat::Qcow2;
    }
    if matches_extension(path, &["squashfs"]) {
        return DetectedFormat::Squashfs;
    }
    if matches_extension(path, &["erofs"]) {
        return DetectedFormat::Erofs;
    }
    if matches_extension(path, &["gz"]) || is_gzip(sample) {
        return DetectedFormat::Gzip;
    }
    if matches_extension(path, &["zst", "zstd"]) || is_zstd(sample) {
        return DetectedFormat::Zstd;
    }
    if matches_extension(path, &["xz"]) || is_xz(sample) {
        return DetectedFormat::Xz;
    }
    if matches_extension(path, &["zip"]) || is_zip(sample) {
        return DetectedFormat::Zip;
    }
    if is_iso9660(sample) {
        return DetectedFormat::Iso9660;
    }
    if sample.starts_with(b"QFI\xfb") {
        return DetectedFormat::Qcow2;
    }
    if sample.starts_with(b"hsqs") {
        return DetectedFormat::Squashfs;
    }
    if is_erofs(sample) {
        return DetectedFormat::Erofs;
    }
    if is_rpm(sample) {
        return DetectedFormat::Rpm;
    }
    if is_deb(sample) {
        return DetectedFormat::Deb;
    }
    if is_tar(sample) {
        return if lower_path.contains("layer.tar") {
            DetectedFormat::OciLayerTar
        } else {
            DetectedFormat::Tar
        };
    }
    if is_cpio(sample) {
        return DetectedFormat::Cpio;
    }
    if zero_density(sample) >= 0.60 {
        return DetectedFormat::RawDiskImage;
    }
    DetectedFormat::Unknown
}

fn looks_like_oci_layer(path: &Path) -> bool {
    let lower = path.to_string_lossy().to_ascii_lowercase();
    lower.contains("layer.tar") || lower.contains("/blobs/") || lower.contains("oci")
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

fn matches_extension_chain(path: &Path, suffix: &[&str]) -> bool {
    let chain = extension_chain(path);
    chain.len() >= suffix.len()
        && chain[chain.len() - suffix.len()..]
            .iter()
            .map(String::as_str)
            .eq(suffix.iter().copied())
}

fn extension_chain(path: &Path) -> Vec<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let trimmed = name.trim_start_matches('.');
            trimmed
                .split('.')
                .skip(1)
                .map(|part| part.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

fn is_tar(sample: &[u8]) -> bool {
    sample.get(257..262) == Some(&b"ustar"[..])
}

fn is_cpio(sample: &[u8]) -> bool {
    matches!(sample.get(0..6), Some(b"070701" | b"070702" | b"070707"))
}

fn is_gzip(sample: &[u8]) -> bool {
    sample.starts_with(&[0x1f, 0x8b])
}

fn is_zstd(sample: &[u8]) -> bool {
    sample.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
}

fn is_xz(sample: &[u8]) -> bool {
    sample.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00])
}

fn is_zip(sample: &[u8]) -> bool {
    sample.starts_with(b"PK\x03\x04")
}

fn is_rpm(sample: &[u8]) -> bool {
    sample.starts_with(&[0xed, 0xab, 0xee, 0xdb])
}

fn is_deb(sample: &[u8]) -> bool {
    sample.starts_with(b"!<arch>\n")
}

fn is_iso9660(sample: &[u8]) -> bool {
    sample.get(32_769..32_774) == Some(&b"CD001"[..])
}

fn is_erofs(sample: &[u8]) -> bool {
    sample.get(1024..1028) == Some(&[0xe2, 0xe1, 0xf5, 0xe0][..])
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
        let (small_mask, large_mask) = normalized_masks(self.avg_size, self.normalization_level);

        while offset < data.len() {
            let chunk_start = offset;
            let min_end = (offset + self.min_size).min(data.len());
            let max_end = (offset + self.max_size).min(data.len());
            let normalization_end = (chunk_start + self.avg_size).clamp(min_end, max_end);
            offset = min_end;

            let mut hash = 0u64;
            let mut selected_end = max_end;

            'search: while offset < normalization_end {
                if let Some(cutoff) = self.zero_run_cutoff {
                    if is_long_zero_run(data, offset, cutoff) {
                        selected_end = offset;
                        break 'search;
                    }
                }

                hash = update_gear_hash(hash, data[offset], &gear);
                offset += 1;
                if hash & small_mask == 0 {
                    selected_end = offset;
                    break 'search;
                }
            }

            while offset < max_end {
                if let Some(cutoff) = self.zero_run_cutoff {
                    if is_long_zero_run(data, offset, cutoff) {
                        selected_end = offset;
                        break;
                    }
                }

                hash = update_gear_hash(hash, data[offset], &gear);
                offset += 1;
                if hash & large_mask == 0 {
                    selected_end = offset;
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

fn update_gear_hash(hash: u64, byte: u8, gear: &[u64; 256]) -> u64 {
    hash.rotate_left(1).wrapping_add(gear[byte as usize])
}

fn normalized_masks(avg_size: usize, normalization_level: u32) -> (u64, u64) {
    let base_bits = avg_size.max(2).next_power_of_two().trailing_zeros().min(63);
    let early_bits = (base_bits + normalization_level).min(63);
    let late_bits = base_bits.saturating_sub(normalization_level).max(1);
    (bitmask(early_bits), bitmask(late_bits))
}

fn bitmask(bits: u32) -> u64 {
    if bits >= 63 {
        u64::MAX >> 1
    } else {
        (1u64 << bits) - 1
    }
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

#[cfg(test)]
mod tests {
    use super::{ChunkAlgorithm, Chunker, ContentDefinedChunker, CHUNK_SIZE};
    use std::path::Path;

    #[test]
    fn fastcdc_boundaries_cover_data_without_gaps() {
        let data: Vec<u8> = (0..(CHUNK_SIZE * 6 + 137))
            .map(|index| ((index * 31) % 251) as u8)
            .collect();
        let chunker = ContentDefinedChunker::default();
        let boundaries = chunker.chunk_boundaries(&data);

        assert!(!boundaries.is_empty());

        let mut expected_offset = 0usize;
        for (index, (offset, length)) in boundaries.iter().copied().enumerate() {
            assert_eq!(offset, expected_offset);
            assert!(length > 0 || data.is_empty());
            if index + 1 != boundaries.len() {
                assert!(length >= chunker.min_size);
            }
            assert!(length <= chunker.max_size || offset + length == data.len());
            expected_offset += length;
        }

        assert_eq!(expected_offset, data.len());
    }

    #[test]
    fn image_profile_aligns_boundaries_and_cuts_on_zero_runs() {
        let mut data: Vec<u8> = (0..(CHUNK_SIZE * 12))
            .map(|index| ((index * 17 + index / 97) % 251) as u8)
            .collect();
        let zero_start = CHUNK_SIZE + 64 * 1024;
        let zero_end = zero_start + 128 * 1024;
        for byte in &mut data[zero_start..zero_end] {
            *byte = 0;
        }

        let boundaries = ChunkAlgorithm::image_default().chunk_boundaries(&data);
        let ends = boundary_ends(&boundaries);

        assert!(ends.iter().all(|end| end % 4096 == 0));
        assert!(ends
            .iter()
            .any(|end| *end >= zero_start && *end <= zero_start + 4096));
    }

    #[test]
    fn fastcdc_profiles_produce_non_fixed_boundaries() {
        let data: Vec<u8> = (0..(CHUNK_SIZE * 8 + 4096))
            .map(|index| ((index * 7 + index / 11) % 251) as u8)
            .collect();

        let fixed = ChunkAlgorithm::Fixed256K.chunk_boundaries(&data);
        let fastcdc = ChunkAlgorithm::fastcdc_default().chunk_boundaries(&data);

        assert_ne!(fastcdc, fixed);
    }

    #[test]
    fn auto_detects_tar_zstd_profile() {
        let algorithm = super::detect_chunking_algorithm(Path::new("rootfs.tar.zst"), &[], false);

        assert_eq!(algorithm, ChunkAlgorithm::tar_zstd_default());
    }

    #[test]
    fn auto_detects_oci_layer_profile() {
        let mut sample = vec![0u8; 2048];
        sample[257..262].copy_from_slice(b"ustar");

        let algorithm =
            super::detect_chunking_algorithm(Path::new("blobs/sha256/layer.tar"), &sample, false);

        assert_eq!(algorithm, ChunkAlgorithm::oci_layer_default());
    }

    #[test]
    fn auto_detects_iso_profile_from_signature() {
        let mut sample = vec![0u8; 40_000];
        sample[32_769..32_774].copy_from_slice(b"CD001");

        let algorithm = super::detect_chunking_algorithm(Path::new("artifact.bin"), &sample, false);

        assert_eq!(algorithm, ChunkAlgorithm::iso_default());
    }

    #[test]
    fn auto_detects_deb_package_profile() {
        let algorithm = super::detect_chunking_algorithm(Path::new("foo.deb"), b"!<arch>\n", false);

        assert_eq!(algorithm, ChunkAlgorithm::package_default());
    }

    fn boundary_ends(boundaries: &[(usize, usize)]) -> Vec<usize> {
        let mut cumulative = 0usize;
        boundaries
            .iter()
            .enumerate()
            .filter_map(|(index, (_, length))| {
                cumulative += *length;
                (index + 1 != boundaries.len()).then_some(cumulative)
            })
            .collect()
    }
}
