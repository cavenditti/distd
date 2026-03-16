use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::{write::GzEncoder, Compression};
use rand::RngCore;

/// Describes the kind of synthetic workload to generate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadKind {
    /// Single large binary file (e.g. ML model, disk image).
    SingleLargeFile,
    /// Many small files laid out as a directory tree.
    ManySmallFiles,
    /// A packed archive (tar) containing many small files.
    PackedArchive,
    /// Low-delta revision: small fraction of content changed.
    LowDeltaRevision,
    /// High-delta revision: large fraction of content changed.
    HighDeltaRevision,
    /// Dedup-heavy corpus: significant content duplication across files.
    DedupHeavy,
    /// A gzip-compressed tarball representative of package roots and source bundles.
    TarGzipArchive,
    /// A zstd-compressed tarball representative of modern package payloads.
    TarZstdArchive,
    /// An OCI-style compressed tar layer using gzip compression.
    OciLayerGzip,
    /// An OCI-style compressed tar layer using zstd compression.
    OciLayerZstd,
    /// A synthetic .deb package with control and data archives.
    DebPackage,
    /// A synthetic .apk package with package metadata and data payload.
    ApkPackage,
}

impl WorkloadKind {
    pub fn all() -> Vec<Self> {
        vec![
            Self::SingleLargeFile,
            Self::ManySmallFiles,
            Self::PackedArchive,
            Self::LowDeltaRevision,
            Self::HighDeltaRevision,
            Self::DedupHeavy,
            Self::TarGzipArchive,
            Self::TarZstdArchive,
            Self::OciLayerGzip,
            Self::OciLayerZstd,
            Self::DebPackage,
            Self::ApkPackage,
        ]
    }
}

impl fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SingleLargeFile => write!(f, "single-large-file"),
            Self::ManySmallFiles => write!(f, "many-small-files"),
            Self::PackedArchive => write!(f, "packed-archive"),
            Self::LowDeltaRevision => write!(f, "low-delta-revision"),
            Self::HighDeltaRevision => write!(f, "high-delta-revision"),
            Self::DedupHeavy => write!(f, "dedup-heavy"),
            Self::TarGzipArchive => write!(f, "tar-gzip-archive"),
            Self::TarZstdArchive => write!(f, "tar-zstd-archive"),
            Self::OciLayerGzip => write!(f, "oci-layer-gzip"),
            Self::OciLayerZstd => write!(f, "oci-layer-zstd"),
            Self::DebPackage => write!(f, "deb-package"),
            Self::ApkPackage => write!(f, "apk-package"),
        }
    }
}

impl std::str::FromStr for WorkloadKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "single-large-file" => Ok(Self::SingleLargeFile),
            "many-small-files" => Ok(Self::ManySmallFiles),
            "packed-archive" => Ok(Self::PackedArchive),
            "low-delta" => Ok(Self::LowDeltaRevision),
            "low-delta-revision" => Ok(Self::LowDeltaRevision),
            "high-delta" => Ok(Self::HighDeltaRevision),
            "high-delta-revision" => Ok(Self::HighDeltaRevision),
            "dedup-heavy" => Ok(Self::DedupHeavy),
            "tar-gzip-archive" | "tar-gz" | "tar.gz" => Ok(Self::TarGzipArchive),
            "tar-zstd-archive" | "tar-zst" | "tar.zst" => Ok(Self::TarZstdArchive),
            "oci-layer-gzip" | "oci-gzip" | "oci-layer-tar-gz" => Ok(Self::OciLayerGzip),
            "oci-layer-zstd" | "oci-zstd" | "oci-layer-tar-zst" => Ok(Self::OciLayerZstd),
            "deb-package" | "deb" => Ok(Self::DebPackage),
            "apk-package" | "apk" => Ok(Self::ApkPackage),
            _ => Err(format!(
                "unknown workload kind '{s}' (valid: single-large-file, many-small-files, packed-archive, low-delta, low-delta-revision, high-delta, high-delta-revision, dedup-heavy, tar-gzip-archive, tar-zstd-archive, oci-layer-gzip, oci-layer-zstd, deb-package, apk-package)"
            )),
        }
    }
}

/// Parameters controlling synthetic workload generation.
#[derive(Debug, Clone)]
pub struct WorkloadParams {
    pub large_file_mib: u64,
    pub small_file_count: u32,
    pub small_file_kib: u64,
    /// Fraction of data changed between revisions (0.0 .. 1.0).
    pub delta_fraction: f64,
    /// If true, generate minimal-size fixtures for smoke testing.
    pub smoke: bool,
}

/// A generated workload with source directory (revision 1) and optionally
/// a second revision for delta/update benchmarks.
#[derive(Debug, Clone)]
pub struct Workload {
    pub kind: WorkloadKind,
    /// Source data directory (the "server" artifact, revision 1).
    pub source_dir: PathBuf,
    /// Optional second revision source for delta benchmarks.
    pub source_dir_v2: Option<PathBuf>,
    /// Total bytes in revision 1.
    pub total_bytes_v1: u64,
    /// Total bytes in revision 2 (if any).
    pub total_bytes_v2: Option<u64>,
    /// Number of files in revision 1.
    pub file_count_v1: u32,
}

/// Generate all requested workloads under `root_dir`.
pub fn generate_workloads(
    root_dir: &Path,
    kinds: &[WorkloadKind],
    params: &WorkloadParams,
) -> Vec<Workload> {
    tracing::info!("Generating workloads under {}", root_dir.display());
    fs::create_dir_all(root_dir).expect("Failed to create workload root");

    kinds
        .iter()
        .map(|kind| {
            let dir = root_dir.join(kind.to_string());
            generate_one(&dir, kind, params)
        })
        .collect()
}

fn generate_one(base: &Path, kind: &WorkloadKind, params: &WorkloadParams) -> Workload {
    // Remove stale data from previous runs so that leftover files
    // (e.g. from a non-smoke run) don't contaminate the new workload.
    if base.exists() {
        fs::remove_dir_all(base).expect("Failed to clean workload directory");
    }
    match kind {
        WorkloadKind::SingleLargeFile => gen_single_large_file(base, params),
        WorkloadKind::ManySmallFiles => gen_many_small_files(base, params),
        WorkloadKind::PackedArchive => gen_packed_archive(base, params),
        WorkloadKind::LowDeltaRevision => gen_delta_revision(base, params, false),
        WorkloadKind::HighDeltaRevision => gen_delta_revision(base, params, true),
        WorkloadKind::DedupHeavy => gen_dedup_heavy(base, params),
        WorkloadKind::TarGzipArchive => {
            gen_compressed_archive(base, params, CompressionFormat::Gzip)
        }
        WorkloadKind::TarZstdArchive => {
            gen_compressed_archive(base, params, CompressionFormat::Zstd)
        }
        WorkloadKind::OciLayerGzip => gen_oci_layer(base, params, CompressionFormat::Gzip),
        WorkloadKind::OciLayerZstd => gen_oci_layer(base, params, CompressionFormat::Zstd),
        WorkloadKind::DebPackage => gen_deb_package(base, params),
        WorkloadKind::ApkPackage => gen_apk_package(base, params),
    }
}

/// Helpers ─────────────────────────────────────────────

fn write_random_file(path: &Path, size: usize) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("Failed to create parent dir");
    }
    let mut rng = rand::thread_rng();
    // Write in 1 MiB chunks to keep memory bounded
    let mut f = fs::File::create(path).expect("Failed to create file");
    let chunk = 1024 * 1024;
    let mut remaining = size;
    let mut buf = vec![0u8; chunk.min(remaining)];
    while remaining > 0 {
        let n = chunk.min(remaining);
        rng.fill_bytes(&mut buf[..n]);
        f.write_all(&buf[..n]).expect("Failed to write");
        remaining -= n;
    }
}

fn dir_total_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    if dir.is_file() {
        return fs::metadata(dir).map(|m| m.len()).unwrap_or(0);
    }
    for entry in walkdir(dir) {
        if entry.is_file() {
            total += fs::metadata(&entry).map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

#[allow(dead_code)]
fn dir_file_count(dir: &Path) -> u32 {
    if dir.is_file() {
        return 1;
    }
    walkdir(dir).into_iter().filter(|p| p.is_file()).count() as u32
}

/// Return all files in a directory tree (non-recursive-friendly public API).
pub fn walkdir_files(dir: &Path) -> Vec<PathBuf> {
    walkdir(dir).into_iter().filter(|p| p.is_file()).collect()
}

fn walkdir(dir: &Path) -> Vec<PathBuf> {
    let mut out = vec![];
    if !dir.exists() {
        return out;
    }
    for entry in fs::read_dir(dir).expect("read_dir failed") {
        let entry = entry.expect("entry failed");
        let path = entry.path();
        if path.is_dir() {
            out.extend(walkdir(&path));
        } else {
            out.push(path);
        }
    }
    out
}

fn smoke_size(normal: usize, smoke: bool) -> usize {
    if smoke {
        normal.min(64 * 1024)
    } else {
        normal
    }
}

fn smoke_count(normal: u32, smoke: bool) -> u32 {
    if smoke {
        normal.min(10)
    } else {
        normal
    }
}

#[derive(Clone, Copy)]
enum CompressionFormat {
    Gzip,
    Zstd,
}

/// Generate a single large random binary file.
fn gen_single_large_file(base: &Path, params: &WorkloadParams) -> Workload {
    let v1 = base.join("v1");
    let size = smoke_size((params.large_file_mib as usize) * 1024 * 1024, params.smoke);
    let file = v1.join("artifact.bin");
    tracing::info!("Generating single-large-file: {} bytes", size);
    write_random_file(&file, size);

    Workload {
        kind: WorkloadKind::SingleLargeFile,
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: size as u64,
        total_bytes_v2: None,
        file_count_v1: 1,
    }
}

/// Generate a directory tree of many small files.
fn gen_many_small_files(base: &Path, params: &WorkloadParams) -> Workload {
    let v1 = base.join("v1");
    let count = smoke_count(params.small_file_count, params.smoke);
    let size = smoke_size((params.small_file_kib as usize) * 1024, params.smoke);
    tracing::info!("Generating many-small-files: {count} files × {size} bytes");

    for i in 0..count {
        // Spread across subdirectories to simulate real trees
        let sub = format!("d{:03}", i % 20);
        let file = v1.join(&sub).join(format!("file_{i:06}.bin"));
        write_random_file(&file, size);
    }

    let total = dir_total_bytes(&v1);
    Workload {
        kind: WorkloadKind::ManySmallFiles,
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: count,
    }
}

/// Generate a tar archive of many small files (packed representation).
fn gen_packed_archive(base: &Path, params: &WorkloadParams) -> Workload {
    let staging = base.join("staging");
    let v1 = base.join("v1");
    let count = smoke_count(params.small_file_count, params.smoke);
    let size = smoke_size((params.small_file_kib as usize) * 1024, params.smoke);
    tracing::info!("Generating packed-archive: {count} files × {size} bytes → tar");

    for i in 0..count {
        let sub = format!("d{:03}", i % 20);
        let file = staging.join(&sub).join(format!("file_{i:06}.bin"));
        write_random_file(&file, size);
    }

    // Pack into a tar (no compression — we want raw transfer comparison)
    fs::create_dir_all(&v1).expect("create v1");
    let tar_path = v1.join("archive.tar");
    pack_tar(&staging, &tar_path);

    // Clean up staging
    let _ = fs::remove_dir_all(&staging);

    let total = dir_total_bytes(&v1);
    Workload {
        kind: WorkloadKind::PackedArchive,
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: 1,
    }
}

fn gen_compressed_archive(
    base: &Path,
    params: &WorkloadParams,
    compression: CompressionFormat,
) -> Workload {
    let staging = base.join("staging");
    let v1 = base.join("v1");
    let count = smoke_count(params.small_file_count, params.smoke);
    let size = smoke_size((params.small_file_kib as usize) * 1024, params.smoke);
    tracing::info!(
        "Generating {}: {count} files × {size} bytes",
        match compression {
            CompressionFormat::Gzip => "tar-gzip-archive",
            CompressionFormat::Zstd => "tar-zstd-archive",
        }
    );

    populate_compressible_tree(&staging, count, size, "archive");

    fs::create_dir_all(&v1).expect("create v1");
    let tar_path = base.join("archive.tar");
    pack_tar(&staging, &tar_path);
    let archive_path = match compression {
        CompressionFormat::Gzip => v1.join("archive.tar.gz"),
        CompressionFormat::Zstd => v1.join("archive.tar.zst"),
    };
    compress_file(&tar_path, &archive_path, compression);
    let _ = fs::remove_file(&tar_path);
    let _ = fs::remove_dir_all(&staging);

    let total = dir_total_bytes(&v1);
    Workload {
        kind: match compression {
            CompressionFormat::Gzip => WorkloadKind::TarGzipArchive,
            CompressionFormat::Zstd => WorkloadKind::TarZstdArchive,
        },
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: 1,
    }
}

fn gen_oci_layer(base: &Path, params: &WorkloadParams, compression: CompressionFormat) -> Workload {
    let rootfs = base.join("rootfs");
    let v1 = base.join("v1");
    let count = smoke_count(params.small_file_count / 2, params.smoke).max(8);
    let size = smoke_size((params.small_file_kib as usize) * 1024, params.smoke);
    tracing::info!(
        "Generating {}: {count} files × {size} bytes",
        match compression {
            CompressionFormat::Gzip => "oci-layer-gzip",
            CompressionFormat::Zstd => "oci-layer-zstd",
        }
    );

    populate_rootfs_tree(&rootfs, count, size);

    fs::create_dir_all(&v1).expect("create v1");
    let tar_path = base.join("layer.tar");
    pack_tar(&rootfs, &tar_path);
    let layer_path = match compression {
        CompressionFormat::Gzip => v1.join("oci-layer.tar.gz"),
        CompressionFormat::Zstd => v1.join("oci-layer.tar.zst"),
    };
    compress_file(&tar_path, &layer_path, compression);
    let _ = fs::remove_file(&tar_path);
    let _ = fs::remove_dir_all(&rootfs);

    let total = dir_total_bytes(&v1);
    Workload {
        kind: match compression {
            CompressionFormat::Gzip => WorkloadKind::OciLayerGzip,
            CompressionFormat::Zstd => WorkloadKind::OciLayerZstd,
        },
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: 1,
    }
}

fn gen_deb_package(base: &Path, params: &WorkloadParams) -> Workload {
    let control_dir = base.join("control");
    let data_dir = base.join("data");
    let v1 = base.join("v1");
    let size = smoke_size((params.small_file_kib as usize) * 1024, params.smoke);
    let count = smoke_count(params.small_file_count / 4, params.smoke).max(6);
    tracing::info!("Generating deb-package: {count} files × {size} bytes");

    fs::create_dir_all(&control_dir).expect("mkdir control dir");
    fs::write(
        control_dir.join("control"),
        format!(
            "Package: distd-bench\nVersion: 1.0-1\nArchitecture: amd64\nMaintainer: distd bench\nDescription: synthetic benchmark package\nInstalled-Size: {}\n",
            (count as usize * size) / 1024
        ),
    )
    .expect("write control");
    populate_package_tree(&data_dir, count, size, "usr/share/distd-bench");

    let control_tar = base.join("control.tar");
    let control_tgz = base.join("control.tar.gz");
    let data_tar = base.join("data.tar");
    let data_tzst = base.join("data.tar.zst");
    pack_tar(&control_dir, &control_tar);
    pack_tar(&data_dir, &data_tar);
    compress_file(&control_tar, &control_tgz, CompressionFormat::Gzip);
    compress_file(&data_tar, &data_tzst, CompressionFormat::Zstd);

    fs::create_dir_all(&v1).expect("create v1");
    let deb_path = v1.join("package.deb");
    write_ar_archive(
        &deb_path,
        &[
            ("debian-binary", b"2.0\n".to_vec()),
            (
                "control.tar.gz",
                fs::read(&control_tgz).expect("read control archive"),
            ),
            (
                "data.tar.zst",
                fs::read(&data_tzst).expect("read data archive"),
            ),
        ],
    );

    let _ = fs::remove_dir_all(&control_dir);
    let _ = fs::remove_dir_all(&data_dir);
    let _ = fs::remove_file(&control_tar);
    let _ = fs::remove_file(&control_tgz);
    let _ = fs::remove_file(&data_tar);
    let _ = fs::remove_file(&data_tzst);

    let total = dir_total_bytes(&v1);
    Workload {
        kind: WorkloadKind::DebPackage,
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: 1,
    }
}

fn gen_apk_package(base: &Path, params: &WorkloadParams) -> Workload {
    let staging = base.join("staging");
    let v1 = base.join("v1");
    let size = smoke_size((params.small_file_kib as usize) * 1024, params.smoke);
    let count = smoke_count(params.small_file_count / 4, params.smoke).max(6);
    tracing::info!("Generating apk-package: {count} files × {size} bytes");

    fs::create_dir_all(&staging).expect("mkdir apk staging");
    fs::write(
        staging.join(".PKGINFO"),
        format!(
            "pkgname = distd-bench\npkgver = 1.0-r0\npkgdesc = synthetic benchmark package\nsize = {}\narch = x86_64\n",
            count as usize * size
        ),
    )
    .expect("write .PKGINFO");
    populate_package_tree(&staging.join("usr/share/distd-bench"), count, size, "");

    fs::create_dir_all(&v1).expect("create v1");
    let tar_path = base.join("package.tar");
    pack_tar(&staging, &tar_path);
    compress_file(&tar_path, &v1.join("package.apk"), CompressionFormat::Gzip);
    let _ = fs::remove_file(&tar_path);
    let _ = fs::remove_dir_all(&staging);

    let total = dir_total_bytes(&v1);
    Workload {
        kind: WorkloadKind::ApkPackage,
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: 1,
    }
}

/// Generate two revisions with a controlled delta fraction.
fn gen_delta_revision(base: &Path, params: &WorkloadParams, high: bool) -> Workload {
    let v1 = base.join("v1");
    let v2 = base.join("v2");
    let size = smoke_size((params.large_file_mib as usize) * 1024 * 1024, params.smoke);
    let delta = if high { 0.5 } else { params.delta_fraction };

    tracing::info!(
        "Generating {}-delta-revision: {} bytes, {:.0}% changed",
        if high { "high" } else { "low" },
        size,
        delta * 100.0,
    );

    // Build v1
    let file_v1 = v1.join("artifact.bin");
    write_random_file(&file_v1, size);

    // Build v2: copy v1 then mutate `delta` fraction of 256 KiB blocks
    fs::create_dir_all(&v2).expect("create v2");
    let file_v2 = v2.join("artifact.bin");
    fs::copy(&file_v1, &file_v2).expect("copy v1 to v2");

    mutate_file_blocks(&file_v2, delta);

    // Bump v2 file's mtime so that timestamp-aware tools (rsync) detect the change.
    // Without this, copy + mutate can finish within the same second, making rsync
    // think the file is unchanged (same size + same mtime).
    bump_mtime(&file_v2);

    let total_v2 = dir_total_bytes(&v2);

    Workload {
        kind: if high {
            WorkloadKind::HighDeltaRevision
        } else {
            WorkloadKind::LowDeltaRevision
        },
        source_dir: v1,
        source_dir_v2: Some(v2),
        total_bytes_v1: size as u64,
        total_bytes_v2: Some(total_v2),
        file_count_v1: 1,
    }
}

/// Generate a corpus with high content duplication.
fn gen_dedup_heavy(base: &Path, params: &WorkloadParams) -> Workload {
    let v1 = base.join("v1");
    let file_size = smoke_size((params.small_file_kib as usize) * 1024 * 4, params.smoke);
    let count = smoke_count(params.small_file_count, params.smoke);

    // Generate a "template" of random bytes, then replicate it with small variations.
    let mut template = vec![0u8; file_size];
    rand::thread_rng().fill_bytes(&mut template);

    tracing::info!("Generating dedup-heavy: {count} files × {file_size} bytes (~80% duplicate)",);

    for i in 0..count {
        let sub = format!("d{:03}", i % 20);
        let file = v1.join(&sub).join(format!("file_{i:06}.bin"));
        if let Some(parent) = file.parent() {
            fs::create_dir_all(parent).expect("mkdir");
        }
        // 80% of files are exact copies; 20% get partial mutation
        if i % 5 == 0 {
            let mut buf = template.clone();
            mutate_bytes(&mut buf, 0.1);
            fs::write(&file, &buf).expect("write");
        } else {
            fs::write(&file, &template).expect("write");
        }
    }

    let total = dir_total_bytes(&v1);
    Workload {
        kind: WorkloadKind::DedupHeavy,
        source_dir: v1,
        source_dir_v2: None,
        total_bytes_v1: total,
        total_bytes_v2: None,
        file_count_v1: count,
    }
}

/// Mutate `fraction` of 256 KiB blocks in a file with random data.
fn mutate_file_blocks(path: &Path, fraction: f64) {
    use std::io::{Seek, SeekFrom};

    let mut f = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open for mutation");
    let len = f.metadata().expect("metadata").len() as usize;
    let block_size = 256 * 1024; // match distd's CHUNK_SIZE
    let num_blocks = (len + block_size - 1) / block_size;
    let blocks_to_mutate = ((num_blocks as f64) * fraction).ceil() as usize;

    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; block_size];

    // Pick evenly spaced blocks to mutate for reproducibility
    let step = if blocks_to_mutate > 0 {
        num_blocks / blocks_to_mutate
    } else {
        num_blocks + 1
    };

    let mut mutated = 0;
    for block_idx in (0..num_blocks).step_by(step.max(1)) {
        if mutated >= blocks_to_mutate {
            break;
        }
        let offset = block_idx * block_size;
        let this_block = block_size.min(len - offset);
        rng.fill_bytes(&mut buf[..this_block]);
        f.seek(SeekFrom::Start(offset as u64)).expect("seek");
        f.write_all(&buf[..this_block]).expect("write block");
        mutated += 1;
    }
}

/// Set a file's modification time to 2 seconds in the future so
/// timestamp-based tools reliably see it as newer.
fn bump_mtime(path: &Path) {
    let future = std::time::SystemTime::now() + std::time::Duration::from_secs(2);
    let times = std::fs::FileTimes::new().set_modified(future);
    std::fs::File::options()
        .write(true)
        .open(path)
        .expect("open for mtime bump")
        .set_times(times)
        .expect("set mtime on v2 file");
}

/// Mutate `fraction` of bytes in a buffer.
fn mutate_bytes(buf: &mut [u8], fraction: f64) {
    let mut rng = rand::thread_rng();
    let num = ((buf.len() as f64) * fraction).ceil() as usize;
    let step = buf.len() / num.max(1);
    for i in (0..buf.len()).step_by(step.max(1)).take(num) {
        let mut b = [0u8; 1];
        rng.fill_bytes(&mut b);
        buf[i] = b[0];
    }
}

fn populate_compressible_tree(root: &Path, count: u32, size: usize, namespace: &str) {
    for i in 0..count {
        let sub = format!("d{:03}", i % 20);
        let file = root.join(&sub).join(format!("{namespace}_{i:06}.bin"));
        write_compressible_file(&file, size, i as u64 + 1);
    }
}

fn populate_rootfs_tree(root: &Path, count: u32, size: usize) {
    fs::create_dir_all(root.join("etc")).expect("mkdir etc");
    fs::create_dir_all(root.join("usr/bin")).expect("mkdir usr/bin");
    fs::create_dir_all(root.join("var/lib/distd")).expect("mkdir var/lib/distd");
    fs::write(
        root.join("etc/os-release"),
        b"NAME=distd bench\nID=distd\nVERSION_ID=1\n",
    )
    .expect("write os-release");

    for i in 0..count {
        let target = match i % 3 {
            0 => root.join("usr/bin").join(format!("tool-{i:04}")),
            1 => root.join("var/lib/distd").join(format!("blob-{i:04}.dat")),
            _ => root
                .join("usr/share/distd")
                .join(format!("asset-{i:04}.bin")),
        };
        write_compressible_file(&target, size, 0x1000 + i as u64);
    }
}

fn populate_package_tree(root: &Path, count: u32, size: usize, prefix: &str) {
    for i in 0..count {
        let relative = if prefix.is_empty() {
            PathBuf::from(format!("payload-{i:04}.bin"))
        } else {
            PathBuf::from(prefix).join(format!("payload-{i:04}.bin"))
        };
        write_compressible_file(&root.join(relative), size, 0x2000 + i as u64);
    }
}

fn write_compressible_file(path: &Path, size: usize, seed: u64) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("Failed to create parent dir");
    }
    let mut f = fs::File::create(path).expect("Failed to create file");
    let template = build_template(seed);
    let mut written = 0usize;
    let mut chunk_index = 0usize;
    while written < size {
        let remaining = size - written;
        let take = remaining.min(template.len());
        let mut block = template[..take].to_vec();
        if chunk_index % 7 == 0 {
            let zero_prefix = block.len().min(4096);
            for byte in block.iter_mut().take(zero_prefix) {
                *byte = 0;
            }
        }
        if chunk_index % 5 == 0 && block.len() > 128 {
            let marker = format!("seed={seed};chunk={chunk_index};path={}\n", path.display());
            let copy_len = marker.len().min(block.len());
            block[..copy_len].copy_from_slice(&marker.as_bytes()[..copy_len]);
        }
        f.write_all(&block).expect("write compressible block");
        written += take;
        chunk_index += 1;
    }
}

fn build_template(seed: u64) -> Vec<u8> {
    let mut template = vec![0u8; 64 * 1024];
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for (index, byte) in template.iter_mut().enumerate() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407 + index as u64);
        *byte = match index % 8 {
            0 | 1 => b'A' + (state as u8 % 26),
            2 => b'0' + (state as u8 % 10),
            3 => b'\n',
            4 => 0,
            _ => ((state >> 16) & 0xff) as u8,
        };
    }
    template
}

fn compress_file(src: &Path, dest: &Path, compression: CompressionFormat) {
    let data = fs::read(src).expect("read source for compression");
    match compression {
        CompressionFormat::Gzip => {
            let out = fs::File::create(dest).expect("create gzip output");
            let mut encoder = GzEncoder::new(out, Compression::new(6));
            encoder.write_all(&data).expect("gzip write");
            encoder.finish().expect("finish gzip");
        }
        CompressionFormat::Zstd => {
            let out = fs::File::create(dest).expect("create zstd output");
            let mut encoder =
                zstd::stream::write::Encoder::new(out, 10).expect("create zstd encoder");
            encoder.write_all(&data).expect("zstd write");
            encoder.finish().expect("finish zstd");
        }
    }
}

fn write_ar_archive(path: &Path, entries: &[(&str, Vec<u8>)]) {
    let mut file = fs::File::create(path).expect("create ar archive");
    file.write_all(b"!<arch>\n").expect("write ar header");

    for (name, data) in entries {
        let mut header = [b' '; 60];
        let identifier = format!("{}/", name);
        let identifier_bytes = identifier.as_bytes();
        let id_len = identifier_bytes.len().min(16);
        header[..id_len].copy_from_slice(&identifier_bytes[..id_len]);
        write_ar_decimal(&mut header[16..28], 0);
        write_ar_decimal(&mut header[28..34], 0);
        write_ar_decimal(&mut header[34..40], 0);
        header[40..48].copy_from_slice(b"100644  ");
        write_ar_decimal(&mut header[48..58], data.len() as u64);
        header[58..60].copy_from_slice(b"`\n");
        file.write_all(&header).expect("write ar member header");
        file.write_all(data).expect("write ar member data");
        if data.len() % 2 != 0 {
            file.write_all(b"\n").expect("write ar padding");
        }
    }
}

fn write_ar_decimal(field: &mut [u8], value: u64) {
    let rendered = format!("{value}");
    let start = field.len().saturating_sub(rendered.len());
    for slot in &mut field[..start] {
        *slot = b' ';
    }
    field[start..start + rendered.len()].copy_from_slice(rendered.as_bytes());
}

/// Create a tar archive from `src_dir` contents at `tar_path`.
pub fn pack_tar(src_dir: &Path, tar_path: &Path) {
    use std::io::BufWriter;

    let file = fs::File::create(tar_path).expect("create tar");
    let mut writer = BufWriter::new(file);

    // Simple tar packing: iterate files and write tar entries manually.
    // We write POSIX ustar-compatible headers.
    for entry_path in walkdir(src_dir) {
        if !entry_path.is_file() {
            continue;
        }
        let rel = entry_path
            .strip_prefix(src_dir)
            .expect("strip prefix")
            .to_string_lossy();
        let mut data = Vec::new();
        fs::File::open(&entry_path)
            .expect("open")
            .read_to_end(&mut data)
            .expect("read");
        write_tar_entry(&mut writer, &rel, &data);
    }

    // Two 512-byte zero blocks signal end of archive
    writer.write_all(&[0u8; 1024]).expect("end of archive");
}

fn write_tar_entry(w: &mut impl Write, name: &str, data: &[u8]) {
    let mut header = [0u8; 512];

    // Name field (0..100)
    let name_bytes = name.as_bytes();
    let copy_len = name_bytes.len().min(99);
    header[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

    // Mode (100..108) — 0644
    header[100..107].copy_from_slice(b"0000644");

    // UID, GID (108..124) — zeros ok
    // Size (124..136) — octal
    let size_str = format!("{:011o}", data.len());
    header[124..135].copy_from_slice(size_str.as_bytes());

    // Mtime (136..148) — 0
    header[136..147].copy_from_slice(b"00000000000");

    // Typeflag (156) — '0' regular file
    header[156] = b'0';

    // Magic (257..263) "ustar\0"
    header[257..263].copy_from_slice(b"ustar\0");
    // Version (263..265) "00"
    header[263..265].copy_from_slice(b"00");

    // Compute checksum (148..156)
    // First set the checksum field to spaces
    header[148..156].copy_from_slice(b"        ");
    let cksum: u32 = header.iter().map(|&b| b as u32).sum();
    let cksum_str = format!("{:06o}\0 ", cksum);
    header[148..156].copy_from_slice(cksum_str.as_bytes());

    w.write_all(&header).expect("write header");
    w.write_all(data).expect("write data");

    // Pad to 512-byte boundary
    let remainder = data.len() % 512;
    if remainder != 0 {
        let pad = 512 - remainder;
        w.write_all(&vec![0u8; pad]).expect("write padding");
    }
}
