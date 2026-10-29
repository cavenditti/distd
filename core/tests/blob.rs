use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Weak},
};

use bytes::Bytes;
use distd_core::{
    blob::{BlobChunkSource, BlobReader, MissingChunk},
    chunk_storage::{
        cas::CasStorage, fs_storage::FsStorage, hashmap_storage::HashMapStorage, node::ArcChunk,
        ChunkStorage,
    },
    chunks::{ChunkAlgorithm, ChunkInfo},
    hash::{hash, Hash},
    item::Item,
};
use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};

fn prepare(data: &[u8], algorithm: ChunkAlgorithm) -> Item {
    HashMapStorage::default()
        .prepare_revision(
            "blob-test".into(),
            "blob-test".into(),
            "logical.bin".into(),
            1,
            None,
            Bytes::copy_from_slice(data),
            algorithm,
        )
        .unwrap()
}

#[derive(Default)]
struct Source {
    chunks: RefCell<HashMap<Hash, Vec<u8>>>,
    requests: RefCell<Vec<Hash>>,
    last_backing: RefCell<Weak<Vec<u8>>>,
}

impl Source {
    fn populated(item: &Item, bytes: &[u8]) -> Self {
        let source = Self::default();
        let mut offset = 0;
        for chunk in &item.chunks {
            let end = offset + chunk.size as usize;
            source
                .chunks
                .borrow_mut()
                .insert(chunk.hash, bytes[offset..end].to_vec());
            offset = end;
        }
        source
    }
}

impl BlobChunkSource for Source {
    fn read_chunk(&self, expected: &ChunkInfo) -> io::Result<ArcChunk> {
        // The reader must release one backing before requesting the next.
        assert!(self.last_backing.borrow().upgrade().is_none());
        self.requests.borrow_mut().push(expected.hash);
        let bytes = Arc::new(
            self.chunks
                .borrow()
                .get(&expected.hash)
                .cloned()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, MissingChunk { chunk: *expected })
                })?,
        );
        *self.last_backing.borrow_mut() = Arc::downgrade(&bytes);
        Ok(bytes)
    }
}

#[test]
fn randomized_reads_match_a_file_for_fixed_and_content_defined_chunks() {
    let mut rng = StdRng::seed_from_u64(91827);
    let mut bytes = vec![0; 8195];
    rng.fill_bytes(&mut bytes);
    let cdc = ChunkAlgorithm::FastCdc {
        min_size: 8,
        avg_size: 16,
        max_size: 64,
        normalization_level: 2,
        seed: 314,
        profile_version: 1,
    };
    for algorithm in [ChunkAlgorithm::fixed_aligned(32, 1), cdc] {
        let item = prepare(&bytes, algorithm);
        if algorithm == cdc {
            assert!(item
                .chunks
                .windows(2)
                .any(|pair| pair[0].size != pair[1].size));
        }
        let source = Source::populated(&item, &bytes);
        let mut reader = BlobReader::new(&source, &item).unwrap();
        let mut file = Cursor::new(&bytes);
        assert_eq!(reader.len(), bytes.len() as u64);
        assert!(!reader.is_empty());
        assert!(source.requests.borrow().is_empty());
        for _ in 0..1000 {
            let offset = rng.gen_range(0..bytes.len() + 100) as u64;
            let length = rng.gen_range(0..150);
            let mut actual = vec![0x5a; length];
            let mut expected = actual.clone();
            reader.seek(SeekFrom::Start(offset)).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            assert_eq!(
                reader.read(&mut actual).unwrap(),
                file.read(&mut expected).unwrap()
            );
            assert_eq!(actual, expected);
            assert_eq!(
                reader.stream_position().unwrap(),
                file.stream_position().unwrap()
            );
        }
        reader.rewind().unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
    }
}

#[test]
fn boundary_reads_and_read_at_preserve_cursor_and_cache_only_one_chunk() {
    let bytes = b"abcdefghabcdefghijklmnop";
    let item = prepare(bytes, ChunkAlgorithm::fixed_aligned(8, 1));
    let source = Source::populated(&item, bytes);
    let mut reader = BlobReader::new(&source, &item).unwrap();
    reader.seek(SeekFrom::Start(5)).unwrap();
    let mut output = [0; 10];
    assert_eq!(reader.read_at(6, &mut output).unwrap(), 10);
    assert_eq!(&output, b"ghabcdefgh");
    assert_eq!(reader.stream_position().unwrap(), 5);
    assert_eq!(
        source.requests.borrow().len(),
        1,
        "repeated chunk reuses verified backing"
    );
    assert_eq!(reader.read_at(16, &mut output).unwrap(), 8);
    assert_eq!(&output[..8], b"ijklmnop");
    assert_eq!(source.requests.borrow().len(), 2);
    assert_eq!(reader.read_at(24, &mut output).unwrap(), 0);
    assert_eq!(reader.read_at(u64::MAX, &mut output).unwrap(), 0);
    assert_eq!(reader.read_at(0, &mut []).unwrap(), 0);
    assert_eq!(source.requests.borrow().len(), 2);
}

#[test]
fn missing_chunks_allow_unrelated_reads_and_retry_without_reopening() {
    let bytes = b"abcdefghijkl";
    let item = prepare(bytes, ChunkAlgorithm::fixed_aligned(4, 1));
    let source = Source::populated(&item, bytes);
    source.chunks.borrow_mut().remove(&item.chunks[1].hash);
    let mut reader = BlobReader::new(&source, &item).unwrap();
    let mut output = [0x5a; 12];
    assert_eq!(reader.read(&mut output).unwrap(), 4);
    assert_eq!(&output[..4], b"abcd");
    assert_eq!(&output[4..], &[0x5a; 8]);
    let error = reader.read(&mut output[4..]).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let missing = error
        .get_ref()
        .unwrap()
        .downcast_ref::<MissingChunk>()
        .unwrap();
    assert_eq!(missing.chunk, item.chunks[1]);
    assert_eq!(reader.stream_position().unwrap(), 4);
    assert_eq!(reader.read_at(8, &mut output[8..]).unwrap(), 4);
    assert_eq!(&output[8..], b"ijkl");
    source
        .chunks
        .borrow_mut()
        .insert(missing.chunk.hash, b"efgh".to_vec());
    reader.read_exact(&mut output[4..]).unwrap();
    assert_eq!(&output, bytes);
}

#[test]
fn corrupt_and_wrong_length_chunks_never_expose_their_bytes() {
    let bytes = b"abcdefgh";
    let item = prepare(bytes, ChunkAlgorithm::fixed_aligned(4, 1));
    for invalid in [b"evil".as_slice(), b"too long", b"x"] {
        let source = Source::populated(&item, bytes);
        source
            .chunks
            .borrow_mut()
            .insert(item.chunks[1].hash, invalid.to_vec());
        let mut reader = BlobReader::new(&source, &item).unwrap();
        let mut output = [0x5a; 8];
        assert_eq!(reader.read(&mut output).unwrap(), 4);
        assert_eq!(&output[..4], b"abcd");
        assert_eq!(&output[4..], &[0x5a; 4]);
        assert_eq!(
            reader.read(&mut output[4..]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(reader.stream_position().unwrap(), 4);
        assert_eq!(&output[4..], &[0x5a; 4]);
        source
            .chunks
            .borrow_mut()
            .insert(item.chunks[1].hash, b"efgh".to_vec());
        reader.read_exact(&mut output[4..]).unwrap();
        assert_eq!(&output, bytes);
    }
}

#[test]
fn seek_checks_signed_bounds_without_changing_cursor_on_error() {
    let item = prepare(b"abcd", ChunkAlgorithm::default());
    let source = Source::default();
    let mut reader = BlobReader::new(&source, &item).unwrap();
    assert_eq!(reader.seek(SeekFrom::End(-1)).unwrap(), 3);
    assert_eq!(reader.seek(SeekFrom::Current(-2)).unwrap(), 1);
    for from in [
        SeekFrom::End(-5),
        SeekFrom::Current(-2),
        SeekFrom::Current(i64::MIN),
    ] {
        assert_eq!(
            reader.seek(from).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(reader.stream_position().unwrap(), 1);
    }
    reader.seek(SeekFrom::Start(u64::MAX)).unwrap();
    assert_eq!(
        reader.seek(SeekFrom::Current(1)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(reader.stream_position().unwrap(), u64::MAX);
    assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
    assert!(source.requests.borrow().is_empty());
}

#[test]
fn empty_files_are_readable_without_a_resident_empty_chunk() {
    let item = prepare(b"", ChunkAlgorithm::default());
    let source = Source::default();
    let mut reader = BlobReader::new(&source, &item).unwrap();
    assert!(reader.is_empty());
    assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
    assert!(source.requests.borrow().is_empty());
}

fn directory_item() -> Item {
    HashMapStorage::default()
        .prepare_revision_from_files(
            "directory".into(),
            "directory".into(),
            "logical".into(),
            1,
            None,
            vec![
                ("a".into(), Bytes::new()),
                ("nested/b".into(), Bytes::from_static(b"abcdefgh")),
                ("z".into(), Bytes::from_static(b"tail")),
            ],
            ChunkAlgorithm::fixed_aligned(4, 1),
        )
        .unwrap()
}

#[test]
fn directory_views_use_file_offsets_and_cannot_read_neighboring_files() {
    let item = directory_item();
    let source = Source::populated(&item, b"abcdefghtail");
    assert!(BlobReader::new(&source, &item).is_err());
    for (path, expected) in [
        ("a", b"".as_slice()),
        ("nested/b", b"abcdefgh"),
        ("z", b"tail"),
    ] {
        let mut reader = BlobReader::open_file(&source, &item, path).unwrap();
        assert_eq!(reader.len(), expected.len() as u64);
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output, expected);
        assert_eq!(reader.read_at(reader.len(), &mut [0; 1]).unwrap(), 0);
    }
    for path in ["missing", "../z", "/z", "nested/../z", "nested//b"] {
        assert!(BlobReader::open_file(&source, &item, path).is_err());
    }
    let file = prepare(b"file", ChunkAlgorithm::default());
    assert!(BlobReader::open_file(&source, &file, "file").is_err());
}

#[test]
fn malformed_metadata_fails_before_payload_access() {
    let source = Source::default();
    let valid = directory_item();
    let mut invalid = Vec::new();
    let mut item = valid.clone();
    item.manifest.root_hash = hash(b"forged");
    invalid.push(item);
    let mut item = valid.clone();
    item.manifest.total_size += 1;
    invalid.push(item);
    let mut item = valid.clone();
    item.manifest.entries[1].chunk_range.1 = u32::MAX;
    invalid.push(item);
    let mut item = valid.clone();
    item.manifest.entries[1].size += 1;
    invalid.push(item);
    let mut item = valid.clone();
    item.manifest.entries[1].relative_path = "../escape".into();
    invalid.push(item);
    let mut item = valid.clone();
    item.chunks[1].size = u64::MAX;
    invalid.push(item);
    let mut item = valid;
    item.chunks.clear();
    invalid.push(item);
    for item in invalid {
        let result = BlobReader::open_file(&source, &item, "nested/b");
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidData));
    }
    assert!(source.requests.borrow().is_empty());
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("distd-blob-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn check_storage<S: ChunkStorage + BlobChunkSource>(storage: &S) -> Item {
    let bytes = Bytes::from_static(b"abcdefghabcdefghij");
    let item = prepare(&bytes, ChunkAlgorithm::fixed_aligned(4, 1));
    storage
        .commit_revision(item.clone(), bytes.clone())
        .unwrap();
    check_reader(storage, &item, &bytes);
    item
}

fn check_reader(source: &dyn BlobChunkSource, item: &Item, expected: &[u8]) {
    let mut reader = BlobReader::new(source, item).unwrap();
    reader.seek(SeekFrom::Start(3)).unwrap();
    let mut output = Vec::new();
    reader.read_to_end(&mut output).unwrap();
    assert_eq!(output, expected[3..]);
}

#[test]
fn hashmap_and_filesystem_backends_read_without_materialization() {
    check_storage(&HashMapStorage::default());
    let dir = TempDir::new();
    let storage = FsStorage::new(dir.0.clone()).unwrap();
    let item = check_storage(&storage);
    assert!(!dir.0.join("logical.bin").exists());
    drop(storage);
    let reopened = FsStorage::new(dir.0.clone()).unwrap();
    check_reader(&reopened, &item, b"abcdefghabcdefghij");
}

#[cfg(feature = "redb")]
#[test]
fn redb_reads_individual_chunks_without_a_committed_tree_and_after_reopen() {
    use distd_core::chunk_storage::redb::RedbStorage;
    let dir = TempDir::new();
    let path = dir.0.join("chunks.redb");
    let storage = RedbStorage::new(&path).unwrap();
    let item = prepare(b"abcdefgh", ChunkAlgorithm::fixed_aligned(4, 1));
    let mut reader = BlobReader::new(&storage, &item).unwrap();
    assert_eq!(
        reader.read(&mut [0; 1]).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    storage.insert_chunk(b"efgh").unwrap();
    let mut output = [0; 4];
    assert_eq!(reader.read_at(4, &mut output).unwrap(), 4);
    assert_eq!(&output, b"efgh");
    assert!(storage.get(&item.manifest.root_hash).is_none());
    drop(reader);
    let item = check_storage(&storage);
    drop(storage);
    check_reader(
        &RedbStorage::new(&path).unwrap(),
        &item,
        b"abcdefghabcdefghij",
    );
}

#[test]
fn cas_reads_only_requested_chunks_and_verifies_untrusted_cas_bytes() {
    let dir = TempDir::new();
    let storage = CasStorage::new(dir.0.clone());
    let item = prepare(b"abcdefgh", ChunkAlgorithm::fixed_aligned(4, 1));
    storage.store(&item.chunks[1].hash, b"efgh").unwrap();
    let mut reader = BlobReader::new(&storage, &item).unwrap();
    let mut output = [0; 4];
    assert_eq!(reader.read_at(4, &mut output).unwrap(), 4);
    assert_eq!(&output, b"efgh");
    assert_eq!(
        reader.read(&mut output).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    // The legacy CAS accepts arbitrary bytes under a supplied key; the reader
    // must reject them even when the length matches the manifest.
    storage.store(&item.chunks[0].hash, b"evil").unwrap();
    output.fill(0x5a);
    assert_eq!(
        reader.read(&mut output).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(output, [0x5a; 4]);
}
