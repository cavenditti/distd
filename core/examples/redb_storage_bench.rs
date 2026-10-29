//! Deterministic, correctness-checked storage microbenchmark (no network).
//! Run: cargo run --release -p distd_core --features redb --example redb_storage_bench

#[cfg(not(feature = "redb"))]
fn main() {
    eprintln!("Enable the redb feature to run this benchmark.");
    std::process::exit(1);
}

#[cfg(feature = "redb")]
mod benchmark {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use bytes::Bytes;
    use distd_core::chunk_storage::{redb::RedbStorage, ChunkStorage};
    use distd_core::chunks::{ChunkAlgorithm, CHUNK_SIZE};
    use distd_core::hash::hash;
    use distd_core::item::Item;

    pub struct CountingAllocator;
    static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

    // Count allocation traffic, not live memory or peak RSS. This example is
    // single-threaded; every measured operation uses the same allocator.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
            unsafe { System.realloc(ptr, layout, size) }
        }
    }

    fn measure<T>(case: &str, phase: &str, sample: usize, operation: impl FnOnce() -> T) -> T {
        let allocations = ALLOCATIONS.load(Ordering::Relaxed);
        let bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
        let start = Instant::now();
        let result = operation();
        let elapsed = start.elapsed();
        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
        let bytes = ALLOCATED_BYTES.load(Ordering::Relaxed) - bytes;
        println!(
            "{{\"case\":\"{case}\",\"phase\":\"{phase}\",\"sample\":{sample},\"elapsed_ns\":{},\"allocations\":{allocations},\"allocated_bytes\":{bytes}}}",
            elapsed.as_nanos()
        );
        result
    }

    fn check_reads(storage: &RedbStorage, item: &Item, case: &str, sample: usize) {
        let root = item.metadata.root.hash;
        let node = measure(case, "tree", sample, || storage.get(&root).unwrap());
        assert_eq!(node.chunk_info(), item.metadata.root);
        assert_eq!(
            node.flatten().unwrap(),
            item.chunks
                .iter()
                .map(|chunk| chunk.hash)
                .collect::<Vec<_>>()
        );
        for (bytes, expected) in node.data().unwrap().iter().zip(&item.chunks) {
            assert_eq!(bytes.len() as u64, expected.size);
            assert_eq!(hash(bytes), expected.hash);
        }
        let chunks = measure(case, "indexed", sample, || {
            (0..item.chunks.len())
                .map(|index| storage.get_chunk_by_index(&root, index as u32).unwrap())
                .collect::<Vec<_>>()
        });
        for (bytes, expected) in chunks.iter().zip(&item.chunks) {
            assert_eq!(bytes.len() as u64, expected.size);
            assert_eq!(hash(bytes), expected.hash);
        }
        let mut output = Vec::with_capacity(item.metadata.root.size as usize);
        measure(case, "append", sample, || {
            for (index, chunk) in item.chunks.iter().enumerate() {
                assert_eq!(
                    storage.append_chunk_by_index(&root, index as u32, &mut output),
                    Some(chunk.size as usize)
                );
            }
        });
        let mut offset = 0;
        for chunk in &item.chunks {
            let end = offset + chunk.size as usize;
            assert_eq!(hash(&output[offset..end]), chunk.hash);
            offset = end;
        }
        assert_eq!(offset, output.len());
    }

    pub fn run() {
        let root = std::env::temp_dir().join(format!("distd-redb-bench-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        for case in ["unique-16mib", "repeated-16mib", "small-files-2048"] {
            let files = if case == "small-files-2048" {
                (0..2048u64)
                    .map(|index| {
                        let mut data = vec![0u8; 64];
                        data[..8].copy_from_slice(&index.to_le_bytes());
                        (PathBuf::from(format!("{index:04}.bin")), Bytes::from(data))
                    })
                    .collect::<Vec<_>>()
            } else {
                let mut data = vec![7u8; 16 * 1024 * 1024];
                if case == "unique-16mib" {
                    for (index, chunk) in data.chunks_mut(CHUNK_SIZE).enumerate() {
                        chunk[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    }
                }
                vec![(PathBuf::from("payload.bin"), Bytes::from(data))]
            };
            for sample in 0..5 {
                let path = root.join(format!("{case}-{sample}.redb"));
                let storage = RedbStorage::new(&path).unwrap();
                let item = storage
                    .prepare_revision_from_files(
                        case.to_string(),
                        case.to_string(),
                        PathBuf::from("artifact"),
                        0,
                        None,
                        files.clone(),
                        ChunkAlgorithm::default(),
                    )
                    .unwrap();
                let candidate = item.clone();
                let payloads = files.clone();
                measure(case, "commit", sample, || {
                    storage
                        .commit_revision_from_files(candidate, payloads)
                        .unwrap()
                });
                drop(storage);
                let storage = RedbStorage::new(&path).unwrap();
                check_reads(&storage, &item, case, sample);
                storage.verify_committed_revision(&item).unwrap();
                drop(storage);
                std::fs::remove_file(path).unwrap();
            }
        }
        std::fs::remove_dir(root).unwrap();
    }
}

#[cfg(feature = "redb")]
#[global_allocator]
static ALLOCATOR: benchmark::CountingAllocator = benchmark::CountingAllocator;

#[cfg(feature = "redb")]
fn main() {
    benchmark::run();
}
