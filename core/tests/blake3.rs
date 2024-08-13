use blake3::guts::{parent_cv, ChunkState, BLOCK_LEN, CHUNK_LEN};
use bytes::BufMut;
use std::io::{Read, Write};

use blake3::{Hash, Hasher};

#[test]
fn test_parents() {
    let mut hasher = Hasher::new();
    let mut buf = [0; CHUNK_LEN];

    buf[0] = 'a' as u8;
    hasher.update(&buf);
    let chunk0_cv = ChunkState::new(0).update(&buf).finalize(false);

    buf[0] = 'b' as u8;
    hasher.update(&buf);
    let chunk1_cv = ChunkState::new(1).update(&buf).finalize(false);

    hasher.update(b"c");
    let chunk2_cv = ChunkState::new(2).update(b"c").finalize(false);

    let parent = parent_cv(&chunk0_cv, &chunk1_cv, false);
    let root = parent_cv(&parent, &chunk2_cv, true);
    assert_eq!(hasher.finalize(), root);
}

#[test]
fn test_blake3() {
    // Define the input data.
    let mut buf = [0u8; CHUNK_LEN * 2 + 42];
    let mut chunks_hashes: Vec<Hash> = vec![];
    let mut hasher = Hasher::new();

    buf.chunks(CHUNK_LEN).enumerate().map(|(i, x)| {
        hasher.update(x);
        ChunkState::new(i as u64).update(x).finalize(false)
    });

    let final_hash = hasher.finalize();

    for i in chunks_hashes.iter().skip(1) {
        //let parent_node = parent_cv(&one, &i, false);
    }

    //println!("Parent Hash: {}", parent_node);
    //let parent_node = parent_cv(&hash1, &hash2, true);
}
