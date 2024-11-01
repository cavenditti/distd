use blake3::guts::{parent_cv, ChunkState, CHUNK_LEN};

use blake3::Hasher;

#[test]
fn parents() {
    let mut hasher = Hasher::new();
    let mut buf = [0; CHUNK_LEN];

    buf[0] = b'a';
    hasher.update(&buf);
    let chunk0_cv = ChunkState::new(0).update(&buf).finalize(false);

    buf[0] = b'b';
    hasher.update(&buf);
    let chunk1_cv = ChunkState::new(1).update(&buf).finalize(false);

    hasher.update(b"c");
    let chunk2_cv = ChunkState::new(2).update(b"c").finalize(false);

    let parent = parent_cv(&chunk0_cv, &chunk1_cv, false);
    let root = parent_cv(&parent, &chunk2_cv, true);
    assert_eq!(hasher.finalize(), root);
}
