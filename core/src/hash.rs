use blake3::{Hash, Hasher};

pub fn merge_hashes(left: &Hash, right: &Hash) -> Hash {
    let mut combined_hashes = left.as_bytes().to_vec();
    combined_hashes.extend(right.as_bytes());
    blake3::hash(&combined_hashes)
}

/// Hashing function. Uses BLAKE3 but without Subtree-freeness
pub fn hash(input_data: &[u8]) -> Hash {
    // Split input data into chunks.
    let chunk_len = blake3::guts::CHUNK_LEN; // 1024, should we fix it here?
    let num_chunks = (input_data.len() + chunk_len - 1) / chunk_len;
    let mut chunk_hashes = Vec::new();

    for i in 0..num_chunks {
        let chunk_start = i * chunk_len;
        let chunk_end = usize::min(chunk_start + chunk_len, input_data.len());
        let chunk = &input_data[chunk_start..chunk_end];

        // Hash the chunk.
        let mut chunk_hasher = Hasher::new();
        chunk_hasher.update(chunk);
        let chunk_hash = chunk_hasher.finalize();
        chunk_hashes.push(chunk_hash);
    }

    // Combine chunk hashes into parent hashes (manual tree construction).
    while chunk_hashes.len() > 1 {
        let mut parent_hashes = Vec::new();
        for pair in chunk_hashes.chunks(2) {
            let parent_hash = if pair.len() == 2 {
                merge_hashes(&pair[0], &pair[1])
            } else {
                // Odd number of chunks, last one moves up directly.
                pair[0]
            };
            parent_hashes.push(parent_hash);
        }
        chunk_hashes = parent_hashes;
    }

    chunk_hashes[0]
}


#[cfg(test)]
mod tests {
    use super::hash;

    #[test]
    fn test_blake3_one_chunk() {
        let data = b"some random data";
        assert_eq!(blake3::hash(data), hash(data))
    }

    #[test]
    /// We're doing it differently, so they should differ
    fn test_blake3_multiple_chunks() {
        let data = [0u8; 10000];
        assert_ne!(blake3::hash(&data), hash(&data))
    }
}
