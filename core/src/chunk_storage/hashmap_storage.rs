use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use blake3::Hash;

use crate::chunk_storage::ChunkStorage;
use crate::hash::merge_hashes;

use super::StoredChunkRef;

// Dead simple in-memory global storage
#[derive(Default, Clone)]
pub struct HashMapStorage {
    // This re-hashes the hashes, but nicely handles collisions in return
    // TODO we may use a Hasher that just returns the first n bytes of the SHA-256?
    data: Arc<RwLock<HashMap<Hash, Arc<StoredChunkRef>>>>,
}

impl ChunkStorage for HashMapStorage {
    fn get(&self, hash: &Hash) -> Option<Arc<StoredChunkRef>> {
        self.data.read().unwrap().get(hash).cloned()
    }

    fn insert_chunk(&self, chunk: &[u8]) -> Option<Arc<StoredChunkRef>> {
        if let Ok(mut data) = self.data.write() {
            //println!("Chunk: {:?}", &chunk);
            let size: u32 = chunk.len().try_into().unwrap(); // FIXME unwrap
            let hash = blake3::hash(&chunk);
            println!("[StorageInsert] Hash: {}, size: {}", hash, size);
            if let Some(raw_chunk) = data.get(&hash.clone()) {
                return Some(raw_chunk.clone());
            }
            data.try_insert(
                hash,
                Arc::new(StoredChunkRef::Stored {
                    hash,
                    data: Arc::new(Vec::from(chunk)),
                }),
            )
            .ok()
            .map(|x| x.clone())
        } else {
            None
        }
    }

    fn link(
        &self,
        left: Arc<StoredChunkRef>,
        right: Arc<StoredChunkRef>,
    ) -> Option<Arc<StoredChunkRef>> {
        let hash = merge_hashes(left.get_hash(), right.get_hash());
        println!(
            "[Storage Link ]: {}: {} + {}",
            hash,
            left.get_hash(),
            right.get_hash()
        );
        if let Ok(mut data) = self.data.write() {
            data.get(&hash).cloned().or(data
                .try_insert(hash, Arc::new(StoredChunkRef::Parent { hash, left, right }))
                .ok()
                .map(|x| x.clone()))
        } else {
            None
        }
    }

    fn chunks(&self) -> Vec<Hash> {
        self.data.read().unwrap().keys().cloned().collect()
    }

    fn size(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .values()
            .map(|x| match &**x {
                StoredChunkRef::Stored { hash: _, data } => data.len(),
                _ => 0,
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::CHUNK_SIZE;

    use super::*;
    use bytes::Bytes;
    use ptree::print_tree;

    #[test]
    fn test_hms_single_chunk_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(b"very few bytes");
        let len = data.len();
        s.insert(data);
        assert_eq!(len, s.size());
    }

    #[test]
    /// Multiple chunks, not aligned with CHUNK_SIZE
    fn test_hms_insertion() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(include_bytes!("../../../Cargo.lock"));
        let len = data.len();
        let root = s.insert(data).unwrap();
        println!("\nOriginal lenght: {}, stored length: {}", len, s.size());
        print_tree(&*root.to_owned()).unwrap();
        println!();
        assert!(len >= s.size());
    }

    //fn test_hms_multi_chunk_insertion() {

    #[test]
    fn test_hms_deduplicated() {
        const SIZE: usize = CHUNK_SIZE * 3;
        let s = HashMapStorage::default();
        let data = Bytes::from_static(&[0u8; SIZE]);
        println!("{:?}", data.len());
        let root = s.insert(data).unwrap();
        //print_tree(&*root.to_owned()).unwrap();
        assert_eq!(CHUNK_SIZE, s.size());

        // TODO make these indipendent of CHUNK_SIZE, just needs to recompute hashes per-chunk and check them
        let root_hash = "e78e8d3d053116365a3a98cf8580254cb6173cbb473d9f2f132fb7b7862e5665";
        assert_eq!(root.get_hash().to_string(), root_hash);

        let root_children = (
            "5d0b31ce6fa4b67269bc42019bde6dfbc5eefd8d13871ba10d972fd3fe559693",
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
        );
        assert_eq!(root.get_hash().to_string(), root_hash);
        assert_eq!(
            root._get_children().unwrap().0.get_hash().to_string(),
            root_children.0
        );
        assert_eq!(
            root._get_children().unwrap().1.get_hash().to_string(),
            root_children.1
        );

        let zero_chunk = "b6fb73fc46938c981e2b0b4b1ef282adcfc89854d01bfe3972fdc4785b41b2c7";
        let root_children_left_children = (
            zero_chunk,
            "a622c121567e4c936e0210532383b3eaef8486854aa65a188e152187a8b080b8",
        );
        assert_eq!(
            root._get_children()
                .unwrap()
                .0
                ._get_children()
                .unwrap()
                .0
                .get_hash()
                .to_string(),
            zero_chunk
        );
        assert_eq!(
            root._get_children()
                .unwrap()
                .0
                ._get_children()
                .unwrap()
                .1
                .get_hash()
                .to_string(),
            root_children_left_children.1
        );

        let hash_vec = root.flatten();
        assert_eq!(hash_vec.len(), 4);
        assert_eq!(hash_vec[0].to_string(), zero_chunk);
        assert_eq!(hash_vec[1].to_string(), zero_chunk);
        assert_eq!(hash_vec[2].to_string(), zero_chunk);
        // This is the last (empty) chunk.
        assert_eq!(
            hash_vec[3].to_string(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );

        let hash_set = root.hashes();
        assert_eq!(hash_set.len(), 2);

        let cloned = root.clone_data().unwrap();
        assert_eq!(cloned.len(), SIZE);
        for b in cloned {
            assert_eq!(b, 0u8);
        }
    }

    #[test]
    fn test_hms_2mb() {
        let s = HashMapStorage::default();
        let data = Bytes::from_static(include_bytes!("../../../1.MOV"));
        let len = data.len();
        let root = s.insert(data.clone()).unwrap();
        //print_tree(&*root.to_owned()).unwrap();
        assert!(len >= s.size());

        let cloned = root.clone_data().unwrap();
        for (i, b) in cloned.iter().enumerate() {
            //println!("{} {} {}", i, data[i], *b);
            assert_eq!(data[i], *b);
        }
    }
}
