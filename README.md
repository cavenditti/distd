# distd

distd (short for **DIST**ribute**D**, or **DIST**ribution **D**aemon, if you prefer) is a tool for updates distribution.

> [!CAUTION]
> pre-alpha software, use at your own risk

## Rationale
This is meant to efficiently store updates deltas and push updates to clients, optimizing bandwidth usage at all levels
and reducing requirements on the providing server(s).

This is meant to cover use cases of rsync, zsync and casync, while providing an integrated solution to replicate
the (eventually private storage repo) and keep all items deduplicated.


## Requirements
- Rust 1.82.0-nightly with cargo

## Architecture
- Every participating node is a peer, they may act both as server and client
- It's layered, forming a distribution tree basically: clients may act as servers for clients in a lower level
- Root server computes BLAKE3 hash trees (and assigns a 64-bit uid to each hash? To reduce overhead)

> [!NOTE]
> We're computing the full hash-tree of each file, intermediate hashes are different than just calling
> `blake3::hash(some_bytes)` for that sub-tree. This is because BLAKE3 is meant to be resistant to
> length-extension attacks but we are not using it for MAC and will ignore the Subtree-freeness of BLAKE3

## TODO:
### Short term (first pre-release):
- [x] ~~Implement merkle-tree computing~~
- [x] ~~Make tests not independent of CHUNK_SIZE~~
- [x] ~~Refactor hash-tree computing~~ (may be improved)
- [x] ~~Give each Item its own tree of hashes instead of Vec~~ (kinda done, we have the Arc to the root StorageChunkRef
    while keeping the list of all the leaf hashes)
- [x] ~~Make order between ChunkInfo and StoredChunkRef (even just renaming them may be enough)~~
- [x] ~~Implement diff algorithm comparing merkle-trees~~ won't do it
    > [!NOTE]
    > We may get away with this, as we have all leaf hashes and their differences. Shouldn't really be needed unless
    > there is the need to compute rolling hashes or doing something more fancy (in that case the approach would be
    > different anyway)
- [ ] Replace some Option with Result to have better visibility on errors (in progress), mostly in chunk storage
- [ ] Overall code clean up
- [ ] Tests for everything
- [x] ~~Make storage agnostic about hash computation, it's not its business~~
- [ ] **Implement FsStorage, to store items directly in the filesystem without deduplication (will be used by client)**
- [ ] Minimal client
- [ ] Evaluate whether to and assign a 64-bit uid to each hash to reduce network overhead or not

### Long term TODO:
- Implement (a subset of) PPSPP in Rust, see [PyPPSPP](https://github.com/justas-/PyPPSPP) as reference
- Add TLS, or use QUIC everywhere
- Make sure the approach is fine tuned for EROFS images, so that they can be chunked efficiently.
- Replace ptree with something unintrusive when running tests

#### P2P (TODO)
This works pretty much like a partitioned Bittorent swarm, but:
- It uses BLAKE3 instead of SHA1 o SHA256
- Only uses ~~µTP~~ kind-of-PPSPP over UDP transport [see at RFC7574 on ietf.org](https://datatracker.ietf.org/doc/rfc7574/)
- The equivalent of torrent/metainfo files are msgpack-encoded instead of bencode-encoded
- Every node is a peer in one or more swarm(s).
- chunk/piece size is currently hardcoded


## Code organization
- distd_core contains common data structures and algorithms
- distd_server is responsible of chunking, hashing and first serving the file
- distd_client fetches files from the server

## Current state
