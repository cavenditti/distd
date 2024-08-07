# distd

distd (short for distribution daemon) is a tool for updates distribution.

> [!CAUTION]
> pre-alpha software, use at your own risk

## Rationale
This is meant to push updates to clients optimizing bandwidth usage at all levels and reducing
requirements on the providing server(s).

## Requirements
- Rust 1.82.0-nightly with cargo

## Architecture
This works pretty much like a partitioned Bittorent swarm, but:
- It uses BLAKE3 instead of SHA1 o SHA256
- Only uses µTP transport
- The equivalent of torrent/metainfo files are msgpack-encoded instead of bencode-encoded
- There are servers, clients and peers
- The server acts both as a tracker and a peer, it may also be a client
- It is somewhat simpler and stricter, as the scope is way reduced
- It's layered, forming a distribution tree basically: clients may act as servers for clients in a lower level
- chunk/piece size is currently hardcoded

## TODO:
- Implement diff algorithm
- Make sure the approach is fine tuned for EROFS images, so that they can be chunked efficiently.

## Code organization
- distd_core contains common data structures and algorithms for p2p sharing
- distd_server is responsible of chunking, hashing and first serving the file
- distd_client fetches files from the server
- distd_peer fetches and provides files and chunks to visible clients

## Current state
