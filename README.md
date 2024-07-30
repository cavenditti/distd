# distd

distd (short for distribution daemon) is a tool for updates distribution.

> [!CAUTION]
> pre-alpha software, use at your own risk

## Rationale
This is meant to push updates to clients optimizing bandwidth usage at all levels and reducing
requirements on the providing server(s).

## Architecture
This works pretty much like a multi-swarm Bittorent client and tracker, but:
- It uses BLAKE3 instead of SHA1 o SHA256
- Only uses µTP transport
- It is somewhat simpler and stricter, as the scope is way reduced
- The server acts both as a tracker and a client
- It's layered, each server has visibility only of same level and 1-level lower clients.
    Clients may act as servers for 1-level lower clients.
- chunk/piece size is currently hardcoded

## Code organization
- distd_core contains common data structures and algorithms for p2p sharing
- distd_server is responsible of chunking, hashing and first serving the file
- distd_client fetches files from the server
- distd_peer fetches and provides files and chunks to visible clients

