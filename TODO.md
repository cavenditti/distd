## Plan: PPSPP-aligned performance recovery

DRAFT — The plan should not chase isolated micro-optimizations in the current tree-stream design. The benchmark and roadmap together point to a structural issue: `distd` is still paying whole-tree, whole-hash-list, and install-path reconstruction costs on the hot path, while HTTP wins by doing almost no discovery and streaming bytes cheaply. The shortest path to “beat `rsync`/`zsync` on updates by margin” and “stop being embarrassingly slower than HTTP on bulk fetches” is a staged redesign that first fixes benchmark truthfulness, then replaces the current path-keyed gRPC/tree stream with revision-aware manifests plus PPSPP-style possession exchange, and only then tunes the bulk data path. The main pressure points are in distd.proto, item.rs, client.rs, grpc.rs, fs_storage.rs, and distd.rs.

**Steps**

1. Establish trustworthy benchmarks before changing the protocol.
   - Fix the `distd` benchmark path in distd.rs so multi-file workloads do not only transfer the first file, `update()` does not silently fall back to full transfer from mod.rs, and correctness is validated against output bytes rather than only process exit.
   - Keep `RunMetrics::finalize()` in metrics.rs but ensure `source_bytes` and `bytes_transferred` reflect actual `distd` work.
   - This prevents false positives on `many-small-files` and creates a stable baseline for later phases.

2. Split artifact identity from transport path identity.
   - Replace the current path-keyed, single-file bias around `Item` in item.rs and metadata maps in metadata.rs with an explicit artifact identity: `item_id`, `version`, `root_hash`, and manifest metadata, matching TODO.md.
   - `Server::publish_item()` in server.rs should publish a manifest/revision object rather than treating a filesystem path as the durable identity.
   - This is the prerequisite for sane update semantics, bounded reconciliation, staged activation, and later peer/swarm support.

3. Replace eager whole-object materialization with manifest-first metadata.
   - Trim `Item` so it does not eagerly store both all leaf chunks and all subtree hashes as persistent vectors from item.rs unless specifically required by the transfer phase.
   - Rework `Item::new()` in item.rs and the tree helpers in hash.rs to produce a compact manifest/root description first, deferring detailed traversal data until requested.
   - Eliminate duplicate root/tree work in `Server::publish_item()` around server.rs.
   - The goal is lower publish-time CPU, lower RSS, and less repeated hashing for large objects.

4. Replace whole-hash-list reconciliation with PPSPP-style possession exchange.
   - Remove the current “send all local hashes” update basis from client.rs and the `Hashes`-based protocol field in distd.proto.
   - Introduce a manifest-first handshake: client asks for a target revision, validates the signed or authoritative manifest, then exchanges possession via binmap/bitfield semantics as called for in TODO.md.
   - For large datasets, add bounded Merkle traversal for mismatch regions instead of enumerating chunk sets, matching TODO.md.
   - This is the most important change for beating `rsync`/`zsync` on update workloads at scale.

5. Replace the current serialized-node stream with chunk-addressed data transfer.
   - Retire the opaque `SerializedTree` payload shape in distd.proto and the current `Distd::tree_transfer()` node batching in grpc.rs.
   - The data plane should expose explicit chunk identity, offset/bin information, and transfer messages aligned with `HAVE` and `REQUEST`.
   - This removes custom tree reconstruction from the hot path and makes bulk transfer closer to “stream requested chunks efficiently” rather than “serialize internal nodes and rebuild structure on the client.”

6. Move the client write path to a true local CAS plus staged assembly.
   - Implement the storage direction described in TODO.md: store chunks in a content-addressed object store, assemble the target in staging, then atomically activate.
   - Today `FsStorage` explicitly writes into a filesystem-oriented store in fs_storage.rs and reconstructs incrementally in fs_storage.rs; that path should stop being the transfer hot path.
   - A CAS-first receive path should only validate/store chunks once, then materialize files from known-good local objects.
   - This should cut memory spikes, reduce duplicate buffering, and make resume logic natural.

7. Introduce a bulk-transfer fast path for low-reuse cases.
   - Keep PPSPP semantics, but optimize the degenerate case where the client has little or nothing: manifest known, possession mostly empty, request long contiguous chunk ranges aggressively.
   - The server path in grpc.rs and client receive path in client.rs should favor streaming large chunk windows with minimal per-chunk round trips and minimal intermediate allocations.
   - The intent is not to beat HTTP on raw full-file loopback, but to move from current ~30–36 MiB/s behavior to a “sanity zone” where `distd` is no longer orders of magnitude behind on bulk fetches.

8. Add adaptive chunking only after the protocol/model are corrected.
   - The fixed 256 KiB chunking in chunks.rs and the missing chunk-adapter noted in README.md are real limitations, but should be addressed after protocol and storage cleanup.
   - Phase this as: keep fixed chunking for PPSPP-baseline compatibility first, then add a content-defined or workload-adaptive chunker behind a stable manifest/chunk interface.
   - This is the main lever for eventually beating `zsync`/`rsync` by margin on shifted/edited binary workloads rather than only exact dedup-heavy cases.

9. Expand from single-file to multi-file artifacts.
   - Generalize the single-file `Item` model in item.rs so manifests can represent a directory tree or image artifact cleanly.
   - Align the benchmark harness and the product model so workload.rs no longer exercises a shape that the product only partially supports.
   - This closes the current gap where `packed-archive` and `many-small-files` are only partially meaningful for `distd`.

10. Re-baseline storage backends after the protocol change.
    - Revisit the server’s hardwired `HashMapStorage` entry path in main.rs and decide whether redb.rs or another persistent CAS should become the default benchmark path.
    - Avoid benchmarking a product architecture intended for resumable, persistent CAS against an ephemeral in-memory publish model unless the benchmark explicitly isolates transport-only costs.
    - This step matters less than the protocol/model work, but it is required for credible production-oriented numbers.

**Verification**

- Benchmark gates should be defined in two layers.
- First, benchmark integrity:
  - `distd` must transfer and verify the correct full output for multi-file and update workloads.
  - `distd` must report real `bytes_transferred`, not inferred workload size.
- Second, performance targets:
  - On update workloads such as `low-delta-revision-update` and `high-delta-revision-update`, `distd` should beat `rsync` and `zsync` consistently once manifest-first reconciliation and bounded traversal land.
  - On bulk fetch workloads such as `single-large-file` and `packed-archive`, `distd` does not need to match HTTP loopback, but it should move out of the current “orders of magnitude slower” range and show stable throughput without extreme RSS spikes.
- Manual checks should explicitly compare:
  - cold start full fetch
  - warm resume
  - low-delta update
  - high-delta update
  - multi-file artifact activation from staging

**Decisions**

- Chose PPSPP-aligned redesign over tuning the current custom tree stream, because TODO.md and TODO.md already describe the needed transport primitives and the current protocol in distd.proto is the main architectural mismatch.
- Chose “benchmark truthfulness first” because current `distd` benchmark results are partially distorted by distd.rs.
- Chose “manifest/CAS/staging before chunk-adapter” because chunking improvements alone will not fix whole-hash-list reconciliation, tree-stream overhead, or install-path reconstruction.
