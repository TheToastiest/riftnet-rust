# RiftNet-Rust Design Document

## 1. Threading & Concurrency
The `TaskThreadPool` provides a panic-isolated execution environment. By wrapping simulation tasks in `catch_unwind`, we prevent simulation-side desyncs from crashing the I/O thread.

## 2. Deterministic Synchronization
The system follows a "Server Authoritative, Client Predictive" model.
- **Client:** Computes physics locally and stores the result in a `HistoryBuffer`.
- **Server:** Receives input, computes the canonical state, and broadcasts snapshots with a hash of the current world state.
- **Rollback:** If a hash mismatch is detected, the client resets to the last valid server snapshot and replays inputs from the `HistoryBuffer` within a single frame to catch up.

## 3. Zero-Copy Pipeline
To ensure minimal latency, the networking stack avoids heap allocation on the packet hot-path:
- Incoming bytes are mapped directly to `GeneralPacketHeader` structs using `zerocopy::FromBytes`.
- Outgoing packets are constructed using a fixed-size `VecDeque` or `BytesMut` to keep memory pressure stable at high tick rates.

## 4. Pipeline-Based Security (Phase 2)
The architecture supports a `NetworkPipeline` trait for dependency injection. This allows for modular insertion of encryption (ChaCha20-Poly1305) and compression (LZ4/Zstd) between the application layer and the network wire.