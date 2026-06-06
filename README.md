RiftNet-rust is a high-performance, 100% pure Rust networking and state-reconciliation engine. It was originally developed in C++ as the custom backbone for real-time robotics simulations and RiftForged—a fully custom, deterministic MMO ARPG driven by continuous F=MA physics.

This repository represents the canonized port of that system into safe, idiomatic Rust. It is built strictly on an "Architecture First" philosophy: no black-box commercial engines, no unsafe shortcuts, and absolute deterministic server authority.

Core Philosophy & Capabilities
100% Pure Rust: The core networking and interpolation layers contain absolutely zero unsafe blocks. Memory safety is guaranteed by the compiler and strict use of the zerocopy crate.

Zero-Copy Serialization: State snapshots and packets are defined using #[repr(C, packed)]. Data is cast directly from the network buffer into application structs without intermediate allocations or cloning.

Deterministic Lockstep & Rollback: Designed for continuous-state physics (flat arrays, no hierarchical physics trees). The engine utilizes N-tick backpropagation. If the client detects a state-hash desynchronization, it snaps to the authoritative server state and rapidly resimulates the historical input buffer in a single frame.

Cryptographic Pipeline: Built-in AEAD pipeline utilizing ring (ChaCha20-Poly1305) and lz4_flex. Packet headers are authenticated as Associated Data (AAD) to prevent MITM packet-type injection, and Nonces are deterministically derived from the tick/sequence to minimize packet overhead.

FFI + ABI Boundary Ready: While the core is pure Rust, a clean, strictly isolated FFI boundary is maintained. This ensures the library can be consumed by C++ robotics environments and future Linux DPDK integration.

Architecture Pipeline
RiftNet decouples network transport from state simulation to ensure the 60Hz tick remains completely unblocked.

1. The Transport Layer (TokioReactor)
   Non-blocking UDP socket management using Tokio. Handles incoming/outgoing datagrams and passes raw byte slices up the chain.

2. The Connection Manager (Dispatcher)
   Maintains the state machines for all active sessions. Implements a sliding window protocol for reliable UDP delivery, tracking RTT (Jacobson/Karels Algorithm) and dynamically calculating Retransmission Timeouts (RTO) for packet backoff.

Crucially, the Connection Manager hands off a zero-copy slice of the application payload to the next layer, completely isolating transport logic from game state.

3. The Security & Compression Pipeline
   Plaintext
   [Plaintext State] -> [LZ4 Compress] -> [ChaCha20 Encrypt + Poly1305 Tag] -> [Wire]
   On receive, the payload is authenticated against the unencrypted routing headers (AAD) before decryption, silently dropping tampered packets.

4. The History Buffer & Interpolator
   A generic, ring-buffered rollback system (HistoryBuffer<T, I>). The client continually predicts forward based on local inputs. When an authoritative server snapshot arrives, the client hashes its local historical WorldState. If the hashes match, the timeline is intact. If they diverge, the timeline is overwritten and repaired.

Roadmap & Phase 2
ECDH Handshake: Implementation of Elliptic Curve Diffie-Hellman using X25519 for secure, ephemeral symmetric key exchange prior to session initialization.

Server-Side Command Buffering: Shifting from immediate input execution to tick-targeted command queues to entirely eliminate network-jitter-induced rollbacks.

DPDK Integration: Exposing the pure-Rust FFI boundary to interface with DPDK for ultra-low latency robotics hardware looping on Linux.

Why Build This?
Commercial engines often force developers into architectural compromises. RiftNet was built to maintain total control over the execution order, memory layout, and deterministic precision required for highly interactive, physics-heavy environments. Whether it is calculating the exact trajectory of a player thrown across a map by gravity or synchronizing the kinematics of a physical robotics gantry, the engine must execute flawlessly on both the server and the client.