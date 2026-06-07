# RiftNet-Rust: High-Performance Distributed Foundation
RiftNet-Rust is a high-performance, 100% safe Rust networking and state-reconciliation engine. It serves as a proof-of-concept demonstration of deterministic networking principles, designed to show how complex simulation state (like real-time ARPG physics) can be synchronized across high-jitter, high-latency connections.

### Why this exists:
1. **URIEL Cognitive Framework Integration:** Providing a low-latency, deterministic I/O bridge for URIEL to observe and interact with simulated physical environments.
2. **Architectural Proof:** Demonstrating that Rust's ownership model can manage low-level network buffers as effectively as C++ while eliminating entire classes of memory-safety bugs.
3. **Collaboration Baseline:** A collaborative baseline for potential integration into the SPAWN Engine ecosystem.

### Key Capabilities
- **Safe-Rust Networking:** Uses `tokio` for I/O and `zerocopy` for wire-level performance; zero `unsafe` blocks.
- **Panic-Isolated Simulation:** Uses `catch_unwind` and lock-poisoning recovery in the thread pool to ensure the networking reactor remains operational even if the simulation encounters a fatal error.
- **Clock Synchronization:** Implements a PI-controller (Proportional-Integral) for dynamic drift correction, tested to maintain synchronization across rural, high-jitter internet links.