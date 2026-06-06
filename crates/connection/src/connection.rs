// crates/connection/src/connection.rs
use std::net::SocketAddr;
use std::time::Instant;
use std::collections::VecDeque;
use riftnet_protocol::ReliableConnectionState;

pub struct PendingSend {
    pub data: Vec<u8>,
    pub reliable: bool,
}

pub struct Connection {
    pub endpoint: SocketAddr,
    pub is_server: bool,
    pub state: ReliableConnectionState,
    pub last_seen: Instant,

    // Cryptography State
    pub tx_nonce: u64,

    // Send Queue (Replacing LockFreeMPMC for now)
    // In Rust, if the ConnectionManager owns this exclusively on a single thread,
    // a standard VecDeque is zero-cost. If accessed cross-thread, use crossbeam::queue::ArrayQueue.
    pub pending_queue: VecDeque<PendingSend>,
    pub pending_bytes: usize,

    pub max_pending_bytes: usize,
}

impl Connection {
    pub fn new(endpoint: SocketAddr, is_server: bool) -> Self {
        Self {
            endpoint,
            is_server,
            state: ReliableConnectionState::new(),
            last_seen: Instant::now(),
            // Nonce parity: Server uses odd, Client uses even to prevent reflection
            tx_nonce: if is_server { 1 } else { 0 },
            pending_queue: VecDeque::with_capacity(1024),
            pending_bytes: 0,
            max_pending_bytes: 1024 * 1024, // 1MB queue limit
        }
    }

    pub fn queue_pending_send(&mut self, data: &[u8], reliable: bool) {
        let size = data.len();

        if self.pending_bytes + size > self.max_pending_bytes {
            eprintln!("QueuePendingSend dropped data: capacity exceeded for {}", self.endpoint);
            return;
        }

        self.pending_queue.push_back(PendingSend {
            data: data.to_vec(),
            reliable,
        });

        self.pending_bytes += size;
    }

    pub fn update(&mut self, now: Instant, send_func: impl Fn(&[u8])) {
        // 1. Process Retransmissions
        let resends = self.state.process_retransmissions(now);
        for packet in resends {
            // Note: In full implementation, this needs to bypass the pending_queue
            // and go straight to encryption -> wire.
            send_func(&packet);
        }

        // 2. Process pending application data queue
        while let Some(ps) = self.pending_queue.pop_front() {
            self.pending_bytes -= ps.data.len();

            // Here is where you would compress and encrypt `ps.data` before sending
            // let wire_packet = packet_factory::create(ps.data, ps.reliable, ...);
            // send_func(&wire_packet);
        }
    }

    pub fn get_tx_nonce(&mut self) -> u64 {
        let current = self.tx_nonce;
        self.tx_nonce += 2;
        current
    }
}