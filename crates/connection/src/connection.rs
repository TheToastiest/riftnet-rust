use std::net::SocketAddr;
use std::time::Instant;
use std::collections::VecDeque;
use riftnet_protocol::{ReliableConnectionState, ReliabilityPacketHeader, GeneralPacketHeader, PacketType};
use zerocopy::AsBytes;
use riftnet_core::RiftError;
pub trait NetworkPipeline {
    fn process(&self, data: &[u8], nonce: u64) -> Result<Vec<u8>, RiftError>;
    fn inverse_process(&self, data: &[u8], nonce: u64) -> Result<Vec<u8>, RiftError>;
}

pub struct NullPipeline;

impl NetworkPipeline for NullPipeline {
    fn process(&self, data: &[u8], _nonce: u64) -> Result<Vec<u8>, RiftError> {
        Ok(data.to_vec())
    }
    fn inverse_process(&self, data: &[u8], _nonce: u64) -> Result<Vec<u8>, RiftError> {
        Ok(data.to_vec())
    }
}
pub struct AntiReplayWindow {
    pub highest_nonce: u64,
    pub window: u64,
}

impl AntiReplayWindow {
    pub fn check(&self, nonce: u64) -> bool {
        if nonce > self.highest_nonce { return true; }
        let diff = self.highest_nonce - nonce;
        if diff >= 64 { return false; }
        (self.window & (1u64 << diff)) == 0
    }

    pub fn record(&mut self, nonce: u64) {
        if nonce > self.highest_nonce {
            let diff = nonce - self.highest_nonce;
            if diff >= 64 {
                self.window = 1u64;
            } else {
                self.window = (self.window << diff) | 1u64;
            }
            self.highest_nonce = nonce;
        } else {
            let diff = self.highest_nonce - nonce;
            if diff < 64 {
                self.window |= 1u64 << diff;
            }
        }
    }
}

pub struct PendingSend {
    pub data: Vec<u8>,
    pub reliable: bool,
}

pub struct Connection {
    pub endpoint: SocketAddr,
    pub is_server: bool,
    pub state: ReliableConnectionState,
    pub last_seen: Instant,
    pub rx_nonce_tracker: AntiReplayWindow,
    pub tx_nonce: u64,
    pub pending_queue: VecDeque<PendingSend>,
    pub pending_bytes: usize,
    pub handshake_sent: bool,
    pub max_pending_bytes: usize,
    pub pipeline: Box<dyn NetworkPipeline + Send>,
    // ARCHITECTURAL FIX: Defer the pipeline swap until after the flush
    pub pending_pipeline_swap: Option<Box<dyn NetworkPipeline + Send>>,
}

impl Connection {
    pub fn new(
        endpoint: SocketAddr,
        is_server: bool,
        pipeline: Box<dyn NetworkPipeline + Send>
    ) -> Self {
        Self {
            endpoint,
            is_server,
            state: ReliableConnectionState::new(),
            last_seen: Instant::now(),
            rx_nonce_tracker: AntiReplayWindow { highest_nonce: 0, window: 0 },
            tx_nonce: if is_server { 1 } else { 0 },
            pending_queue: VecDeque::with_capacity(1024),
            pending_bytes: 0,
            handshake_sent: false,
            max_pending_bytes: 1024 * 1024,
            pipeline,
            pending_pipeline_swap: None,
        }
    }

    pub fn queue_pending_send(&mut self, data: &[u8], reliable: bool) {
        let size = data.len();
        if self.pending_bytes + size > self.max_pending_bytes {
            eprintln!("QueuePendingSend dropped data: capacity exceeded for {}", self.endpoint);
            return;
        }
        self.pending_queue.push_back(PendingSend { data: data.to_vec(), reliable });
        self.pending_bytes += size;
    }

    pub fn update(&mut self, now: Instant, mut send_func: impl FnMut(&[u8])) {
        let resends = self.state.process_retransmissions(now);
        for packet in resends {
            send_func(&packet);
        }

        while let Some(ps) = self.pending_queue.pop_front() {
            self.pending_bytes -= ps.data.len();

            let wire_data = if ps.reliable {
                let seq = self.state.next_outgoing_sequence;
                self.state.next_outgoing_sequence = self.state.next_outgoing_sequence.wrapping_add(1);

                let rel_hdr = ReliabilityPacketHeader {
                    sequence: seq,
                    ack: self.state.highest_received_sequence,
                    ack_bitfield: self.state.received_sequence_bitfield,
                };

                let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Reliable as u8 };

                // Handle the Result from the pipeline
                match self.pipeline.process(&ps.data, seq as u64) {
                    Ok(encrypted_payload) => {
                        let mut out = Vec::new();
                        out.extend_from_slice(gen_hdr.as_bytes());
                        out.extend_from_slice(rel_hdr.as_bytes());
                        out.extend_from_slice(&encrypted_payload);

                        self.state.unacknowledged_packets.push(riftnet_protocol::protocol::UnackedPacket {
                            sequence: seq,
                            time_sent: now,
                            data: out.clone(),
                            retries: 0,
                        });
                        out
                    }
                    Err(e) => {
                        eprintln!("Pipeline process failed: {:?}", e);
                        continue; // Drop packet on error
                    }
                }
            } else {
                let gen_size = std::mem::size_of::<GeneralPacketHeader>();
                let gen_hdr = &ps.data[..gen_size];
                let payload = &ps.data[gen_size..];

                let nonce = self.get_tx_nonce();

                match self.pipeline.process(payload, nonce) {
                    Ok(encrypted) => {
                        let mut out = Vec::with_capacity(gen_size + 8 + encrypted.len());
                        out.extend_from_slice(gen_hdr);
                        out.extend_from_slice(&nonce.to_le_bytes());
                        out.extend_from_slice(&encrypted);
                        out
                    }
                    Err(e) => {
                        eprintln!("Pipeline process failed: {:?}", e);
                        continue;
                    }
                }
            };

            send_func(&wire_data);
        }

        if let Some(new_pipeline) = self.pending_pipeline_swap.take() {
            self.pipeline = new_pipeline;
        }
    }
    pub fn get_tx_nonce(&mut self) -> u64 {
        let current = self.tx_nonce;
        self.tx_nonce += 2;
        current
    }

}