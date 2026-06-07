use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::{info, warn, debug};
use zerocopy::FromBytes;
use riftnet_protocol::packet::{GeneralPacketHeader, ReliabilityPacketHeader, PacketType, DisconnectPacket, HandshakePacket};
use riftnet_protocol::ReliableConnectionState;
use crate::connection::Connection;
use crate::pipeline::SecurePipeline;
use crate::NetworkPipeline;
use zerocopy::AsBytes;

pub struct ConnectionManager {
    connections: HashMap<SocketAddr, Connection>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self { connections: HashMap::new() }
    }

    pub fn handle_packet<'a>(&mut self, addr: SocketAddr, data: &'a [u8]) -> Option<Vec<u8>> {
        let mut is_new_connection = false;

        let conn = self.connections.entry(addr).or_insert_with(|| {
            info!(client = %addr, "New connection established");
            is_new_connection = true;
            let static_poc_key = [0x42; 32];
            Connection::new(addr, true, Box::new(SecurePipeline::new(static_poc_key)))
        });
        conn.last_seen = Instant::now();

        if is_new_connection && conn.is_server {
            let mut key = [0u8; 32];
            for (i, b) in addr.ip().to_string().bytes().enumerate().take(32) { key[i] = b; }
            let time_bytes = Instant::now().elapsed().as_nanos().to_le_bytes();
            for i in 0..16 { key[16 + i] ^= time_bytes[i]; }

            info!(client = %addr, "SERVER: Dispatching Secure Handshake Key");

            let handshake = HandshakePacket { session_key: key };
            let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Reliable as u8 };

            let mut out = Vec::new();
            out.extend_from_slice(gen_hdr.as_bytes());
            out.extend_from_slice(handshake.as_bytes());

            conn.queue_pending_send(&out, true);
            conn.handshake_sent = true;

            // Queue the swap for AFTER the flush
            conn.pending_pipeline_swap = Some(Box::new(SecurePipeline::new(key)));
        }

        let gen_hdr = GeneralPacketHeader::read_from_prefix(data)?;
        let gen_size = std::mem::size_of::<GeneralPacketHeader>();

        match gen_hdr.packet_type {
            t if t == PacketType::Reliable as u8 => {
                let rel_size = std::mem::size_of::<ReliabilityPacketHeader>();

                if data.len() >= gen_size + rel_size {
                    // Safe slice reading using zerocopy
                    if let Some(rel_hdr) = ReliabilityPacketHeader::read_from_prefix(&data[gen_size..]) {
                        // THIS UPDATES THE ACKS ON BOTH CLIENT AND SERVER
                        conn.state.process_incoming_header(&rel_hdr, Instant::now());

                        let payload_offset = gen_size + rel_size;
                        let nonce = rel_hdr.sequence as u64;

                        // Decrypt inner bytes
                        if let Some(decrypted) = conn.pipeline.inverse_process(&data[payload_offset..], nonce) {

                            // Check if the decrypted payload is the Handshake
                            if let Some(inner_hdr) = GeneralPacketHeader::read_from_prefix(&decrypted) {
                                if inner_hdr.packet_type == PacketType::Reliable as u8 {
                                    let hs_offset = std::mem::size_of::<GeneralPacketHeader>();
                                    if decrypted.len() >= hs_offset + std::mem::size_of::<HandshakePacket>() {
                                        if let Some(hs) = HandshakePacket::read_from_prefix(&decrypted[hs_offset..]) {
                                            info!(server = %addr, "CLIENT: Received Secure Handshake Key. Hot-swapping pipeline.");
                                            conn.pipeline = Box::new(SecurePipeline::new(hs.session_key));
                                            return None; // Handshake consumed, do not pass to app
                                        }
                                    }
                                }
                            }
                            return Some(decrypted);
                        }
                    }
                }
            }
            t if t == PacketType::Input as u8 || t == PacketType::Snapshot as u8 => {
                if data.len() < gen_size + 8 { return None; }
                let mut nonce_bytes = [0u8; 8];
                nonce_bytes.copy_from_slice(&data[gen_size..gen_size + 8]);
                let nonce = u64::from_le_bytes(nonce_bytes);

                if let Some(decrypted) = conn.pipeline.inverse_process(&data[gen_size + 8..], nonce) {
                    let mut full_packet = gen_hdr.as_bytes().to_vec();
                    full_packet.extend(decrypted);
                    return Some(full_packet);
                }
            }
            t if t == PacketType::Disconnect as u8 => {
                self.process_disconnect(addr, data);
            }
            _ => warn!(client = %addr, type = gen_hdr.packet_type, "Unknown packet type"),
        }
        None
    }

    fn process_disconnect(&mut self, addr: SocketAddr, data: &[u8]) {
        if let Some(disc) = DisconnectPacket::read_from_prefix(data) {
            let aligned_reason = disc.reason_code;
            info!(client = %addr, reason_code = aligned_reason, "Client disconnected");
        }
        self.connections.remove(&addr);
    }
    // --- Test & Telemetry Helpers ---

    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn get_connection_state(&self, addr: &SocketAddr) -> Option<&ReliableConnectionState> {
        self.connections.get(addr).map(|c| &c.state)
    }
    pub fn get_active_connections(&self) -> &HashMap<SocketAddr, Connection> {
        &self.connections
    }

    pub fn get_active_connections_mut(&mut self) -> &mut HashMap<SocketAddr, Connection> {
        &mut self.connections
    }

    pub fn send_to_client(&mut self, addr: SocketAddr, data: &[u8], reliable: bool, is_server: bool) {
        let conn = self.connections.entry(addr).or_insert_with(|| {
            info!(client = %addr, "New connection established");
            let static_poc_key = [0x42; 32];
            Connection::new(addr, is_server, Box::new(SecurePipeline::new(static_poc_key)))
        });
        conn.queue_pending_send(data, reliable);
    }

    pub fn set_pipeline(&mut self, addr: SocketAddr, pipeline: Box<dyn NetworkPipeline + Send>) {
        if let Some(conn) = self.connections.get_mut(&addr) {
            conn.pipeline = pipeline;
        }
    }

    pub fn flush_all(&mut self, mut send_func: impl FnMut(&[u8], SocketAddr)) {
        let now = Instant::now();
        for (addr, conn) in self.connections.iter_mut() {
            conn.update(now, |wire_bytes| {
                send_func(wire_bytes, *addr);
            });
        }
    }

    pub fn evict_stale_connections(&mut self, timeout: std::time::Duration) {
        let now = Instant::now();
        self.connections.retain(|addr, conn| {
            let keep = now.duration_since(conn.last_seen) < timeout;
            if !keep { info!(client = %addr, "Evicting stale connection (timeout)"); }
            keep
        });
    }
}