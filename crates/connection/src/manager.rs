use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::{info, warn, debug};
use zerocopy::FromBytes;
use riftnet_protocol::packet::{GeneralPacketHeader, ReliabilityPacketHeader, PacketType, DisconnectPacket};
use riftnet_protocol::ReliableConnectionState;
use crate::connection::Connection;
use crate::NullPipeline;

pub struct ConnectionManager {
    connections: HashMap<SocketAddr, Connection>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self { connections: HashMap::new() }
    }

    pub fn handle_packet<'a>(&mut self, addr: SocketAddr, data: &'a [u8]) -> Option<&'a [u8]> {
        let conn = self.connections.entry(addr).or_insert_with(|| {
            info!(client = %addr, "New connection established");
            Connection::new(addr, true, Box::new(NullPipeline))
        });
        conn.last_seen = Instant::now();

        if let Some(header) = GeneralPacketHeader::read_from_prefix(data) {
            match header.packet_type {
                t if t == PacketType::Reliable as u8 => {
                    let rel_size = std::mem::size_of::<ReliabilityPacketHeader>();
                    let gen_size = std::mem::size_of::<GeneralPacketHeader>();

                    if data.len() >= gen_size + rel_size {
                        if let Some(rel_hdr) = ReliabilityPacketHeader::read_from(&data[gen_size..]) {
                            let is_new = conn.state.process_incoming_header(&rel_hdr, Instant::now());

                            if is_new {
                                return Some(&data[gen_size + rel_size..]);
                            } else {
                                let sequence = rel_hdr.sequence;
                                debug!(client = %addr, seq = sequence, "Dropped duplicate/stale reliable packet");
                            }
                        }
                    }
                }
                t if t == PacketType::Input as u8 || t == PacketType::Snapshot as u8 => {
                    return Some(data);
                }
                t if t == PacketType::Disconnect as u8 => {
                    self.process_disconnect(addr, data);
                }
                _ => warn!(client = %addr, packet_type = header.packet_type, "Received unknown packet type"),
            }
        }
        None
    }

    fn process_disconnect(&mut self, addr: SocketAddr, data: &[u8]) {
        if let Some(disc) = DisconnectPacket::read_from_prefix(data) {
            let reason = disc.reason_code;
            info!(client = %addr, reason_code = reason, "Client disconnected");
        }
        self.connections.remove(&addr);
    }

    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn get_connection_state(&self, addr: &SocketAddr) -> Option<&ReliableConnectionState> {
        self.connections.get(addr).map(|c| &c.state)
    }
}