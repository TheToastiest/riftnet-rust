use riftnet_transport::{TokioReactor, NetworkReactor};
use riftnet_connection::manager::ConnectionManager;
use riftnet_protocol::packet::{GeneralPacketHeader, SnapshotHeader, InputPacket, PacketType};
use std::net::SocketAddr;
use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::Hasher;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use zerocopy::{FromBytes, AsBytes};

#[derive(Clone, Debug)]
pub struct WorldState {
    pub positions: Vec<[f32; 3]>,
}

impl WorldState {
    pub fn calculate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for pos in &self.positions {
            for v in pos { hasher.write_u32(v.to_bits()); }
        }
        hasher.finish()
    }

    pub fn step_physics(&mut self, input: u32) {
        if (input & 0x01) != 0 {
            self.positions[0][0] += 0.01;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let mut reactor = TokioReactor::new(addr).await?;
    let mut manager = ConnectionManager::new();

    let mut world = WorldState { positions: vec![[0.0, 0.0, 0.0]] };
    let mut current_tick: u64 = 0;
    let mut known_clients: HashSet<SocketAddr> = HashSet::new();

    let mut ticker = interval(Duration::from_nanos(16_666_666));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!(address = %addr, "SERVER INITIALIZED - DETERMINISTIC 60HZ TICK");

    loop {
        ticker.tick().await;
        current_tick += 1;
        let mut packets_received_this_tick = 0;
        let mut inputs_this_tick = 0;

        if let Ok(packets) = reactor.poll_packets() {
            for (packet, sender) in packets {
                known_clients.insert(sender);
                packets_received_this_tick += 1;

                if let Some(app_data) = manager.handle_packet(sender, &packet) {
                    if let Some(gen_hdr) = GeneralPacketHeader::read_from_prefix(app_data) {
                        if gen_hdr.packet_type == PacketType::Input as u8 {
                            let offset = std::mem::size_of::<GeneralPacketHeader>();
                            if let Some(input_pkt) = InputPacket::read_from_prefix(&app_data[offset..]) {
                                inputs_this_tick |= input_pkt.input_bitmask;
                            }
                        }
                    }
                }
            }
        }

        world.step_physics(inputs_this_tick);
        let hash = world.calculate_hash();

        let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Snapshot as u8 };
        let snap_hdr = SnapshotHeader { tick: current_tick, state_hash: hash };

        let mut out_buffer = Vec::new();
        out_buffer.extend_from_slice(gen_hdr.as_bytes());
        out_buffer.extend_from_slice(snap_hdr.as_bytes());

        for pos in &world.positions {
            for v in pos {
                out_buffer.extend_from_slice(&v.to_le_bytes());
            }
        }

        for client_addr in &known_clients {
            let _ = reactor.send_packet(&out_buffer, *client_addr);
        }

        if current_tick % 60 == 0 {
            info!(
                tick = current_tick,
                clients = known_clients.len(),
                hash = hash,
                pkts_this_tick = packets_received_this_tick,
                "Telemetry Pulse"
            );
        }
    }
}