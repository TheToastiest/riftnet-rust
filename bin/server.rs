// server.rs
use riftnet_transport::{TokioReactor, NetworkReactor};
use riftnet_connection::manager::ConnectionManager;
use riftnet_protocol::packet::{GeneralPacketHeader, SnapshotHeader, InputPacket, PacketType};
use std::net::SocketAddr;
use std::collections::{HashMap, BTreeMap, HashSet, hash_map::DefaultHasher};
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

// NEW: Session state to hold our jitter buffer
struct ClientSession {
    /// BTreeMap keeps ticks sorted chronologically: Tick -> Input Bitmask
    input_buffer: BTreeMap<u64, u32>,
    /// The input applied in the previous tick (used for extrapolation)
    last_known_input: u32,
    latest_input_tick: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    let mut reactor = TokioReactor::new(addr).await?;
    let mut manager = ConnectionManager::new();
    let mut world = WorldState { positions: vec![[0.0, 0.0, 0.0]] };
    let mut current_tick: u64 = 0;
    let mut client_sessions: HashMap<SocketAddr, ClientSession> = HashMap::new();

    let mut ticker = interval(Duration::from_nanos(16_666_666));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!(address = %addr, "SERVER INITIALIZED - JITTER-BUFFERED COMMAND ENGINE");

    loop {
        ticker.tick().await;
        current_tick += 1;

        // 1. Ingest packets into jitter buffer
        if let Ok(packets) = reactor.poll_packets() {
            for (packet, sender) in packets {
                if let Some(app_data) = manager.handle_packet(sender, &packet) {
                    if let Some(gen_hdr) = GeneralPacketHeader::read_from_prefix(app_data) {
                        if gen_hdr.packet_type == PacketType::Input as u8 {
                            let offset = std::mem::size_of::<GeneralPacketHeader>();
                            if let Some(input_pkt) = InputPacket::read_from_prefix(&app_data[offset..]) {
                                let session = client_sessions.entry(sender).or_insert(ClientSession {
                                    input_buffer: BTreeMap::new(),
                                    last_known_input: 0,
                                    latest_input_tick: 0,
                                });
                                session.input_buffer.insert(input_pkt.tick, input_pkt.input_bitmask);
                                session.latest_input_tick = input_pkt.tick; // Capture latest
                            }
                        }
                    }
                }
            }
        }

        // 2. Deterministic Tick Execution
        let mut inputs_this_tick = 0;
        for (_, session) in client_sessions.iter_mut() {
            // Apply exact input or extrapolate last known
            if let Some(input) = session.input_buffer.remove(&current_tick) {
                session.last_known_input = input;
            }
            inputs_this_tick |= session.last_known_input;
            // Purge ancient data
            session.input_buffer.retain(|&t, _| t > current_tick);
        }

        world.step_physics(inputs_this_tick);
        let hash = world.calculate_hash();

        // 3. Broadcast to all active sessions
        for (addr, session) in client_sessions.iter_mut() {
            // Construct the unique header for THIS client
            let snap_hdr = SnapshotHeader {
                tick: current_tick,
                state_hash: hash,
                last_input_tick: session.latest_input_tick,
            };

            // Construct buffer for THIS client
            let mut out_buffer = [
                GeneralPacketHeader { packet_type: PacketType::Snapshot as u8 }.as_bytes(),
                snap_hdr.as_bytes()
            ].concat();

            for pos in &world.positions {
                for v in pos {
                    out_buffer.extend_from_slice(&v.to_le_bytes());
                }
            }

            let _ = reactor.send_packet(&out_buffer, *addr);
        }
    }
}