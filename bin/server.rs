// riftnet-rust\bin\server.rs

use riftnet_transport::{TokioReactor, Transporter}; // Removed unused NetworkReactor
use riftnet_connection::manager::ConnectionManager;
use riftnet_protocol::packet::{GeneralPacketHeader, SnapshotHeader, InputPacket, PacketType};
use riftnet_core::threading::TaskThreadPool;
use riftnet_transport::interpolator::{Interpolatable, Snapshot};
use std::net::SocketAddr;
use std::collections::{HashMap, BTreeMap, hash_map::DefaultHasher};
// Removed unused Instant
use std::hash::Hasher;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{info, debug, Level, warn};
use tracing_subscriber::FmtSubscriber;
use zerocopy::{FromBytes, AsBytes};
use riftnet_core::fixed_vec3::FixedVec3;

#[derive(Clone, Debug)]
pub struct WorldState {
    pub positions: Vec<FixedVec3>,
}

impl WorldState {
    pub fn calculate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for pos in &self.positions {
            hasher.write_i32(pos.x);
            hasher.write_i32(pos.y);
            hasher.write_i32(pos.z);
        }
        hasher.finish()
    }

    pub fn step_physics(&mut self, input: u32) {
        // Enforcing Left-Handed Y-up coordinates: X=Right, Y=Up, Z=Forward
        // Using FixedVec3::SCALE (1000) means 10 = 10mm or 0.01 units
        if (input & 0x01) != 0 {
            self.positions[0].x += 10; // Moving Positive X deterministically
        }
    }
}

// Architectural Wiring: Lerp the integers for the renderer without bleeding
// floats back into the authoritative state buffer.
impl Interpolatable for WorldState {
    fn lerp(&self, other: &Self, factor: f32) -> Self {
        let mut new_pos = Vec::with_capacity(self.positions.len());
        for (a, b) in self.positions.iter().zip(other.positions.iter()) {
            new_pos.push(FixedVec3 {
                x: a.x + ((b.x - a.x) as f32 * factor).round() as i32,
                y: a.y + ((b.y - a.y) as f32 * factor).round() as i32,
                z: a.z + ((b.z - a.z) as f32 * factor).round() as i32,
            });
        }
        Self { positions: new_pos }
    }
}

struct ClientSession {
    input_buffer: BTreeMap<u64, u32>,
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

    // 1. Initialize the Core Infrastructure
    let reactor = TokioReactor::new(addr).await?;
    let mut transporter = Transporter::<WorldState, TokioReactor>::new(reactor, 128);
    let mut manager = ConnectionManager::new();

    // Pre-warm the thread pool for authoritative simulation tasks
    let _thread_pool = TaskThreadPool::new(4);

    let mut world = WorldState { positions: vec![FixedVec3 { x: 0, y: 0, z: 0 }] };
    let mut current_tick: u64 = 0;
    let mut client_sessions: HashMap<SocketAddr, ClientSession> = HashMap::new();

    let mut ticker = interval(Duration::from_nanos(16_666_666));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!(address = %addr, "SERVER INITIALIZED - JITTER-BUFFERED COMMAND ENGINE");

    loop {
        ticker.tick().await;
        current_tick += 1;

        // 2. Poll the abstracted Transporter
        if let Ok(packets) = transporter.poll() {
            for (packet, sender) in packets {
                if let Some(app_data) = manager.handle_packet(sender, &packet) {
                    if let Some(gen_hdr) = GeneralPacketHeader::read_from_prefix(&app_data) {
                        if gen_hdr.packet_type == PacketType::Input as u8 {
                            let offset = std::mem::size_of::<GeneralPacketHeader>();

                            // HARDENING: Check bounds before assuming packet size
                            if app_data.len() >= offset + std::mem::size_of::<InputPacket>() {
                                #[repr(C, align(8))]
                                struct AlignedBuf([u8; 32]);
                                let mut buf = AlignedBuf([0; 32]);
                                let pkt_size = std::mem::size_of::<InputPacket>();

                                buf.0[..pkt_size].copy_from_slice(&app_data[offset..offset + pkt_size]);

                                if let Some(input_pkt) = InputPacket::read_from_prefix(&buf.0[..pkt_size]) {
                                    let session = client_sessions.entry(sender).or_insert(ClientSession {
                                        input_buffer: std::collections::BTreeMap::new(),
                                        last_known_input: 0,
                                        latest_input_tick: 0,
                                    });
                                    session.input_buffer.insert(input_pkt.tick, input_pkt.input_bitmask);
                                    session.latest_input_tick = input_pkt.tick;
                                } else {
                                    warn!("SERVER: zerocopy failed to parse InputPacket");
                                }
                            } else {
                                warn!("SERVER: Dropped undersized input packet");
                            }
                        }
                    }
                }
            }
        }

        for (addr, _conn_state) in manager.get_active_connections() {
            client_sessions.entry(*addr).or_insert(ClientSession {
                input_buffer: BTreeMap::new(),
                last_known_input: 0,
                latest_input_tick: 0,
            });
        }

        // 3. Deterministic Tick Execution
        let mut inputs_this_tick = 0;
        for (_, session) in client_sessions.iter_mut() {
            if let Some(input) = session.input_buffer.remove(&current_tick) {
                session.last_known_input = input;
            }
            inputs_this_tick |= session.last_known_input;
            session.input_buffer.retain(|&t, _| t > current_tick);
        }

        world.step_physics(inputs_this_tick);
        let hash = world.calculate_hash();

        // 4. Record state to the server's internal Interpolator history
        transporter.interpolator.push_snapshot(Snapshot {
            tick: current_tick,
            state: world.clone(),
        });

        // 5. Queue Broadcasts
        for (addr, session) in client_sessions.iter_mut() {
            if let Some(_conn) = manager.get_active_connections().get(addr) {
                let snap_hdr = SnapshotHeader {
                    tick: current_tick,
                    state_hash: hash,
                    last_input_tick: session.latest_input_tick,
                };

                let mut out_buffer = [
                    GeneralPacketHeader { packet_type: PacketType::Snapshot as u8 }.as_bytes(),
                    snap_hdr.as_bytes()
                ].concat();

                for pos in &world.positions {
                    out_buffer.extend_from_slice(&pos.x.to_le_bytes());
                    out_buffer.extend_from_slice(&pos.y.to_le_bytes());
                    out_buffer.extend_from_slice(&pos.z.to_le_bytes());
                }

                manager.send_to_client(*addr, &out_buffer, false, true);
            }
        }

        // 6. Memory Cleanup & Strict Eviction
        let eviction_timeout = Duration::from_secs(5);
        manager.evict_stale_connections(eviction_timeout);

        let active_addrs: std::collections::HashSet<_> = manager.get_active_connections().keys().copied().collect();
        client_sessions.retain(|addr, _| {
            let is_active = active_addrs.contains(addr);
            if !is_active {
                info!(client = %addr, "SERVER: Purging session data for evicted connection.");
            }
            is_active
        });

        manager.flush_all(|wire_bytes, target_addr| {
            debug!(bytes = wire_bytes.len(), target = %target_addr, "SERVER: Flushing packet to socket");
            let _ = transporter.send(wire_bytes, target_addr);
        });

        // 7. Server Telemetry Pulse
        if current_tick % 60 == 0 {
            let active_clients = client_sessions.len();
            let total_buffered_inputs: usize = client_sessions.values().map(|s| s.input_buffer.len()).sum();

            info!(
                srv_tick = current_tick,
                hash = hash,
                clients = active_clients,
                buffered_inputs = total_buffered_inputs,
                "Server Telemetry Pulse"
            );
        }
    }
}