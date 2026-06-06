use riftnet_transport::{TokioReactor, NetworkReactor};
use riftnet_protocol::{HistoryBuffer, FrameRecord};
use riftnet_protocol::packet::{GeneralPacketHeader, SnapshotHeader, InputPacket, PacketType};
use std::net::SocketAddr;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::time::Instant;
use std::hash::Hasher;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{info, warn, debug, Level};
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

    pub fn step_physics(&mut self, _input: u32) {
        self.positions[0][0] += 0.01;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false) // Keeps console output clean
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let server_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let client_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let mut reactor = TokioReactor::new(client_addr).await?;

    let mut world = WorldState { positions: vec![[0.0, 0.0, 0.0]] };
    let mut history = HistoryBuffer::<WorldState, u32>::new(128);
    let mut input_dispatch_times: HashMap<u64, Instant> = HashMap::new();

    let mut predicted_tick: u64 = 0;
    let mut last_server_tick: u64 = 0;
    let mut rollbacks_this_sec: u32 = 0;
    let mut current_rtt_ms: u128 = 0;

    info!(target_server = %server_addr, "CLIENT INITIALIZED - WAITING FOR SERVER SYNC");

    let mut synced = false;
    while !synced {
        if let Ok(packets) = reactor.poll_packets() {
            for (packet, _) in packets {
                let offset = std::mem::size_of::<GeneralPacketHeader>();
                if packet.len() >= offset + std::mem::size_of::<SnapshotHeader>() {
                    if let Some(server_snap) = SnapshotHeader::read_from_prefix(&packet[offset..]) {
                        let state_offset = offset + std::mem::size_of::<SnapshotHeader>();
                        if packet.len() >= state_offset + 12 {
                            let mut new_pos = [0.0f32; 3];
                            for i in 0..3 {
                                let start = state_offset + (i * 4);
                                let chunk = &packet[start..start + 4];
                                new_pos[i] = f32::from_le_bytes(chunk.try_into().unwrap());
                            }

                            world.positions = vec![new_pos];
                            last_server_tick = server_snap.tick;
                            predicted_tick = last_server_tick + 3;

                            info!(server_tick = last_server_tick, start_tick = predicted_tick, "SYNC ACQUIRED");
                            synced = true;
                            break;
                        }
                    }
                }
            }
        }

        if !synced {
            let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Input as u8 };
            let input_pkt = InputPacket { tick: 0, input_bitmask: 0 };
            let mut out_buffer = Vec::new();
            out_buffer.extend_from_slice(gen_hdr.as_bytes());
            out_buffer.extend_from_slice(input_pkt.as_bytes());
            let _ = reactor.send_packet(&out_buffer, server_addr);
            tokio::time::sleep(Duration::from_millis(16)).await;
        }
    }

    info!("ENTERING DETERMINISTIC PREDICTIVE LOOP");

    let mut ticker = interval(Duration::from_nanos(16_666_666));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        predicted_tick += 1;

        if let Ok(packets) = reactor.poll_packets() {
            for (packet, _) in packets {
                let offset = std::mem::size_of::<GeneralPacketHeader>();
                if packet.len() >= offset + std::mem::size_of::<SnapshotHeader>() {
                    if let Some(server_snap) = SnapshotHeader::read_from_prefix(&packet[offset..]) {

                        let snap_tick = server_snap.tick;
                        let snap_hash = server_snap.state_hash;
                        last_server_tick = snap_tick;

                        // Calculate Loop RTT based on when this tick was dispatched
                        if let Some(dispatch_time) = input_dispatch_times.remove(&snap_tick) {
                            current_rtt_ms = dispatch_time.elapsed().as_millis();
                        }

                        if let Some(local_record) = history.get(snap_tick) {
                            if local_record.state_hash != snap_hash {
                                rollbacks_this_sec += 1;
                                debug!(tick = snap_tick, local_hash = local_record.state_hash, server_hash = snap_hash, "DESYNC DETECTED");

                                let state_offset = offset + std::mem::size_of::<SnapshotHeader>();
                                if packet.len() >= state_offset + 12 {
                                    let mut new_pos = [0.0f32; 3];
                                    for i in 0..3 {
                                        let start = state_offset + (i * 4);
                                        let chunk = &packet[start..start + 4];
                                        new_pos[i] = f32::from_le_bytes(chunk.try_into().unwrap());
                                    }
                                    world.positions = vec![new_pos];

                                    history.insert(FrameRecord {
                                        tick: snap_tick,
                                        state: world.clone(),
                                        state_hash: snap_hash,
                                        input: local_record.input,
                                    });
                                }

                                for t in (snap_tick + 1)..=predicted_tick {
                                    if let Some(record) = history.get(t) {
                                        let historical_input = record.input;
                                        world.step_physics(historical_input);

                                        history.insert(FrameRecord {
                                            tick: t,
                                            state: world.clone(),
                                            state_hash: world.calculate_hash(),
                                            input: historical_input,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let current_input = 0x01;
        world.step_physics(current_input);

        let current_hash = world.calculate_hash();
        history.insert(FrameRecord {
            tick: predicted_tick,
            state: world.clone(),
            state_hash: current_hash,
            input: current_input,
        });

        let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Input as u8 };
        let input_pkt = InputPacket { tick: predicted_tick, input_bitmask: current_input };

        let mut out_buffer = Vec::new();
        out_buffer.extend_from_slice(gen_hdr.as_bytes());
        out_buffer.extend_from_slice(input_pkt.as_bytes());

        let _ = reactor.send_packet(&out_buffer, server_addr);

        // Track dispatch time to calculate RTT when snapshot arrives
        input_dispatch_times.insert(predicted_tick, Instant::now());

        // Memory cleanup: Prevent memory leak from dropped packets
        if input_dispatch_times.len() > 128 {
            input_dispatch_times.retain(|&k, _| k >= last_server_tick);
        }

        if predicted_tick % 60 == 0 {
            let offset = predicted_tick.saturating_sub(last_server_tick);
            info!(
                pred_tick = predicted_tick,
                srv_tick = last_server_tick,
                offset = offset,
                hash = current_hash,
                rollbacks = rollbacks_this_sec,
                rtt_ms = current_rtt_ms,
                "Telemetry Pulse"
            );
            rollbacks_this_sec = 0;
        }
    }
}