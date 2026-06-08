use riftnet_transport::{TokioReactor, Transporter};
use riftnet_protocol::{HistoryBuffer, FrameRecord};
use riftnet_protocol::packet::{GeneralPacketHeader, SnapshotHeader, InputPacket, PacketType};
use riftnet_connection::manager::ConnectionManager;
use riftnet_core::threading::TaskThreadPool;
use riftnet_transport::interpolator::{Interpolatable, Snapshot};
use std::net::SocketAddr;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::time::Instant;
use std::hash::Hasher;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{info};
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
        if (input & 0x01) != 0 {
            self.positions[0].x += 10;
        }
    }
}

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder().with_max_level(tracing::Level::INFO).with_target(false).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    let server_addr: SocketAddr = "155.138.129.238:8080".parse()?;
    let client_addr: SocketAddr = "0.0.0.0:0".parse()?;
    let mut last_ema: u128 = 0;
    let reactor = TokioReactor::new(client_addr).await?;
    let mut transporter = Transporter::new(reactor, 128);
    let mut manager = ConnectionManager::new();
    let _thread_pool = TaskThreadPool::new(4);

    let mut world = WorldState { positions: vec![FixedVec3 { x: 0, y: 0, z: 0 }] };
    let mut history = HistoryBuffer::<WorldState, u32>::new(128);
    let mut input_dispatch_times: HashMap<u64, Instant> = HashMap::new();

    let mut predicted_tick: u64 = 0;
    let mut last_server_tick: u64 = 0;
    let mut rollbacks_this_sec: u32 = 0;
    let current_rtt_ms: u128 = 0;
    let mut last_seen_packet_time = Instant::now();

    info!(target_server = %server_addr, "CLIENT INITIALIZED - AWAITING SYNC");

    let mut synced = false;
    while !synced {
        if let Ok(packets) = transporter.poll() {
            if !packets.is_empty() { last_seen_packet_time = Instant::now(); }
            for (packet, sender) in packets {
                // Let the manager invisibly handle the Handshake and ACK updates
                if let Some(app_data) = manager.handle_packet(sender, &packet) {
                    if let Some(gen_hdr) = GeneralPacketHeader::read_from_prefix(&app_data) {
                        if gen_hdr.packet_type == PacketType::Snapshot as u8 {
                            let state_offset = std::mem::size_of::<GeneralPacketHeader>() + std::mem::size_of::<SnapshotHeader>();
                            if let Some(snap) = SnapshotHeader::read_from_prefix(&app_data[std::mem::size_of::<GeneralPacketHeader>()..state_offset]) {
                                let new_pos = FixedVec3 {
                                    x: i32::from_le_bytes(app_data[state_offset..state_offset + 4].try_into().unwrap()),
                                    y: i32::from_le_bytes(app_data[state_offset + 4..state_offset + 8].try_into().unwrap()),
                                    z: i32::from_le_bytes(app_data[state_offset + 8..state_offset + 12].try_into().unwrap()),
                                };
                                world.positions = vec![new_pos];
                                last_server_tick = snap.tick;
                                predicted_tick = last_server_tick + 3;
                                transporter.interpolator.push_snapshot(Snapshot { tick: last_server_tick, state: world.clone() });
                                info!("SYNC ACQUIRED");
                                synced = true;
                            }
                        }
                    }
                }
            }
        }

        if !synced && Instant::now().duration_since(last_seen_packet_time) > Duration::from_secs(1) {
            let out_buffer = [GeneralPacketHeader { packet_type: PacketType::Input as u8 }.as_bytes(), InputPacket { tick: 0, input_bitmask: 0 }.as_bytes()].concat();
            manager.send_to_client(server_addr, &out_buffer, true, false);
            manager.flush_all(|wire_bytes, addr| { let _ = transporter.send(wire_bytes, addr); });
            last_seen_packet_time = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(16)).await;
    }

    let mut ticker = interval(Duration::from_nanos(16_666_666));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        if let Ok(packets) = transporter.poll() {
            for (packet, sender) in packets {
                if let Some(app_data) = manager.handle_packet(sender, &packet) {
                    if let Some(gen_hdr) = GeneralPacketHeader::read_from_prefix(&app_data) {
                        if gen_hdr.packet_type == PacketType::Snapshot as u8 {
                            let state_offset = std::mem::size_of::<GeneralPacketHeader>() + std::mem::size_of::<SnapshotHeader>();
                            if let Some(snap) = SnapshotHeader::read_from_prefix(&app_data[std::mem::size_of::<GeneralPacketHeader>()..state_offset]) {
                                last_server_tick = snap.tick;
                                let new_pos = FixedVec3 {
                                    x: i32::from_le_bytes(app_data[state_offset..state_offset + 4].try_into().unwrap()),
                                    y: i32::from_le_bytes(app_data[state_offset + 4..state_offset + 8].try_into().unwrap()),
                                    z: i32::from_le_bytes(app_data[state_offset + 8..state_offset + 12].try_into().unwrap()),
                                };
                                transporter.interpolator.push_snapshot(Snapshot { tick: snap.tick, state: WorldState { positions: vec![new_pos] } });
                                if let Some(local) = history.get(snap.tick) {
                                    if local.state_hash != snap.state_hash {
                                        rollbacks_this_sec += 1;
                                        world.positions = vec![new_pos];
                                        predicted_tick = snap.tick;
                                        while predicted_tick < snap.tick + 3 {
                                            predicted_tick += 1;
                                            world.step_physics(history.get(predicted_tick).map(|r| r.input).unwrap_or(0));
                                            history.insert(FrameRecord { tick: predicted_tick, state: world.clone(), state_hash: world.calculate_hash(), input: 0x01 });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let ping_ticks = (current_rtt_ms / 2) as u64 / 16;
        let target_tick = last_server_tick + ping_ticks + 3;
        let steps_this_loop = if predicted_tick < target_tick { 2 } else if predicted_tick > target_tick + 2 { 0 } else { 1 };

        for _ in 0..steps_this_loop {
            predicted_tick += 1;
            world.step_physics(0x01);
            history.insert(FrameRecord { tick: predicted_tick, state: world.clone(), state_hash: world.calculate_hash(), input: 0x01 });
            let out_buffer = [GeneralPacketHeader { packet_type: PacketType::Input as u8 }.as_bytes(), InputPacket { tick: predicted_tick, input_bitmask: 0x01 }.as_bytes()].concat();
            manager.send_to_client(server_addr, &out_buffer, false, false);
            input_dispatch_times.insert(predicted_tick, Instant::now());
        }

        if input_dispatch_times.len() > 128 {
            input_dispatch_times.retain(|&k, _| k >= last_server_tick);
        }

        manager.flush_all(|bytes, addr| { let _ = transporter.send(bytes, addr); });

        if steps_this_loop > 0 && predicted_tick % 60 == 0 {
            let offset = predicted_tick.saturating_sub(last_server_tick);

            // Apply EMA
            let smoothed_rtt = if current_rtt_ms > 0 {
                last_ema = (current_rtt_ms as f32 * 0.1 + last_ema as f32 * 0.9) as u128;
                last_ema
            } else {
                current_rtt_ms
            };

            info!(
                pred_tick = predicted_tick,
                srv_tick = last_server_tick,
                offset = offset,
                hash = world.calculate_hash(),
                rollbacks = rollbacks_this_sec,
                rtt_avg = smoothed_rtt,

                "Telemetry Pulse"
            );
            rollbacks_this_sec = 0;
        }
    }
}