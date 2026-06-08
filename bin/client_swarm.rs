// // client_swarm.rs
// use riftnet_transport::{TokioReactor, NetworkReactor};
// use riftnet_protocol::{HistoryBuffer, FrameRecord};
// use riftnet_protocol::packet::{GeneralPacketHeader, SnapshotHeader, InputPacket, PacketType};
// use std::net::SocketAddr;
// use std::collections::{HashMap, hash_map::DefaultHasher};
// use std::time::Instant;
// use std::hash::Hasher;
// use tokio::time::{interval, Duration, MissedTickBehavior};
// use tracing::{info, warn, debug, Level};
// use tracing_subscriber::FmtSubscriber;
// use zerocopy::{FromBytes, AsBytes};
//
// #[derive(Clone, Debug)]
// pub struct WorldState {
//     pub positions: Vec<[f32; 3]>,
// }
//
// impl WorldState {
//     pub fn calculate_hash(&self) -> u64 {
//         let mut hasher = DefaultHasher::new();
//         for pos in &self.positions {
//             for v in pos { hasher.write_u32(v.to_bits()); }
//         }
//         hasher.finish()
//     }
//
//     pub fn step_physics(&mut self, input: u32) {
//         if (input & 0x01) != 0 { self.positions[0][0] += 0.01; }
//     }
// }
// pub struct SwarmClient {
//     pub world: WorldState,
//     pub history: HistoryBuffer<WorldState, u32>,
//     pub input_dispatch_times: HashMap<u64, Instant>,
//     pub predicted_tick: u64,
//     pub last_server_tick: u64,
//     pub current_rtt_ms: u128,
// }
//
// impl SwarmClient {
//     pub fn new() -> Self {
//         Self {
//             world: WorldState { positions: vec![[0.0, 0.0, 0.0]] },
//             history: HistoryBuffer::new(128),
//             input_dispatch_times: HashMap::new(),
//             predicted_tick: 0,
//             last_server_tick: 0,
//             current_rtt_ms: 0,
//         }
//     }
//
//     // You should move the logic from your loop into these methods:
//     pub fn process_packet(&mut self, packet: Vec<u8>) {
//         let offset = std::mem::size_of::<GeneralPacketHeader>();
//         if packet.len() < offset + std::mem::size_of::<SnapshotHeader>() { return; }
//
//         if let Some(snap) = SnapshotHeader::read_from_prefix(&packet[offset..]) {
//             self.last_server_tick = snap.tick;
//
//             // RTT Calculation
//             let last_input_tick = snap.last_input_tick;
//             if let Some(dispatch_time) = self.input_dispatch_times.remove(&last_input_tick) {
//                 self.current_rtt_ms = dispatch_time.elapsed().as_millis();
//             }
//
//             // Rollback/Correction Logic
//             if let Some(local) = self.history.get(snap.tick) {
//                 if local.state_hash != snap.state_hash {
//                     // Dynamic Padding
//                     let ping_ms = (self.current_rtt_ms / 2) as u64;
//                     let padding = std::cmp::max((ping_ms / 16) + 1, 2);
//
//                     self.predicted_tick = snap.tick + padding;
//                     // Replay inputs from history here if needed
//                 }
//             }
//         }
//     }
//     pub fn prepare_packet(&self) -> Vec<u8> {
//         let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Input as u8 };
//         let input_pkt = InputPacket {
//             tick: self.predicted_tick,
//             input_bitmask: 0x01
//         };
//
//         let mut out_buffer = Vec::new();
//         out_buffer.extend_from_slice(gen_hdr.as_bytes());
//         out_buffer.extend_from_slice(input_pkt.as_bytes());
//         out_buffer
//     }
//     pub fn tick(&mut self, gain: f32) {
//         // --- PI CONTROL LOGIC ---
//         let error = self.last_server_tick as i64 - self.predicted_tick as i64;
//         let tick_acceleration = (error as f32) * gain;
//         let step = (1.0 + tick_acceleration).round() as u64;
//         self.predicted_tick += step;
//         // -------------------------
//
//         // Physics Prediction
//         let current_input = 0x01;
//         self.world.step_physics(current_input);
//
//         // Store in History
//         let current_hash = self.world.calculate_hash();
//         self.history.insert(FrameRecord {
//             tick: self.predicted_tick,
//             state: self.world.clone(),
//             state_hash: current_hash,
//             input: current_input,
//         });
//
//         // Track dispatch time for the packet that will be sent after this function
//         self.input_dispatch_times.insert(self.predicted_tick, Instant::now());
//     }
// }
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let server_addr: SocketAddr = "155.138.129.238:8080".parse()?; // Ensure this matches your server
//     let mut reactor = TokioReactor::new("0.0.0.0:0".parse()?).await?;
//     let mut swarm: HashMap<SocketAddr, SwarmClient> = HashMap::new();
//     let mut ticker = interval(Duration::from_nanos(16_666_666));
//     let gain: f32 = 0.05;
//
//     // ADD THIS: Explicitly seed the server address so the first packet is sent
//     let mut discovered_server = false;
//
//     loop {
//         ticker.tick().await;
//
//         // 1. Send Handshake if we haven't reached the server yet
//         if !discovered_server {
//             let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Input as u8 };
//             let input_pkt = InputPacket { tick: 0, input_bitmask: 0 };
//             let out_buffer = [gen_hdr.as_bytes(), input_pkt.as_bytes()].concat();
//             let _ = reactor.send_packet(&out_buffer, server_addr);
//         }
//
//         // 2. Ingest packets
//         if let Ok(packets) = reactor.poll_packets() {
//             for (packet, sender) in packets {
//                 discovered_server = true; // We found them!
//                 let client = swarm.entry(sender).or_insert_with(|| SwarmClient::new());
//                 client.process_packet(packet);
//             }
//         }
//
//         // 3. Tick/Update existing clients
//         for (addr, client) in swarm.iter_mut() {
//             if client.last_server_tick != 0 {
//                 client.tick(gain);
//                 let out_buffer = client.prepare_packet();
//                 let _ = reactor.send_packet(&out_buffer, *addr);
//             }
//         }
//     }
// }