// crates/connection/tests/connection_test.rs

use riftnet_connection::manager::ConnectionManager;
use riftnet_protocol::packet::{GeneralPacketHeader, ReliabilityPacketHeader, PacketType};
use riftnet_connection::SecurePipeline;
use riftnet_connection::connection::NetworkPipeline;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use zerocopy::AsBytes;

#[test]
fn test_connection_isolation() {
    let mut manager = ConnectionManager::new();
    let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);

    // Explicitly initialize the connections to bypass the handshake trigger
    manager.send_to_client(addr1, &[], false, false);
    manager.send_to_client(addr2, &[], false, false);

    assert_eq!(manager.connection_count(), 2);
}

#[test]
fn test_secure_packet_routing() {
    let mut manager = ConnectionManager::new();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    // The default POC key used during initial connection bootstrapping
    let poc_key = [0x42; 32];

    // Initialize the receiver (is_server = false to prevent handshake auto-generation)
    // This internally sets up the connection with SecurePipeline(poc_key)
    manager.send_to_client(addr, &[], false, false);

    // Create an independent pipeline to act as the remote sender
    let sender_pipeline = SecurePipeline::new(poc_key);

    let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Reliable as u8 };
    let mock_payload = b"deterministic_test_payload";

    // ---------------------------------------------------------
    // 1. Send Sequence 1 (Valid Nonce & MAC)
    // ---------------------------------------------------------
    let seq_1 = 1;
    let rel_hdr_1 = ReliabilityPacketHeader { sequence: seq_1, ack: 0, ack_bitfield: 0 };

    // Encrypt and compress using the sequence number as the exact nonce
    let encrypted_payload_1 = sender_pipeline.process(mock_payload, seq_1 as u64)
        .expect("Sender encryption failed");

    let mut data_1 = Vec::new();
    data_1.extend_from_slice(gen_hdr.as_bytes());
    data_1.extend_from_slice(rel_hdr_1.as_bytes());
    data_1.extend_from_slice(&encrypted_payload_1);

    manager.handle_packet(addr, &data_1);

    // Assert the receiver progressed its state and accepted the authenticated packet
    let state_before = manager.get_connection_state(&addr).expect("Connection lost").clone();
    assert_eq!(state_before.highest_received_sequence, 1);

    // ---------------------------------------------------------
    // 2. Send Sequence 2 (Valid Nonce & MAC)
    // ---------------------------------------------------------
    let seq_2 = 2;
    let rel_hdr_2 = ReliabilityPacketHeader { sequence: seq_2, ack: 0, ack_bitfield: 0 };
    let encrypted_payload_2 = sender_pipeline.process(mock_payload, seq_2 as u64)
        .expect("Sender encryption failed");

    let mut data_2 = Vec::new();
    data_2.extend_from_slice(gen_hdr.as_bytes());
    data_2.extend_from_slice(rel_hdr_2.as_bytes());
    data_2.extend_from_slice(&encrypted_payload_2);

    manager.handle_packet(addr, &data_2);
    let state_after = manager.get_connection_state(&addr).expect("Connection lost").clone();

    // The state should now be different because highest_received_sequence progressed
    assert!(state_after != state_before);
    assert_eq!(state_after.highest_received_sequence, 2);
}