use riftnet_protocol::packet::{DataPacketHeader, ReliabilityPacketHeader};
use zerocopy::{FromBytes, AsBytes};
use riftnet_protocol::protocol::{ReliableConnectionState};
use std::time::Instant; // Add this import

#[test]
fn test_data_packet_header_serialization() {
    let wire_bytes: [u8; 4] = [0x00, 0x04, 0x00, 0x00];
    let expected_size: u32 = 1024;

    let header = DataPacketHeader::read_from(&wire_bytes)
        .expect("Failed to deserialize packet header");

    let size = header.uncompressed_size;
    assert_eq!(size, expected_size, "Header size mismatch");

    assert_eq!(std::mem::size_of::<DataPacketHeader>(), 4, "Struct padding detected");
}

#[test]
fn test_data_packet_roundtrip() {
    let original = DataPacketHeader { uncompressed_size: 2048 };

    let bytes = original.as_bytes();
    let recovered = DataPacketHeader::read_from(bytes)
        .expect("Failed to recover header from bytes");

    assert_eq!(original, recovered, "Roundtrip data mismatch");
}

#[test]
fn test_sequence_wrapping_logic() {
    assert!(ReliableConnectionState::is_sequence_more_recent(1, 65535));
    assert!(!ReliableConnectionState::is_sequence_more_recent(65535, 1));
}

#[test]
fn test_receive_window_update() {
    let mut state = ReliableConnectionState::new();

    let header = ReliabilityPacketHeader {
        sequence: 5,
        ack: 0,
        ack_bitfield: 0,
    };

    // Pass Instant::now() to satisfy the RTT-aware protocol signature
    state.process_incoming_header(&header, Instant::now());

    assert_eq!(state.highest_received_sequence, 5);
    // The bitfield should have the 0th bit set (representing sequence 5)
    assert_eq!(state.received_sequence_bitfield & 1, 1);
}