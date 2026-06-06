use riftnet_connection::manager::{ConnectionManager};
use riftnet_protocol::packet::{GeneralPacketHeader, ReliabilityPacketHeader, PacketType};
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use zerocopy::AsBytes;

#[test]
fn test_connection_isolation() {
    let mut manager = ConnectionManager::new();
    let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);

    manager.handle_packet(addr1, &[0u8; 10]);
    manager.handle_packet(addr2, &[0u8; 10]);

    assert_eq!(manager.connection_count(), 2);
}

#[test]
fn test_packet_routing() {
    let mut manager = ConnectionManager::new();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    let gen_hdr = GeneralPacketHeader { packet_type: PacketType::Reliable as u8 };

    // 1. Send Sequence 1
    let rel_hdr_1 = ReliabilityPacketHeader { sequence: 1, ack: 0, ack_bitfield: 0 };
    let mut data_1 = Vec::new();
    data_1.extend_from_slice(gen_hdr.as_bytes());
    data_1.extend_from_slice(rel_hdr_1.as_bytes());

    manager.handle_packet(addr, &data_1);
    let state_before = manager.get_connection_state(&addr).unwrap().clone();

    // 2. Send Sequence 2
    let rel_hdr_2 = ReliabilityPacketHeader { sequence: 2, ack: 0, ack_bitfield: 0 };
    let mut data_2 = Vec::new();
    data_2.extend_from_slice(gen_hdr.as_bytes());
    data_2.extend_from_slice(rel_hdr_2.as_bytes());

    manager.handle_packet(addr, &data_2);
    let state_after = manager.get_connection_state(&addr).unwrap().clone();

    // The state should now be different because highest_received_sequence progressed
    assert!(state_after != state_before);
    assert_eq!(state_after.highest_received_sequence, 2);
}