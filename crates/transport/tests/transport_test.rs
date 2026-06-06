use riftnet_transport::interpolator::{Interpolator, Snapshot, Interpolatable};
use riftnet_transport::transporter::Transporter;
use riftnet_transport::NetworkReactor;
use riftnet_core::{Tick, RiftError};
use std::collections::VecDeque;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

// 1. Existing State/Logic tests (Unchanged)
#[derive(Clone, Debug, PartialEq)]
struct GameState { pos: f32 }

impl Interpolatable for GameState {
    fn lerp(&self, other: &Self, factor: f32) -> Self {
        GameState { pos: self.pos + (other.pos - self.pos) * factor }
    }
}

#[test]
fn test_interpolation_logic() {
    let mut interp = Interpolator::new(10);
    interp.push_snapshot(Snapshot { tick: 10, state: GameState { pos: 0.0 } });
    interp.push_snapshot(Snapshot { tick: 20, state: GameState { pos: 10.0 } });

    let result = interp.interpolate(15, 0.5).expect("Interpolation failed");
    assert_eq!(result.pos, 5.0);
}

// 2. Updated MockReactor to match the new Trait signature
struct MockReactor {
    packets: VecDeque<(Vec<u8>, SocketAddr)>,
}

impl NetworkReactor for MockReactor {
    fn poll_packets(&mut self) -> Result<Vec<(Vec<u8>, SocketAddr)>, RiftError> {
        Ok(self.packets.drain(..).collect())
    }
    fn send_packet(&mut self, _data: &[u8], _addr: SocketAddr) -> Result<(), RiftError> {
        Ok(())
    }
}

// 3. Updated Transporter orchestration test
#[test]
fn test_transporter_reactor_integration() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let mock = MockReactor {
        packets: VecDeque::from(vec![(vec![0u8; 4], addr)])
    };

    let mut transporter = Transporter::<GameState, MockReactor>::new(mock, 10);

    // Verify the Transporter correctly polls the reactor and preserves the address
    let packets = transporter.poll().expect("Poll failed");
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].0.len(), 4);
    assert_eq!(packets[0].1, addr);
}