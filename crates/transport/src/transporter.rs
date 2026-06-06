// crates/transport/src/transporter.rs
use crate::reactor::NetworkReactor;
use crate::interpolator::{Interpolator, Interpolatable};
use riftnet_core::RiftError;
use std::net::SocketAddr;

pub struct Transporter<T, R: NetworkReactor> {
    reactor: R,
    pub interpolator: Interpolator<T>,
}

impl<T, R: NetworkReactor> Transporter<T, R> {
    pub fn new(reactor: R, buffer_size: usize) -> Self {
        Self {
            reactor,
            interpolator: Interpolator::new(buffer_size),
        }
    }

    /// Pulls raw serialize along with their source addresses from the reactor.
    pub fn poll(&mut self) -> Result<Vec<(Vec<u8>, SocketAddr)>, RiftError> {
        self.reactor.poll_packets()
    }

    /// Forwards raw data to a specific address through the reactor.
    pub fn send(&mut self, data: &[u8], addr: SocketAddr) -> Result<(), RiftError> {
        self.reactor.send_packet(data, addr)
    }
}