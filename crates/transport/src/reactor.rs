use std::net::SocketAddr;
use tokio::net::UdpSocket;
use riftnet_core::RiftError;

pub trait NetworkReactor: Send + Sync {
    fn poll_packets(&mut self) -> Result<Vec<(Vec<u8>, SocketAddr)>, RiftError>;
    fn send_packet(&mut self, data: &[u8], addr: SocketAddr) -> Result<(), RiftError>;
}

pub struct TokioReactor {
    socket: UdpSocket,
}

impl TokioReactor {
    pub async fn new(addr: SocketAddr) -> Result<Self, RiftError> {
        let socket = UdpSocket::bind(addr).await
            .map_err(|e| RiftError::NetworkError(e.to_string()))?;
        Ok(Self { socket })
    }
}

impl NetworkReactor for TokioReactor {
    fn poll_packets(&mut self) -> Result<Vec<(Vec<u8>, SocketAddr)>, RiftError> {
        let mut packets = Vec::new();
        let mut buf = [0u8; 2048]; // MTU-safe buffer

        loop {
            match self.socket.try_recv_from(&mut buf) {
                Ok((len, addr)) => {
                    packets.push((buf[..len].to_vec(), addr));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Socket buffer is empty, break the loop
                    break;
                }
                Err(e) => return Err(RiftError::NetworkError(e.to_string())),
            }
        }

        Ok(packets)
    }

    fn send_packet(&mut self, data: &[u8], addr: SocketAddr) -> Result<(), RiftError> {
        tokio::task::block_in_place(|| {
            let runtime = tokio::runtime::Handle::current();
            runtime.block_on(self.socket.send_to(data, addr))
                .map_err(|e| RiftError::NetworkError(e.to_string()))?;
            Ok(())
        })
    }
}