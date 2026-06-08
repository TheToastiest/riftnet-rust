// core.rs
use std::fmt;
#[derive(Debug)]
pub enum RiftError {
    NetworkError(String),
    CompressionError,
    CompressionFailed,
    DecompressionError,
    DecompressionFailed,
    EncryptionError,
    EncryptionFailed,
    DecryptionError,
    DecryptionFailed,
    CapacityExceeded,
    ThreadPanic,
}

impl fmt::Display for RiftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RiftError {}
pub type ConnId = u64;
pub type Tick = u64;

pub trait RiftTask: Send + 'static {
    type Output: Send + 'static;
    fn execute(self) -> Self::Output;
}