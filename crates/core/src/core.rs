use std::fmt;
#[derive(Debug)]
pub enum RiftError {
    NetworkError(String),
    EncryptionError,
    CompressionError,
}

// 1. Required for all Error types
impl fmt::Display for RiftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// 2. This satisfies the trait bound that the '?' operator requires
impl std::error::Error for RiftError {}
pub type ConnId = u64;
pub type Tick = u64;