// crates/core/src/lib.rs

pub mod core;
pub mod threading;

// EXPORT these so other crates can see them at the root
pub use crate::core::{RiftError, Tick, ConnId, RiftTask};
pub use crate::threading::TaskThreadPool;