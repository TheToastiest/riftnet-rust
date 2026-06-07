// crates/connection/src/pipeline.rs
use crate::connection::NetworkPipeline;
use riftnet_encryption::encryptor::Cipher;
use riftnet_compression::compressor::{Compressor, Lz4Compressor};
use tracing::error;

pub struct SecurePipeline {
    cipher: Cipher,
    compressor: Lz4Compressor,
}

impl SecurePipeline {
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            // Instantiate the formal architectural abstractions
            cipher: Cipher::new(&key),
            compressor: Lz4Compressor,
        }
    }
}

impl NetworkPipeline for SecurePipeline {
    fn process(&self, plaintext: &[u8], nonce: u64) -> Vec<u8> {
        // 1. Compress the plaintext via abstraction
        let mut payload = match self.compressor.compress(plaintext) {
            Ok(data) => data,
            Err(e) => {
                error!("Pipeline compression failed: {}", e);
                return Vec::new();
            }
        };

        // 2. Format the 12-byte ChaCha20 nonce deterministically
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());

        // 3. Encrypt in place, appending the Poly1305 tag
        if let Err(e) = self.cipher.encrypt(nonce_bytes, &[], &mut payload) {
            error!("Pipeline encryption failed: {}", e);
            return Vec::new();
        }

        payload
    }

    fn inverse_process(&self, ciphertext: &[u8], nonce: u64) -> Option<Vec<u8>> {
        // Clone into a mutable buffer for in-place decryption
        let mut buffer = ciphertext.to_vec();

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());

        // 1. Authenticate and Decrypt via abstraction
        // Note: Silent drop on Err() to prevent log spam during potential tampering/DDoS attacks
        let decrypted_slice = match self.cipher.decrypt(nonce_bytes, &[], &mut buffer) {
            Ok(slice) => slice,
            Err(_) => return None,
        };

        // 2. Decompress
        self.compressor.decompress(decrypted_slice).ok()
    }
}