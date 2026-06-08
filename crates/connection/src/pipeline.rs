// crates/connection/src/pipeline.rs
use crate::connection::NetworkPipeline;
use riftnet_encryption::encryptor::Cipher;
use riftnet_compression::compressor::{Compressor, Lz4Compressor};
use riftnet_core::RiftError;

pub struct SecurePipeline {
    cipher: Cipher,
    compressor: Lz4Compressor,
}

impl SecurePipeline {
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            cipher: Cipher::new(&key),
            compressor: Lz4Compressor,
        }
    }
}

impl NetworkPipeline for SecurePipeline {
    fn process(&self, plaintext: &[u8], nonce: u64) -> Result<Vec<u8>, RiftError> {
        // 1. Compress
        let mut payload = self.compressor.compress(plaintext)
            .map_err(|_| RiftError::CompressionError)?;

        // 2. Format nonce
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());

        // 3. Encrypt
        self.cipher.encrypt(nonce_bytes, &[], &mut payload)
            .map_err(|_| RiftError::EncryptionError)?;

        Ok(payload)
    }

    fn inverse_process(&self, ciphertext: &[u8], nonce: u64) -> Result<Vec<u8>, RiftError> {
        let mut buffer = ciphertext.to_vec();
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&nonce.to_le_bytes());

        // 1. Decrypt/Authenticate
        let decrypted_slice = self.cipher.decrypt(nonce_bytes, &[], &mut buffer)
            .map_err(|_| RiftError::EncryptionError)?;

        // 2. Decompress
        self.compressor.decompress(decrypted_slice)
            .map_err(|_| RiftError::CompressionError)
    }
}