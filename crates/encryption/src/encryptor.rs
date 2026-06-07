// encryptor.rs
use ring::aead::{self, LessSafeKey, UnboundKey, Nonce, Aad};
use riftnet_core::RiftError;

pub struct Cipher {
    key: LessSafeKey,
}

impl Cipher {
    /// Initializes the cipher. `key_bytes` must be exactly 32 bytes for ChaCha20.
    pub fn new(key_bytes: &[u8]) -> Self {
        let unbound = UnboundKey::new(&aead::CHACHA20_POLY1305, key_bytes)
            .expect("Fatal: Invalid ChaCha20 key length. Must be 32 bytes.");
        Self { key: LessSafeKey::new(unbound) }
    }

    /// Encrypts the payload in place and appends the 16-byte Poly1305 authentication tag.
    /// `aad` (Additional Authenticated Data) should be your packet headers.
    pub fn encrypt(
        &self,
        nonce_bytes: [u8; 12],
        aad: &[u8],
        buffer: &mut Vec<u8>
    ) -> Result<(), RiftError> {
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let aad_data = Aad::from(aad);

        self.key.seal_in_place_append_tag(nonce, aad_data, buffer)
            .map_err(|_| RiftError::EncryptionError)
    }

    /// Decrypts the payload in place and verifies the Poly1305 tag against the headers (aad).
    /// If successful, returns a mutable slice to the decrypted plaintext.
    pub fn decrypt<'a>(
        &self,
        nonce_bytes: [u8; 12],
        aad: &[u8],
        buffer: &'a mut [u8]
    ) -> Result<&'a mut [u8], RiftError> {
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let aad_data = Aad::from(aad);

        self.key.open_in_place(nonce, aad_data, buffer)
            .map_err(|_| RiftError::EncryptionError)
    }

    /// Helper to deterministically generate a unique 12-byte nonce from a tick/sequence
    pub fn derive_nonce(tick: u64, session_id: u32) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[0..8].copy_from_slice(&tick.to_le_bytes());
        nonce[8..12].copy_from_slice(&session_id.to_le_bytes());
        nonce
    }
}