use riftnet_encryption::encryptor::Cipher;
use riftnet_compression::compressor::{Compressor, Lz4Compressor};
use riftnet_protocol::packet::{GeneralPacketHeader, PacketType};
use zerocopy::AsBytes;
use riftnet_connection::SecurePipeline;
use riftnet_connection::NetworkPipeline;
#[test]
fn test_full_packet_pipeline() {
    // 1. Setup Data & Keys
    let raw_app_data = b"RiftForged Deterministic Continuous Physics State Block - Tick 420";
    let key = [0x42; 32]; // 32-byte dummy key for ChaCha20
    let session_id = 999;
    let tick = 420;

    let cipher = Cipher::new(&key);
    let compressor = Lz4Compressor;

    // Simulate Headers (This acts as our Associated Data)
    let header = GeneralPacketHeader { packet_type: PacketType::Snapshot as u8 };
    let aad = header.as_bytes();
    let nonce = Cipher::derive_nonce(tick, session_id);

    // --- SENDER PIPELINE ---

    // A. Compress the raw data
    let mut payload = compressor.compress(raw_app_data).expect("Compression failed");
    let compressed_len = payload.len();

    // B. Encrypt the compressed data (mutates in place, appends 16-byte tag)
    cipher.encrypt(nonce, aad, &mut payload).expect("Encryption failed");

    assert_eq!(
        payload.len(),
        compressed_len + 16,
        "Encrypted payload should be compressed length + 16 byte Poly1305 tag"
    );

    // --- RECEIVER PIPELINE ---

    // C. Decrypt the payload
    let decrypted_slice = cipher.decrypt(nonce, aad, &mut payload).expect("Decryption failed");

    // D. Decompress the decrypted payload
    let recovered_data = compressor.decompress(decrypted_slice).expect("Decompression failed");

    // E. Assert total parity
    assert_eq!(
        raw_app_data.as_slice(),
        recovered_data.as_slice(),
        "Recovered application data does not match original state!"
    );
}

#[test]
fn test_pipeline_rejects_tampering() {
    let raw_app_data = b"Sensitive AI state data";
    let key = [0x42; 32];
    let cipher = Cipher::new(&key);
    let compressor = Lz4Compressor;

    let valid_header = GeneralPacketHeader { packet_type: PacketType::Snapshot as u8 };
    let nonce = Cipher::derive_nonce(1, 1);

    // Sender encrypts with valid header
    let mut payload = compressor.compress(raw_app_data).unwrap();
    cipher.encrypt(nonce, valid_header.as_bytes(), &mut payload).unwrap();

    // MITM Attack: Attacker intercepts and flips the packet type to 'Disconnect'
    let tampered_header = GeneralPacketHeader { packet_type: PacketType::Disconnect as u8 };

    // Receiver attempts to decrypt with the tampered header
    let result = cipher.decrypt(nonce, tampered_header.as_bytes(), &mut payload);

    assert!(
        result.is_err(),
        "AEAD Cipher MUST fail to decrypt if the AAD (headers) have been altered!"
    );
}
    #[test]
    fn test_pipeline_roundtrip() {
        let key = [0x42; 32];
        let pipeline = SecurePipeline::new(key);
        let data = b"test_payload";
        let nonce = 101;

        let encrypted = pipeline.process(data, nonce);
        let decrypted = pipeline.inverse_process(&encrypted, nonce).unwrap();

        assert_eq!(data.to_vec(), decrypted);
    }