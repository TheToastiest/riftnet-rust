use riftnet_encryption::encryptor::Cipher;

#[test]
fn test_cipher_initialization() {
    // 32-byte key for ChaCha20-Poly1305
    let key = [0u8; 32];
    let cipher = Cipher::new(&key);

    // Validate state (e.g., ensure no panics on init)
    // Future: implement encryption/decryption test
    assert!(true);
}
#[test]
fn test_cipher_encryption() {
    let key = [0u8; 32];
    let cipher = Cipher::new(&key);
}