use riftnet_core::core::RiftError;

#[test]
fn test_error_variants() {
    let err = RiftError::EncryptionError;
    // Perform deterministic check
    assert!(format!("{:?}", err).contains("EncryptionError"));
}