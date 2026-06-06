use riftnet_compression::compressor::{Compressor, Lz4Compressor};

#[test]
fn test_lz4_roundtrip_determinism() {
    let compressor = Lz4Compressor;
    let original = b"RiftNet Deterministic Data Stream".to_vec();

    let compressed = compressor.compress(&original).expect("Compression failed");
    let decompressed = compressor.decompress(&compressed).expect("Decompression failed");

    assert_eq!(original, decompressed, "Decompressed data does not match original");
}