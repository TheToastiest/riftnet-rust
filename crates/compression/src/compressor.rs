use riftnet_core::RiftError;

pub trait Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, RiftError>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, RiftError>;
}

pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, RiftError> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, RiftError> {
        lz4_flex::decompress_size_prepended(data)
            .map_err(|_| RiftError::CompressionError)
    }
}