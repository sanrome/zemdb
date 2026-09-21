use crate::id::RoomId;
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("Encryption failed: {0}")]
    Encryption(String),
    #[error("Decryption failed: {0}")]
    Decryption(String),
    #[error("Key derivation error: {0}")]
    KeyDerivation(String),
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CryptoEngine: Send + Sync {
    async fn encrypt(&self, room_id: &RoomId, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError>;
    async fn decrypt(&self, room_id: &RoomId, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoOpCryptoEngine;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CryptoEngine for NoOpCryptoEngine {
    async fn encrypt(&self, _room_id: &RoomId, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(plaintext.to_vec())
    }
    async fn decrypt(&self, _room_id: &RoomId, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(ciphertext.to_vec())
    }
}
