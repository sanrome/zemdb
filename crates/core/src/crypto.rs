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

#[cfg(not(target_arch = "wasm32"))]
pub trait CryptoConcurrencyBounds: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> CryptoConcurrencyBounds for T {}

#[cfg(target_arch = "wasm32")]
pub trait CryptoConcurrencyBounds {}
#[cfg(target_arch = "wasm32")]
impl<T> CryptoConcurrencyBounds for T {}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CryptoEngine: CryptoConcurrencyBounds {
    async fn encrypt(
        &self,
        room_id: &RoomId,
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError>;
    async fn decrypt(
        &self,
        room_id: &RoomId,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, CryptoError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoOpCryptoEngine;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CryptoEngine for NoOpCryptoEngine {
    async fn encrypt(
        &self,
        _room_id: &RoomId,
        _aad: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        Ok(plaintext.to_vec())
    }
    async fn decrypt(
        &self,
        _room_id: &RoomId,
        _aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        Ok(ciphertext.to_vec())
    }
}
