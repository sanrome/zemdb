use futures::executor::block_on;
use rimdb_core::{CryptoConcurrencyBounds, CryptoEngine, NoOpCryptoEngine, RoomId};

#[test]
fn test_noop_crypto_engine_roundtrip_with_aad() {
    block_on(async {
        let engine = NoOpCryptoEngine;
        let room_id = RoomId::new("room-e2ee-test");
        let aad = b"table_1:pk_42:col_3";
        let plaintext = b"sensitive user payload";

        let encrypted = engine.encrypt(&room_id, aad, plaintext).await.unwrap();
        assert_eq!(encrypted, plaintext);

        let decrypted = engine.decrypt(&room_id, aad, &encrypted).await.unwrap();
        assert_eq!(decrypted, plaintext);
    });
}

fn assert_crypto_concurrency_bounds<T: CryptoConcurrencyBounds>() {}

#[test]
fn test_crypto_concurrency_bounds_satisfied() {
    assert_crypto_concurrency_bounds::<NoOpCryptoEngine>();
}
