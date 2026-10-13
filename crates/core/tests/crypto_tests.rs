use futures::executor::block_on;
use zemdb_core::{CryptoEngine, NoOpCryptoEngine, RoomId};

#[test]
fn test_noop_crypto_engine_roundtrip_with_aad() {
    block_on(async {
        let engine = NoOpCryptoEngine;
        let room_id = RoomId::new("room-e2ee-test").unwrap();
        let aad = b"table_1:pk_42:col_3";
        let plaintext = b"sensitive user payload";

        let encrypted = engine.encrypt(&room_id, aad, plaintext).await.unwrap();
        assert_eq!(encrypted, plaintext);

        let decrypted = engine.decrypt(&room_id, aad, &encrypted).await.unwrap();
        assert_eq!(decrypted, plaintext);
    });
}
