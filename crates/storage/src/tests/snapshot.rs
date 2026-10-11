use super::*;

fn compressed(bytes: &[u8]) -> Vec<u8> {
    zstd::encode_all(bytes, 3).unwrap()
}

#[test]
fn string_in_a_stream_round_trips() {
    let raw = bincode::serialize("a string").unwrap();
    let decoded: String =
        deserialize_zstd(&compressed(&raw), raw.len() as u64, Some(raw.len() as u64)).unwrap();
    assert_eq!(decoded, "a string");
}

#[test]
fn string_longer_than_its_stream_reserves_nothing() {
    // A string that declares almost `usize::MAX` bytes and holds none. Reserving its declared
    // length up front would panic (capacity overflow) instead of failing as corrupt data; the
    // limit does not stop it, as when a disk room reads its own snapshot.
    let raw = (usize::MAX as u64 - 64).to_le_bytes();
    let result = deserialize_zstd::<String>(&compressed(&raw), u64::MAX, None);
    assert!(
        matches!(result, Err(StorageError::SnapshotCorruption(_))),
        "{result:?}"
    );
}

#[test]
fn string_that_is_not_utf8_is_rejected() {
    let mut raw = 2u64.to_le_bytes().to_vec();
    raw.extend_from_slice(&[0xFF, 0xFE]);
    let result = deserialize_zstd::<String>(&compressed(&raw), raw.len() as u64, None);
    assert!(
        matches!(result, Err(StorageError::SnapshotCorruption(_))),
        "{result:?}"
    );
}
