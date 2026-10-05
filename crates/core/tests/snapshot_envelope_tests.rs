use zemdb_core::protocol::snapshot_envelope::*;

fn raw_envelope(body: &[u8]) -> Vec<u8> {
    let header =
        SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, body.len() as u32, body);
    let mut out = header.to_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

#[test]
fn header_roundtrips_and_validates() {
    let envelope = raw_envelope(b"payload");
    let header = validate_snapshot_envelope(&envelope).unwrap();
    assert_eq!(header.compression(), SnapshotCompression::Raw);
    assert_eq!(header.uncompressed_len(), 7);
    assert_eq!(SnapshotEnvelopeHeader::parse(&envelope).unwrap(), header);
    assert_eq!(&header.to_bytes()[..], &envelope[..SNAPSHOT_HEADER_LEN]);
}

#[test]
fn invalid_headers_are_rejected() {
    let envelope = raw_envelope(b"payload");

    assert_eq!(
        validate_snapshot_envelope(&envelope[..SNAPSHOT_HEADER_LEN - 1]),
        Err(SnapshotEnvelopeError::TooShort)
    );

    let mut bad_magic = envelope.clone();
    bad_magic[0] = b'X';
    assert!(matches!(
        validate_snapshot_envelope(&bad_magic),
        Err(SnapshotEnvelopeError::InvalidMagic(_))
    ));

    let mut bad_version = envelope.clone();
    bad_version[4] = 9;
    assert_eq!(
        validate_snapshot_envelope(&bad_version),
        Err(SnapshotEnvelopeError::UnsupportedVersion(9))
    );

    let mut bad_flag = envelope.clone();
    bad_flag[5] = 7;
    assert_eq!(
        validate_snapshot_envelope(&bad_flag),
        Err(SnapshotEnvelopeError::UnknownCompression(7))
    );
}

#[test]
fn corrupted_body_fails_the_checksum() {
    let mut envelope = raw_envelope(b"payload");
    let last = envelope.len() - 1;
    envelope[last] ^= 0xFF;
    assert!(matches!(
        validate_snapshot_envelope(&envelope),
        Err(SnapshotEnvelopeError::ChecksumMismatch { .. })
    ));
}

#[test]
fn raw_body_must_match_the_declared_length() {
    let body = b"payload";
    let header = SnapshotEnvelopeHeader::for_body(SnapshotCompression::Raw, 100, body);
    let mut envelope = header.to_bytes().to_vec();
    envelope.extend_from_slice(body);
    assert_eq!(
        validate_snapshot_envelope(&envelope),
        Err(SnapshotEnvelopeError::RawLengthMismatch {
            declared: 100,
            actual: 7
        })
    );
}

#[test]
fn incremental_validation_matches_whole_validation() {
    let envelope = raw_envelope(&[0x5Au8; 1000]);
    for piece in [1, 3, 13, 14, 15, 999] {
        let mut validator = SnapshotEnvelopeValidator::new();
        for chunk in envelope.chunks(piece) {
            validator.update(chunk);
        }
        assert_eq!(
            validator.finish(),
            validate_snapshot_envelope(&envelope),
            "piece size {piece}"
        );
    }

    let mut corrupted = envelope.clone();
    corrupted[500] ^= 1;
    let mut validator = SnapshotEnvelopeValidator::new();
    validator.update(&corrupted);
    assert!(matches!(
        validator.finish(),
        Err(SnapshotEnvelopeError::ChecksumMismatch { .. })
    ));
}
