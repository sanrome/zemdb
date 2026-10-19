use super::*;
use tempfile::tempdir;

#[test]
fn write_atomic_creates_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta.json");

    write_atomic(&path, b"first").unwrap();

    assert_eq!(fs::read(&path).unwrap(), b"first");
}

#[test]
fn write_atomic_replaces_content_and_leaves_no_tmp() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta.json");
    fs::write(&path, b"a much longer previous content").unwrap();

    write_atomic(&path, b"new").unwrap();

    assert_eq!(fs::read(&path).unwrap(), b"new");
    assert!(!tmp_path_for(&path).exists());
}

#[test]
fn write_atomic_overwrites_stale_tmp_from_previous_crash() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta.json");
    fs::write(tmp_path_for(&path), b"garbage left by a crash").unwrap();

    write_atomic(&path, b"valid").unwrap();

    assert_eq!(fs::read(&path).unwrap(), b"valid");
    assert!(!tmp_path_for(&path).exists());
}

#[test]
fn create_dir_all_synced_creates_missing_components() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("a").join("b");

    create_dir_all_synced(&target).unwrap();
    create_dir_all_synced(&target).unwrap();

    assert!(target.is_dir());
}

#[test]
fn create_dir_all_synced_fails_when_syncing_a_new_parent_fails() {
    let dir = tempdir().unwrap();
    let first = dir.path().join("a");

    crate::fail_point::arm("sync_dir", &first);
    assert!(create_dir_all_synced(&first.join("b")).is_err());
}

/// The bytes `write_checked` leaves on disk for `json`.
fn checked_bytes(json: &str) -> Vec<u8> {
    let dir = tempdir().unwrap();
    let path = dir.path().join("checked.json");
    write_checked(&path, json.as_bytes()).unwrap();
    fs::read(&path).unwrap()
}

/// Every file `write_checked` writes is accepted by `verified_json`, which returns exactly the
/// JSON written: compact and pretty JSON, an empty document, non-ASCII text, and JSON whose
/// strings mention the checksum line (escaped, as serde_json always writes a newline inside a
/// string).
#[test]
fn checked_file_round_trips_any_json() {
    let pretty = serde_json::to_string_pretty(&serde_json::json!({
        "a": [1, 2, {"b": "c"}],
        "crc32": "\ncrc32:00000000\n",
        "ñandú": -0.0,
    }))
    .unwrap();
    for json in [
        r#"{"pruned_through_seq":10,"sealed_through_seq":22}"#,
        pretty.as_str(),
        "",
        "{}",
        "\n",
        "crc32:00000000",
        "{\"x\":\"\\ncrc32:deadbeef\"}",
    ] {
        let bytes = checked_bytes(json);
        assert_eq!(
            verified_json(&bytes),
            Some(json),
            "write_checked wrote {:?}, which verified_json does not give back",
            String::from_utf8_lossy(&bytes)
        );
    }
}

/// `verified_json` accepts only the exact format `write_checked` produces: no CRLF, exactly
/// one final newline, lowercase hex of exactly 8 digits, nothing after the checksum line.
#[test]
fn checked_file_rejects_every_variant_of_its_format() {
    let json = r#"{"pruned_through_seq":10,"sealed_through_seq":22}"#;
    let crc = format!("{:08x}", crc32fast::hash(json.as_bytes()));
    let canonical = format!("{json}\ncrc32:{crc}\n");
    assert_eq!(checked_bytes(json), canonical.as_bytes());
    assert_eq!(verified_json(canonical.as_bytes()), Some(json));

    let variants = [
        ("no checksum line", json.to_string()),
        ("no final newline", format!("{json}\ncrc32:{crc}")),
        ("two final newlines", format!("{json}\ncrc32:{crc}\n\n")),
        ("CRLF", format!("{json}\r\ncrc32:{crc}\r\n")),
        (
            "CR before the final newline",
            format!("{json}\ncrc32:{crc}\r\n"),
        ),
        (
            "uppercase hex",
            format!("{json}\ncrc32:{}\n", crc.to_uppercase()),
        ),
        ("7 hex digits", format!("{json}\ncrc32:{}\n", &crc[1..])),
        ("9 hex digits", format!("{json}\ncrc32:0{crc}\n")),
        ("0x prefix", format!("{json}\ncrc32:0x{crc}\n")),
        ("space after the colon", format!("{json}\ncrc32: {crc}\n")),
        ("trailing space", format!("{json}\ncrc32:{crc} \n")),
        ("leading space", format!("{json}\n crc32:{crc}\n")),
        ("other label", format!("{json}\nCRC32:{crc}\n")),
        (
            "two checksum lines",
            format!("{json}\ncrc32:{crc}\ncrc32:{crc}\n"),
        ),
        (
            "a line after the checksum",
            format!("{json}\ncrc32:{crc}\n{{}}\n"),
        ),
        (
            "a space added to the JSON",
            format!("{json} \ncrc32:{crc}\n"),
        ),
        (
            "a BOM before the JSON",
            format!("\u{feff}{json}\ncrc32:{crc}\n"),
        ),
        ("checksum line first", format!("crc32:{crc}\n{json}\n")),
        ("checksum of 0", format!("{json}\ncrc32:00000000\n")),
    ];
    for (variant, text) in variants {
        assert_eq!(
            verified_json(text.as_bytes()),
            None,
            "verified_json accepted a file with {variant}: {text:?}"
        );
    }
    let mut not_utf8 = canonical.into_bytes();
    not_utf8.insert(1, 0xff);
    assert_eq!(verified_json(&not_utf8), None, "accepted invalid UTF-8");
}

/// No single bit flip and no truncation of a checked file passes verification, wherever it
/// lands: in the JSON, in the checksum line or in the newlines.
#[test]
fn every_bit_flip_and_truncation_of_a_checked_file_is_rejected() {
    let pretty = serde_json::to_string_pretty(&serde_json::json!({
        "room_id": "room-a",
        "schema_id": "todo",
        "room_incarnation": 123_456_789_u128,
        "lifecycle": {"ram_max_ops": 1000},
    }))
    .unwrap();
    for json in [
        r#"{"pruned_through_seq":10,"sealed_through_seq":22}"#,
        &pretty,
    ] {
        let bytes = checked_bytes(json);
        for (damage, damaged) in crate::test_support::every_truncation_and_bit_flip(&bytes) {
            assert_eq!(
                verified_json(&damaged),
                None,
                "a file {damage} verifies: {:?}",
                String::from_utf8_lossy(&damaged)
            );
        }
    }
}

/// `verified_json` reads hostile bytes from disk (empty, no newline, a large file without
/// newlines, invalid UTF-8, odd checksum lines, a checksum of other JSON) as unreadable, without
/// panicking, and only the last line counts as the checksum, even when the JSON holds a line
/// that looks like one. A write that stopped before its rename keeps the previous version,
/// which still verifies.
#[test]
fn verified_json_reads_hostile_files_as_unreadable() {
    let checked = |json: &str| format!("{json}\ncrc32:{:08x}\n", crc32fast::hash(json.as_bytes()));
    let valid = checked("{\"a\":1}");
    assert_eq!(verified_json(valid.as_bytes()), Some("{\"a\":1}"));

    let large = vec![b'a'; 4 << 20];
    let mut not_utf8 = valid.clone().into_bytes();
    not_utf8[2] = 0xff;
    let crc = crc32fast::hash(b"{\"a\":1}");
    let hostile: Vec<Vec<u8>> = vec![
        Vec::new(),
        b"\n".to_vec(),
        b"crc32:\n".to_vec(),
        b"\ncrc32:\n".to_vec(),
        large,
        not_utf8,
        valid.trim_end().as_bytes().to_vec(),
        format!("{{\"a\":1}}\ncrc32:{crc:08X}\n").into_bytes(),
        format!("{{\"a\":1}}\ncrc32:{crc:09x}\n").into_bytes(),
        format!("{{\"a\":1}}\ncrc32:{crc:08x}\r\n").into_bytes(),
        format!("{{\"a\":1}}\ncrc32:{crc:08x}\n\n").into_bytes(),
        format!("{{\"a\":1}}\ncrc32:é{crc:07x}\n").into_bytes(),
        format!("{{\"a\":2}}\ncrc32:{crc:08x}\n").into_bytes(),
    ];
    for bytes in &hostile {
        assert_eq!(
            verified_json(bytes),
            None,
            "verified_json accepted {:?}",
            String::from_utf8_lossy(&bytes[..bytes.len().min(40)])
        );
    }
    let embedded = checked("{\"a\":\"x\ncrc32:00000000\"}");
    assert_eq!(
        verified_json(embedded.as_bytes()),
        Some("{\"a\":\"x\ncrc32:00000000\"}")
    );

    let dir = tempdir().unwrap();
    let path = dir.path().join("log_meta.json");
    write_checked(&path, b"{\"v\":1}").unwrap();
    crate::fail_point::arm("write_atomic_before_rename", &path);
    assert!(write_checked(&path, b"{\"v\":2}").is_err());
    assert_eq!(verified_json(&fs::read(&path).unwrap()), Some("{\"v\":1}"));
    assert_eq!(
        verified_json(&fs::read(tmp_path_for(&path)).unwrap()),
        Some("{\"v\":2}"),
        "the interrupted write left its complete temporary file, which readers remove"
    );
}
