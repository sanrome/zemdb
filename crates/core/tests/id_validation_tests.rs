use zemdb_core::{ClientId, RoomId, SchemaId};

#[test]
fn path_ids_accept_lowercase_digits_dash_and_underscore() {
    for id in ["room", "room-1", "team_a", "0", "a-b_c-9"] {
        assert!(RoomId::new(id).is_ok(), "room id {id:?} should be valid");
        assert!(
            SchemaId::new(id).is_ok(),
            "schema id {id:?} should be valid"
        );
    }
    assert!(RoomId::new("a".repeat(64)).is_ok());
}

#[test]
fn path_ids_reject_anything_that_could_escape_or_collide_on_disk() {
    let invalid = [
        "", "..", "../etc", "a/b", "a\\b", "room.1", "Room", "ROOM", "room id", "sala-ñ", "room\0",
    ];
    for id in invalid {
        assert!(
            RoomId::new(id).is_err(),
            "room id {id:?} should be rejected"
        );
        assert!(
            SchemaId::new(id).is_err(),
            "schema id {id:?} should be rejected"
        );
    }
    assert!(RoomId::new("a".repeat(65)).is_err());
}

#[test]
fn path_ids_reject_windows_reserved_device_names() {
    for id in ["con", "prn", "aux", "nul", "com1", "com9", "lpt1", "lpt9"] {
        assert!(
            RoomId::new(id).is_err(),
            "room id {id:?} should be rejected"
        );
        assert!(
            SchemaId::new(id).is_err(),
            "schema id {id:?} should be rejected"
        );
    }
    for id in ["console", "com10", "lpt", "nul-room"] {
        assert!(RoomId::new(id).is_ok(), "room id {id:?} should be valid");
    }
}

#[test]
fn client_ids_accept_any_printable_text_up_to_256_bytes() {
    for id in [
        "alice",
        "user.name@example.com",
        "Ñandú 42",
        "a/b",
        "x".repeat(256).as_str(),
    ] {
        assert!(
            ClientId::new(id).is_ok(),
            "client id {id:?} should be valid"
        );
    }
}

#[test]
fn client_ids_reject_empty_oversized_and_control_characters() {
    for id in ["", "line\nbreak", "tab\there", "nul\0"] {
        assert!(
            ClientId::new(id).is_err(),
            "client id {id:?} should be rejected"
        );
    }
    assert!(ClientId::new("x".repeat(257)).is_err());
}

#[test]
fn deserializing_an_invalid_id_fails() {
    assert!(serde_json::from_str::<RoomId>("\"../escape\"").is_err());
    assert!(serde_json::from_str::<SchemaId>("\"UPPER\"").is_err());
    assert!(serde_json::from_str::<ClientId>("\"\"").is_err());
    assert_eq!(
        serde_json::from_str::<RoomId>("\"room-1\"")
            .unwrap()
            .as_str(),
        "room-1"
    );

    let encoded = bincode::serialize("../escape").unwrap();
    assert!(bincode::deserialize::<RoomId>(&encoded).is_err());
}

#[test]
fn ids_serialize_as_plain_strings() {
    let room = RoomId::new("room-1").unwrap();
    assert_eq!(serde_json::to_string(&room).unwrap(), "\"room-1\"");
    assert_eq!(
        bincode::serialize(&room).unwrap(),
        bincode::serialize("room-1").unwrap()
    );
}

#[test]
fn path_ids_reject_com0_and_lpt0() {
    for id in ["com0", "lpt0"] {
        assert!(
            RoomId::new(id).is_err(),
            "room id {id:?} should be rejected"
        );
        assert!(
            SchemaId::new(id).is_err(),
            "schema id {id:?} should be rejected"
        );
    }
}

#[test]
fn client_ids_reject_invisible_format_and_bidi_characters() {
    let forbidden = [
        '\u{00AD}', '\u{061C}', '\u{180E}', '\u{200B}', '\u{200C}', '\u{200D}', '\u{200E}',
        '\u{200F}', '\u{2028}', '\u{2029}', '\u{202A}', '\u{202B}', '\u{202C}', '\u{202D}',
        '\u{202E}', '\u{2060}', '\u{2061}', '\u{2062}', '\u{2063}', '\u{2064}', '\u{2066}',
        '\u{2067}', '\u{2068}', '\u{2069}', '\u{206A}', '\u{206F}', '\u{FEFF}', '\u{FFF9}',
        '\u{FFFA}', '\u{FFFB}',
    ];
    for c in forbidden {
        let id = format!("ali{c}ce");
        assert!(
            ClientId::new(id.as_str()).is_err(),
            "client id with U+{:04X} should be rejected",
            c as u32
        );
    }
    // Neighbours of the forbidden ranges remain valid.
    for c in ['\u{2010}', '\u{2030}', '\u{2065}', '\u{2070}', '\u{FFFC}'] {
        let id = format!("ali{c}ce");
        assert!(
            ClientId::new(id.as_str()).is_ok(),
            "client id with U+{:04X} should be valid",
            c as u32
        );
    }
}

#[test]
fn invalid_id_error_does_not_echo_unbounded_input() {
    let long = "ñ".repeat(1000);
    let err = ClientId::new(long.as_str()).unwrap_err();
    assert_eq!(err.value.chars().count(), 81);
    assert!(err.value.ends_with('…'));
    assert!(err.value.starts_with(&"ñ".repeat(80)));
    assert!(err.to_string().len() < 400);

    let err = RoomId::new("A".repeat(100)).unwrap_err();
    assert_eq!(err.value, format!("{}…", "A".repeat(80)));

    // Short inputs are kept as they are.
    let err = RoomId::new("UPPER").unwrap_err();
    assert_eq!(err.value, "UPPER");
    let err = RoomId::new("B".repeat(80)).unwrap_err();
    assert_eq!(err.value, "B".repeat(80));
}

#[test]
fn client_ids_reject_other_invisible_filler_and_tag_characters() {
    for c in [
        '\u{034F}',
        '\u{115F}',
        '\u{3164}',
        '\u{FFA0}',
        '\u{180B}',
        '\u{FE0F}',
        '\u{E0041}',
        '\u{E0100}',
        '\u{0600}',
        '\u{1D173}',
    ] {
        let id = format!("alice{c}bob");
        assert!(
            ClientId::new(id.as_str()).is_err(),
            "client id with {c:?} should be rejected"
        );
    }
}

#[test]
fn client_ids_reject_leading_and_trailing_whitespace() {
    for id in [" alice", "alice ", "\u{3000}alice", "alice\u{00A0}"] {
        assert!(
            ClientId::new(id).is_err(),
            "client id {id:?} should be rejected"
        );
    }
    assert!(ClientId::new("alice smith").is_ok());
}
