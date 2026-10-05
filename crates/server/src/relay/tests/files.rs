use super::*;
use crate::relay::test_common::*;

#[test]
fn snapshot_file_names_roundtrip_with_underscores_in_room_ids() {
    let room_id = RoomId::new("a_b_c").unwrap();
    let hash = [0xAB; 32];
    let name = snapshot_file_name(&room_id, seq(77), &hash);
    assert_eq!(
        parse_snapshot_file_name(&name),
        Some((room_id.clone(), seq(77), hash))
    );
    let upload = upload_file_name(&room_id, seq(5), PART_EXTENSION);
    assert_eq!(parse_upload_file_name(&upload), Some((room_id, seq(5))));
    assert_eq!(parse_snapshot_file_name("a_b_c_77.snap"), None);
}
