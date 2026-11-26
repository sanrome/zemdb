use super::*;

#[test]
fn the_error_of_a_taken_file_is_recognized_and_no_other_is() {
    let taken = FileTaken::error("move(room_a.wal, room_b.wal): NoModificationAllowedError");
    assert!(FileTaken::is(&taken));
    assert_eq!(taken.kind(), io::ErrorKind::ResourceBusy);
    assert_eq!(
        taken.to_string(),
        "move(room_a.wal, room_b.wal): NoModificationAllowedError"
    );

    let busy = io::Error::new(io::ErrorKind::ResourceBusy, "busy");
    assert!(!FileTaken::is(&busy));
    assert!(!FileTaken::is(&io::Error::from(
        io::ErrorKind::ResourceBusy
    )));
    assert!(!FileTaken::is(&io::Error::other(
        "NoModificationAllowedError"
    )));
}

#[test]
fn sync_dir_covers_the_earlier_changes_on_the_file_system_of_the_process() {
    assert!(NativeFiles.sync_dir_covers_earlier_changes());
}
