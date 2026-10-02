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
