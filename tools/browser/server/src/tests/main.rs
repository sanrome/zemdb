use super::page_file;

#[test]
fn a_path_names_a_file_of_the_page_directory() {
    assert_eq!(page_file("/"), Some("index.html"));
    assert_eq!(page_file("/harness.js"), Some("harness.js"));
    assert_eq!(
        page_file("/pkg/zemdb_browser_steps_bg.wasm"),
        Some("pkg/zemdb_browser_steps_bg.wasm")
    );
    // Percent-encoding is not decoded: `%2e%2e` is a name, not `..`.
    assert_eq!(page_file("/%2e%2e/Cargo.toml"), Some("%2e%2e/Cargo.toml"));
}

#[test]
fn no_path_leaves_the_page_directory() {
    for path in [
        "",
        "harness.js",
        "/..",
        "/../Cargo.toml",
        "/pkg/../../Cargo.toml",
        "/./harness.js",
        "//etc/passwd",
        "/pkg//x.wasm",
        "/pkg/",
        "/..\\Cargo.toml",
        "/C:/Windows/win.ini",
    ] {
        assert_eq!(page_file(path), None, "{path}");
    }
}
