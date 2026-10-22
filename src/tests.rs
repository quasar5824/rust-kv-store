use super::*;

#[test]
fn same_store_path_matches_identical_strings() {
    assert!(same_store_path("store.db", "store.db"));
}

#[test]
fn same_store_path_rejects_different_missing_paths() {
    assert!(!same_store_path("left.db", "right.db"));
}

#[test]
fn same_store_path_canonicalizes_existing_files() {
    let dir = std::env::temp_dir();
    let left = dir.join("rust_kv_store_left.db");
    let right = dir.join("rust_kv_store_right.db");
    std::fs::write(&left, b"").unwrap();
    std::fs::write(&right, b"").unwrap();

    assert!(!same_store_path(
        left.to_str().unwrap(),
        right.to_str().unwrap()
    ));

    let aliased = dir.join(".").join("rust_kv_store_left.db");
    assert!(same_store_path(
        aliased.to_str().unwrap(),
        left.to_str().unwrap()
    ));

    let _ = std::fs::remove_file(left);
    let _ = std::fs::remove_file(right);
}

#[test]
fn same_store_path_normalizes_missing_lexical_aliases() {
    assert!(same_store_path("missing.db", "./missing.db"));
    assert!(same_store_path("nested/../missing.db", "missing.db"));
    assert!(!same_store_path("nested/../missing.db", "other.db"));
}

#[test]
fn same_store_path_normalizes_existing_lexical_aliases() {
    let dir = std::env::temp_dir();
    let file = dir.join("rust_kv_store_alias.db");
    std::fs::write(&file, b"").unwrap();

    let alias = dir.join("nested").join("..").join("rust_kv_store_alias.db");
    assert!(same_store_path(
        alias.to_str().unwrap(),
        file.to_str().unwrap()
    ));

    let _ = std::fs::remove_file(file);
}

#[test]
fn same_store_path_resolves_aliases_through_existing_parents() {
    let dir = std::env::temp_dir().join("rust_kv_store_parents");
    let existing = dir.join("existing");
    std::fs::create_dir_all(&existing).unwrap();
    let file = existing.join("store.db");
    std::fs::write(&file, b"").unwrap();

    let alias = existing.join("nested").join("..").join("store.db");
    assert!(same_store_path(
        alias.to_str().unwrap(),
        file.to_str().unwrap()
    ));

    let missing = existing.join("missing.db");
    let missing_alias = existing.join("x").join("..").join("missing.db");
    assert!(same_store_path(
        missing_alias.to_str().unwrap(),
        missing.to_str().unwrap()
    ));

    let _ = std::fs::remove_file(file);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn normalize_store_path_collapses_current_and_parent_components() {
    assert_eq!(
        normalize_store_path("a/./b/../c"),
        std::path::PathBuf::from("a/c")
    );
    assert_eq!(
        normalize_store_path("./missing.db"),
        std::path::PathBuf::from("missing.db")
    );
}

#[test]
fn normalize_store_path_keeps_parent_segments_outside_root() {
    assert_eq!(
        normalize_store_path("/../a"),
        std::path::PathBuf::from("/a")
    );
}
