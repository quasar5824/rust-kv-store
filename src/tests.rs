use crate::store::KvStore;
use std::thread;
use std::time::Duration;
use std::fs;

fn setup_store() -> KvStore {
    let path = "test_store.db";
    let _ = fs::remove_file(path);
    KvStore::open(path).expect("Failed to open test store")
}

#[test]
fn test_basic_set_get() {
    let mut store = setup_store();
    store.set("key1", "val1").unwrap();
    assert_eq!(store.get("key1"), Some(&"val1".to_string()));
    assert_eq!(store.get("nonexistent"), None);
}

#[test]
fn test_ttl_expiration() {
    let mut store = setup_store();
    store.set_with_ttl("temp", "val", Some(1)).unwrap();
    assert_eq!(store.get("temp"), Some(&"val".to_string()));
    
    thread::sleep(Duration::from_secs(2));
    assert_eq!(store.get("temp"), None);
}

#[test]
fn test_incr() {
    let mut store = setup_store();
    store.incr("counter", 1).unwrap();
    assert_eq!(store.incr("counter", 5).unwrap(), 6);
    assert_eq!(store.get("counter"), Some(&"6".to_string()));
}

#[test]
fn test_transaction() {
    let mut store = setup_store();
    store.transaction(|tx| {
        tx.set("t1".to_string(), "v1".to_string());
        tx.set("t2".to_string(), "v2".to_string());
        true
    }).unwrap();
    assert_eq!(store.get("t1"), Some(&"v1".to_string()));
    assert_eq!(store.get("t2"), Some(&"v2".to_string()));

    store.transaction(|tx| {
        tx.set("t1".to_string(), "v1_new".to_string());
        false // rollback
    }).unwrap();
    assert_eq!(store.get("t1"), Some(&"v1".to_string()));
}

#[test]
fn test_compaction() {
    let mut store = setup_store();
    store.set("k1", "v1").unwrap();
    store.set("k1", "v2").unwrap();
    store.delete("k1").unwrap();
    
    store.compact().unwrap();
    assert_eq!(store.get("k1"), None);
    assert_eq!(store.stats().key_count, 0);
}

#[test]
fn test_persistence() {
    let path = "persist_test.db";
    let _ = fs::remove_file(path);
    
    {
        let mut store = KvStore::open(path).unwrap();
        store.set("persist", "ok").unwrap();
    }

    let mut store = KvStore::open(path).unwrap();
    assert_eq!(store.get("persist"), Some(&"ok".to_string()));
    let _ = fs::remove_file(path);
}

#[test]
fn test_range_scan() {
    let mut store = setup_store();
    store.set("a", "1").unwrap();
    store.set("b", "2").unwrap();
    store.set("c", "3").unwrap();

    let range = store.range("a", "b");
    assert_eq!(range.len(), 2);
    
    let scan = store.scan("b");
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].0, "b");
}

#[test]
fn test_get_or_set() {
    let mut store = setup_store();
    
    // Set new value
    let val1 = store.get_or_set("gos1", "val1", None).unwrap();
    assert_eq!(val1, "val1");
    assert_eq!(store.get("gos1"), Some(&"val1".to_string()));

    // Get existing value
    let val2 = store.get_or_set("gos1", "val_new", None).unwrap();
    assert_eq!(val2, "val1");
    assert_eq!(store.get("gos1"), Some(&"val1".to_string()));
}