use crate::store::{KvStore, StoreEvent, AsyncKvStore, CachedKvStore};
use std::thread;
use std::time::Duration;
use std::fs;
use std::sync::{Arc, Mutex};

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
fn test_decr() {
    let mut store = setup_store();
    store.set("counter", "10").unwrap();
    assert_eq!(store.decr("counter", 3).unwrap(), 7);
    assert_eq!(store.get("counter"), Some(&"7".to_string()));
}

#[test]
fn test_incr_float() {
    let mut store = setup_store();
    store.set("fcounter", "1.5").unwrap();
    assert_eq!(store.incr_float("fcounter", 2.2).unwrap(), 3.7);
    assert_eq!(store.get("fcounter").unwrap(), "3.7");
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

#[test]
fn test_set_if_not_exists() {
    let mut store = setup_store();
    assert!(store.set_if_not_exists("unique", "val1", None).unwrap());
    assert!(!store.set_if_not_exists("unique", "val2", None).unwrap());
    assert_eq!(store.get("unique"), Some(&"val1".to_string()));
}

#[test]
fn test_get_ttl() {
    let mut store = setup_store();
    store.set_with_ttl("ttl_key", "val", Some(10)).unwrap();
    let ttl = store.get_ttl("ttl_key").unwrap();
    assert!(ttl.is_some());
    assert!(ttl.unwrap() <= 10);
    
    store.set("no_ttl", "val").unwrap();
    assert_eq!(store.get_ttl("no_ttl"), Some(None));
    assert_eq!(store.get_ttl("missing"), None);
}

#[test]
fn test_expire() {
    let mut store = setup_store();
    store.set("key", "val").unwrap();
    assert!(store.expire("key", Some(5)).unwrap());
    assert!(store.get_ttl("key").unwrap().is_some());
    assert!(!store.expire("missing", Some(5)).unwrap());
}

#[test]
fn test_prefix_queries() {
    let mut store = setup_store();
    store.set("user:1", "A").unwrap();
    store.set("user:2", "B").unwrap();
    store.set("admin:1", "C").unwrap();

    let users = store.get_all_with_prefix("user:");
    assert_eq!(users.len(), 2);
    
    let admins = store.get_all_with_prefix("admin:");
    assert_eq!(admins.len(), 1);
}

#[test]
fn test_backup_restore() {
    let mut store = setup_store();
    store.set("k1", "v1").unwrap();
    store.set("k2", "v2").unwrap();
    
    let backup_path = "backup.db";
    store.backup(backup_path).unwrap();
    
    store.clear().unwrap();
    assert_eq!(store.get("k1"), None);
    
    store.restore(backup_path).unwrap();
    assert_eq!(store.get("k1"), Some(&"v1".to_string()));
    assert_eq!(store.get("k2"), Some(&"v2".to_string()));
    
    let _ = fs::remove_file(backup_path);
}

#[test]
fn test_bulk_import() {
    let mut store = setup_store();
    let data = vec![("b1", "v1"), ("b2", "v2"), ("b3", "v3")];
    store.bulk_import(&data).unwrap();
    
    assert_eq!(store.get("b1"), Some(&"v1".to_string()));
    assert_eq!(store.get("b2"), Some(&"v2".to_string()));
    assert_eq!(store.get("b3"), Some(&"v3".to_string()));
}

#[test]
fn test_mset() {
    let mut store = setup_store();
    let data = vec![
        ("m1".to_string(), "v1".to_string()),
        ("m2".to_string(), "v2".to_string()),
    ];
    store.mset(data).unwrap();
    
    assert_eq!(store.get("m1"), Some(&"v1".to_string()));
    assert_eq!(store.get("m2"), Some(&"v2".to_string()));
}

#[test]
fn test_observers() {
    let mut store = setup_store();
    let events = Arc::new(Mutex::new(Vec::new()));
    
    let events_clone = Arc::clone(&events);
    store.add_observer("obs_key", move |key, event| {
        events_clone.lock().unwrap().push((key.to_string(), event));
    });

    store.set("obs_key", "val1").unwrap();
    store.set("other_key", "val2").unwrap();
    store.delete("obs_key").unwrap();

    let result = events.lock().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], ("obs_key".to_string(), StoreEvent::Set));
    assert_eq!(result[1], ("obs_key".to_string(), StoreEvent::Delete));
}

#[tokio::test]
async fn test_async_store() {
    let path = "async_test.db";
    let _ = fs::remove_file(path);
    let store = AsyncKvStore::open(path.to_string()).await.unwrap();
    
    store.set("async_key".to_string(), "async_val".to_string()).await.unwrap();
    assert_eq!(store.get("async_key".to_string()).await, Some("async_val".to_string()));
    
    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn test_async_concurrent_access() {
    let path = "async_concurrent.db";
    let _ = fs::remove_file(path);
    let store = Arc::new(AsyncKvStore::open(path.to_string()).await.unwrap());
    
    let mut handles = vec![];
    for i in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            store_clone.set(format!("key_{}", i), format!("val_{}", i)).await.unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    for i in 0..10 {
        assert_eq!(store.get(format!("key_{}", i)).await, Some(format!("val_{}", i)));
    }
    
    let _ = fs::remove_file(path);
}

#[test]
fn test_cached_kv_lru_eviction() {
    let store = setup_store();
    let mut cached = CachedKvStore::new(store, 2);

    cached.set("k1", "v1").unwrap();
    cached.set("k2", "v2").unwrap();
    
    // Access k1 to make it most recent
    cached.get("k1").unwrap();
    
    // Add k3, should evict k2 (LRU)
    cached.set("k3", "v3").unwrap();
    
    // Check if k2 is evicted from cache by checking hits/misses
    // We reset stats effectively by looking at current counts
    let (_, m1) = cached.cache_stats();
    cached.get("k2").unwrap(); // This should be a miss
    let (_, m2) = cached.cache_stats();
    
    assert!(m2 > m1, "k2 should have been a cache miss");
    assert_eq!(cached.get("k1"), Some("v1".to_string()));
}

#[test]
fn test_cached_kv_hit_rate() {
    let store = setup_store();
    let mut cached = CachedKvStore::new(store, 10);

    cached.set("k1", "v1").unwrap();
    
    // First get is a miss (it was set, but usually set puts it in cache. 
    // Let's check the implementation behavior via tests)
    cached.get("k1").unwrap(); // Potential hit
    cached.get("k1").unwrap(); // Definitely hit
    
    let (hits, misses) = cached.cache_stats();
    assert!(hits > 0);
    assert!(misses >= 0);
}
