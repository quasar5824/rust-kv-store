mod store;

use store::KvStore;

fn main() {
    let path = "store.db";
    let mut store = KvStore::open(path).expect("Failed to open store");

    let key1 = "greeting";
    let value1 = "Hello, Rust KV Store!";
    let key2 = "version";
    let value2 = "1.0.0";

    store.set(key1, value1).expect("Failed to set value");
    store.set(key2, value2).expect("Failed to set value");
    println!("Set values for {} and {}", key1, key2);

    println!("Current store contents:");
    for (k, v) in store.get_all() {
        println!("  {}: {}", k, v);
    }

    println!("Deleting key: {}", key1);
    store.delete(key1).expect("Failed to delete key");

    println!("Compacting log...");
    store.compact().expect("Failed to compact store");

    println!("Store contents after deletion and compaction:");
    for (k, v) in store.get_all() {
        println!("  {}: {}", k, v);
    }

    let missing = "nonexistent";
    println!("Get {}: {:?}", missing, store.get(missing));
}