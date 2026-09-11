mod store;

use store::KvStore;

fn main() {
    let path = "store.db";
    let mut store = KvStore::open(path).expect("Failed to open store");

    let key = "greeting";
    let value = "Hello, Rust KV Store!";

    store.set(key, value).expect("Failed to set value");
    println!("Set {} = {}", key, value);

    match store.get(key) {
        Some(val) => println!("Get {}: {}", key, val),
        None => println!("Key not found"),
    }

    println!("Deleting key: {}", key);
    store.delete(key).expect("Failed to delete key");

    match store.get(key) {
        Some(val) => println!("Get {}: {}", key, val),
        None => println!("Get {}: Key not found (as expected)", key),
    }

    let missing = "nonexistent";
    println!("Get {}: {:?}", missing, store.get(missing));
}