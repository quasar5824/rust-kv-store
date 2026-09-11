mod store;

use store::KvStore;
use std::env;

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

    let missing = "nonexistent";
    println!("Get {}: {:?}", missing, store.get(missing));
}