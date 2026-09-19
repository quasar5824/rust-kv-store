mod store;

use store::{KvStore, Command};

fn main() {
    let path = "store.db";
    let mut store = KvStore::open(path).expect("Failed to open store");

    let key1 = "user:1";
    let value1 = "Alice";
    let key2 = "user:2";
    let value2 = "Bob";
    let key3 = "config:version";
    let value3 = "1.0.0";

    store.set(key1, value1).expect("Failed to set value");
    store.set(key2, value2).expect("Failed to set value");
    store.set(key3, value3).expect("Failed to set value");
    println!("Set values for {}, {}, and {}", key1, key2, key3);

    println!("\nPerforming batch update...");
    let batch = vec![
        Command::Set { key: "user:3".to_string(), value: "Charlie".to_string() },
        Command::Set { key: "user:4".to_string(), value: "Dave".to_string() },
        Command::Delete { key: "config:version".to_string() },
    ];
    store.batch(batch).expect("Failed to execute batch");

    println!("\nScanning for keys starting with 'user:':");
    for (k, v) in store.scan("user:") {
        println!("  {}: {}", k, v);
    }

    println!("\nDeleting key: {}", key1);
    store.delete(key1).expect("Failed to delete key");

    println!("Compacting log...");
    store.compact().expect("Failed to compact store");

    println!("\nStore contents after deletion and compaction:");
    for (k, v) in store.get_all() {
        println!("  {}: {}", k, v);
    }

    let missing = "nonexistent";
    println!("Get {}: {:?}", missing, store.get(missing));
    println!("Exists {}: {}", missing, store.exists(missing));
    println!("Exists {}: {}", "user:2", store.exists("user:2"));
}
