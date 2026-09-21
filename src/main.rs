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
        Command::Set { key: "config:version".to_string(), value: "1.1.0".to_string() },
    ];
    store.batch(batch).expect("Failed to execute batch");

    println!("\nPerforming atomic transaction...");
    store.transaction(|tx| {
        tx.set("tx:1".to_string(), "Value 1".to_string());
        tx.set("tx:2".to_string(), "Value 2".to_string());
        true // commit
    }).expect("Transaction failed");

    println!("\nScanning for keys starting with 'user:':");
    for (k, v) in store.scan("user:") {
        println!("  {}: {}", k, v);
    }

    println!("\nQuerying range 'user:2' to 'user:4':");
    for (k, v) in store.range("user:2", "user:4") {
        println!("  {}: {}", k, v);
    }

    println!("\nUpdating key: {}", key2);
    if store.update(key2, "Bobby").expect("Failed to update") {
        println!("Successfully updated {} to Bobby", key2);
    } else {
        println!("Key {} not found for update", key2);
    }

    println!("\nDeleting key: {}", key1);
    store.delete(key1).expect("Failed to delete key");

    println!("Compacting log...");
    store.compact().expect("Failed to compact store");

    println!("\nStore contents after deletion and compaction (via cursor):");
    for (k, v) in store.cursor() {
        println!("  {}: {}", k, v);
    }

    let missing = "nonexistent";
    println!("Get {}: {:?}", missing, store.get(missing));
    println!("Exists {}: {}", missing, store.exists(missing));
    println!("Exists {}: {}", "user:2", store.exists("user:2"));

    println!("\nStore Stats: {:?}", store.stats());

    println!("\nClearing store...");
    store.clear().expect("Failed to clear store");
    println!("Store size after clear: {}", store.get_all().len());
    println!("Store Stats after clear: {:?}", store.stats());
}
