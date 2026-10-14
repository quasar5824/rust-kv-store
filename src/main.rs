mod store;

#[cfg(test)]
mod tests;

use store::{KvStore, Command};
use std::io::{self, Write};
use std::thread;
use std::time::{Duration, Instant};

fn run_benchmarks(store: &mut KvStore) {
    println!("\n--- Running Performance Benchmarks ---");
    
    let iterations = 5000;
    
    // Benchmark SET
    let start = Instant::now();
    for i in 0..iterations {
        store.set(&format!("bench_set_{}", i), "value").unwrap();
    }
    let duration = start.elapsed();
    println!("SET: {} ops in {:?}, avg {:?} per op, throughput {:.2} ops/s", 
        iterations, duration, duration / iterations, iterations as f64 / duration.as_secs_f64());

    // Benchmark GET
    let start = Instant::now();
    for i in 0..iterations {
        store.get(&format!("bench_set_{}", i));
    }
    let duration = start.elapsed();
    println!("GET: {} ops in {:?}, avg {:?} per op, throughput {:.2} ops/s", 
        iterations, duration, duration / iterations, iterations as f64 / duration.as_secs_f64());

    // Mixed Workload Benchmark (80% GET, 20% SET)
    println!("Mixed Workload (80% GET, 20% SET): ");
    let start = Instant::now();
    for i in 0..iterations {
        if i % 5 == 0 {
            store.set(&format!("bench_mixed_{}", i), "value").unwrap();
        } else {
            store.get(&format!("bench_mixed_{}", i % 100));
        }
    }
    let duration = start.elapsed();
    println!("Mixed: {} ops in {:?}, throughput {:.2} ops/s", 
        iterations, duration, iterations as f64 / duration.as_secs_f64());
    
    println!("-------------------------------------\n");
}

fn run_demo(store: &mut KvStore) {
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

    println!("\nTesting TTL (Time-To-Live)...");
    let ttl_key = "temp:session";
    let ttl_val = "secret_token";
    store.set_with_ttl(ttl_key, ttl_val, Some(2)).expect("Failed to set TTL value");
    println!("Set {} with 2s TTL: {:?}", ttl_key, store.get(ttl_key));

    println!("Waiting 3 seconds for expiration...");
    thread::sleep(Duration::from_secs(3));
    println!("Get {} after 3s: {:?}", ttl_key, store.get(ttl_key));

    println!("\nPerforming batch update...");
    let batch = vec![
        Command::Set { key: "user:3".to_string(), value: "Charlie".to_string(), ttl: None },
        Command::Set { key: "user:4".to_string(), value: "Dave".to_string(), ttl: None },
        Command::Set { key: "config:version".to_string(), value: "1.1.0".to_string(), ttl: None },
    ];
    store.batch(batch).expect("Failed to execute batch");

    println!("\nPerforming bulk import...");
    let imports = vec![
        ("app:name", "RustKV"),
        ("app:env", "development"),
        ("app:debug", "true"),
    ];
    store.bulk_import(&imports).expect("Failed to bulk import");

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

    println!("\nStore contents after deletion and compaction:");
    for (k, v) in store.cursor() {
        println!("  {}: {}", k, v);
    }

    println!("\nRetrieving all entries using get_all:");
    let all_entries = store.get_all().expect("Failed to get all");
    for (k, v) in all_entries {
        println!("  {}: {}", k, v);
    }

    let missing = "nonexistent";
    println!("Get {}: {:?}", missing, store.get(missing));
    println!("Exists {}: {}", missing, store.exists(missing));
    println!("Exists {}: {}", "user:2", store.exists("user:2"));

    println!("\nStore Stats: {:?}", store.stats());
}

fn run_repl(store: &mut KvStore) {
    println!("Welcome to RustKV REPL!");
    println!("Commands: set <key> <val>, get <key>, del <key>, stats, exit");
    
    loop {
        print!("kv> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let parts: Vec<&str> = input.trim().split_whitespace().collect();

        if parts.is_empty() { continue; }

        match parts[0] {
            "set" if parts.len() >= 3 => {
                store.set(parts[1], parts[2..].join(" ")).unwrap();
                println!("OK");
            }
            "get" if parts.len() >= 2 => {
                match store.get(parts[1]) {
                    Some(val) => println!("\n{}", val),
                    None => println!("(nil)"),
                }
            }
            "del" if parts.len() >= 2 => {
                store.delete(parts[1]).unwrap();
                println!("OK");
            }
            "stats" => {
                println!("{:?}", store.stats());
            }
            "exit" | "quit" => break,
            _ => println!("Unknown command or wrong arguments"),
        }
    }
}

fn main() {
    let path = "store.db";
    let mut store = KvStore::open(path).expect("Failed to open store");

    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "--bench" => run_benchmarks(&mut store),
            "--demo" => {
                run_demo(&mut store);
                run_benchmarks(&mut store);
            }
            "--repl" => run_repl(&mut store),
            _ => println!("Usage: cargo run [--bench | --demo | --repl] (default: --demo)"),
        }
    } else {
        run_demo(&mut store);
        run_benchmarks(&mut store);
    }
}
