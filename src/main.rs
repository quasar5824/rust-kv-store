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
    
    println!("\nTesting store clearing...");
    store.clear().expect("Failed to clear store");
    println!("Store cleared. Stats: {:?}", store.stats());
}

fn print_help() {
    println!("Available Commands:");
    println!("  set <key> <val> [ttl_secs] - Store a value with optional TTL");
    println!("  get <key>                - Retrieve a value");
    println!("  ttl <key>                - Get remaining TTL for a key");
    println!("  get-or-set <key> <val>   - Get value, or set to <val> if missing");
    println!("  del <key>                - Delete a value");
    println!("  incr <key> <amount>      - Increment a numeric value");
    println!("  decr <key> <amount>      - Decrement a numeric value");
    println!("  scan <prefix>            - Scan keys starting with prefix");
    println!("  range <start> <end>     - Get keys in a range");
    println!("  keys                     - List all keys");
    println!("  count                    - Get total number of keys");
    println!("  stats                    - Show store statistics");
    println!("  flush                    - Force write-ahead log to disk");
    println!("  dump <file>              - Export store to JSON file");
    println!("  watch <key>              - Notify when the specified key changes");
    println!("  clear                    - Wipe all data from the store");
    println!("  version                  - Show store version");
    println!("  help                     - Show this help message");
    println!("  exit | quit              - Exit the REPL");
}

fn run_repl(store: &mut KvStore) {
    println!("Welcome to RustKV REPL!");
    println!("Type 'help' to see available commands.");
    
    loop {
        print!("kv> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let parts: Vec<&str> = input.trim().split_whitespace().collect();

        if parts.is_empty() { continue; }

        match parts[0] {
            "set" if parts.len() >= 3 => {
                let key = parts[1];
                if let Some(last_part) = parts.last() {
                    if let Ok(ttl) = last_part.parse::<u64>() && parts.len() > 3 {
                        let val = parts[2..parts.len()-1].join(" ");
                        store.set_with_ttl(key, &val, Some(ttl)).unwrap();
                        println!("OK (ttl: {})", ttl);
                        continue;
                    }
                }
                let val = parts[2..].join(" ");
                store.set(key, &val).unwrap();
                println!("OK");
            }
            "get" if parts.len() >= 2 => {
                match store.get(parts[1]) {
                    Some(val) => println!("\n{}", val),
                    None => println!("(nil)"),
                }
            }
            "ttl" if parts.len() >= 2 => {
                match store.get_ttl(parts[1]) {
                    Some(Some(t)) => println!("\nTTL: {}s", t),
                    Some(None) => println!("\nNo TTL set"),
                    None => println!("(nil)"),
                }
            }
            "get-or-set" if parts.len() >= 3 => {
                let key = parts[1];
                let val = parts[2..].join(" ");
                match store.get_or_set(key, &val, None) {
                    Ok(v) => println!("\n{}", v),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "del" if parts.len() >= 2 => {
                store.delete(parts[1]).unwrap();
                println!("OK");
            }
            "incr" if parts.len() >= 3 => {
                if let Ok(amount) = parts[2].parse::<i64>() {
                    let res = store.incr(parts[1], amount).unwrap();
                    println!("\n{}", res);
                } else {
                    println!("Invalid amount");
                }
            }
            "decr" if parts.len() >= 3 => {
                if let Ok(amount) = parts[2].parse::<i64>() {
                    let res = store.decr(parts[1], amount).unwrap();
                    println!("\n{}", res);
                } else {
                    println!("Invalid amount");
                }
            }
            "scan" if parts.len() >= 2 => {
                let results = store.get_all_with_prefix(parts[1]);
                if results.is_empty() {
                    println!("(empty)");
                } else {
                    for (k, v) in results {
                        println!("\n{}: {}", k, v);
                    }
                }
            }
            "range" if parts.len() >= 3 => {
                let results = store.range(parts[1], parts[2]);
                if results.is_empty() {
                    println!("(empty)");
                } else {
                    for (k, v) in results {
                        println!("\n{}: {}", k, v);
                    }
                }
            }
            "keys" => {
                let keys: Vec<_> = store.cursor().map(|(k, _)| k).collect();
                if keys.is_empty() {
                    println!("(empty)");
                } else {
                    println!("\n{}", keys.join("\n"));
                }
            }
            "count" => {
                println!("\n{}", store.stats().key_count);
            }
            "stats" => {
                let s = store.stats();
                println!("\n--- Store Statistics ---");
                println!("Keys: {}", s.key_count);
                println!("Log Size: {} bytes", s.log_size);
                println!("Operations: {}", s.ops_count);
                println!("------------------------");
            }
            "flush" => {
                store.flush().unwrap();
                println!("OK");
            }
            "dump" if parts.len() >= 2 => {
                let path = parts[1];
                match store.get_all() {
                    Ok(data) => {
                        if let Ok(json) = serde_json::to_string_pretty(&data) {
                            if std::fs::write(path, json).is_ok() {
                                println!("OK");
                            } else {
                                println!("Error: Failed to write to file");
                            }
                        } else {
                            println!("Error: Serialization failed");
                        }
                    }
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "watch" if parts.len() >= 2 => {
                let key = parts[1].to_string();
                store.add_observer(&key, move |k, event| {
                    println!("\n[WATCH] Key '{}' changed: {:?}", k, event);
                    print!("kv> ");
                    io::stdout().flush().unwrap();
                });
                println!("OK watching {}", key);
            }
            "clear" => {
                store.clear().unwrap();
                println!("OK");
            }
            "version" => {
                println!("\nRustKV Store v0.1.0");
            }
            "help" => print_help(),
            "exit" | "quit" => break,
            _ => println!("Unknown command or wrong arguments. Type 'help' for usage"),
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
