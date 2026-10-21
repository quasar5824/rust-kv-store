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

    println!("\nScanning for keys starting with 'user:'");
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
    println!("  set-nx <key> <val>        - Set value only if key does not exist");
    println!("  mset <k1> <v1> <k2> <v2>...- Set multiple key-value pairs");
    println!("  get <key>                - Retrieve a value");
    println!("  mget <k1> <k2>...        - Retrieve multiple values");
    println!("  exists <key>             - Check whether a key exists");
    println!("  ttl <key>                - Get remaining TTL for a key");
    println!("  expire <key> <secs>      - Set TTL for an existing key");
    println!("  persist <key>            - Remove TTL from a key");
    println!("  get-or-set <key> <val>   - Get value, or set to <val> if missing");
    println!("  del <key>                - Delete a value");
    println!("  append <key> <val>       - Append value to an existing string");
    println!("  strlen <key>             - Get string length");
    println!("  getset <key> <val>       - Set value and return previous");
    println!("  setex <key> <sec> <val>  - Set value with expiration");
    println!("  rename <old> <new>       - Rename a key");
    println!("  mdelete <k1> <k2>...    - Delete multiple keys");
    println!("  update <key> <val>       - Update an existing key's value");
    println!("  incr <key> <amount>      - Increment a numeric value");
    println!("  decr <key> <amount>      - Decrement a numeric value");
    println!("  incr-float <key> <amt>   - Increment a floating-point value");
    println!("  decr-float <key> <amt>   - Decrement a floating-point value");
    println!("  scan <prefix>            - Scan keys starting with prefix");
    println!("  range <start> <end>     - Get keys in a range");
    println!("  keys                     - List all keys");
    println!("  count                    - Get total number of keys");
    println!("  stats                    - Show store statistics");
    println!("  info                     - Detailed store information");
    println!("  backup <file>            - Export store snapshot to a file");
    println!("  restore <file>           - Restore store from a snapshot file");
    println!("  flush                    - Force write-ahead log to disk");
    println!("  dump <file>              - Export store to JSON file");
    println!("  watch <key>              - Notify when the specified key changes");
    println!("  clear                    - Wipe all data from the store");
    println!("  version                  - Show store version");
    println!("  ping                     - Test store connectivity");
    println!("  help                     - Show this help message");
    println!("  exit | quit              - Exit the REPL");
}

fn same_store_path(candidate: &str, store_path: &str) -> bool {
    if candidate == store_path {
        return true;
    }
    match (std::fs::canonicalize(candidate), std::fs::canonicalize(store_path)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn run_repl(store: &mut KvStore, store_path: &str) {
    println!("Welcome to RustKV REPL!");
    println!("Type 'help' to see available commands.");
    
    loop {
        print!("kv> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => {
                println!();
                break;
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("Error reading input: {}", error);
                break;
            }
        }
        let parts: Vec<&str> = input.trim().split_whitespace().collect();

        if parts.is_empty() { continue; }

        match parts[0] {
            "set" if parts.len() >= 3 => {
                let key = parts[1];
                let mut ttl_parsed: Option<u64> = None;
                if parts.len() > 3 {
                    if let Ok(ttl) = parts.last().unwrap().parse::<u64>() {
                        ttl_parsed = Some(ttl);
                    }
                }
                if let Some(ttl) = ttl_parsed {
                    let val = parts[2..parts.len()-1].join(" ");
                    match store.set_with_ttl(key, &val, Some(ttl)) {
                        Ok(_) => println!("OK (ttl: {})", ttl),
                        Err(error) => println!("Error: {:?}", error),
                    }
                } else {
                    let val = parts[2..].join(" ");
                    match store.set(key, &val) {
                        Ok(_) => println!("OK"),
                        Err(error) => println!("Error: {:?}", error),
                    }
                }
            }
            "set-nx" if parts.len() >= 3 => {
                let key = parts[1];
                let val = parts[2..].join(" ");
                match store.set_if_not_exists(key, &val, None) {
                    Ok(true) => println!("OK"),
                    Ok(false) => println!("(nil)"),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "mset" if parts.len() >= 3 && parts.len() % 2 == 1 => {
                let mut pairs = Vec::new();
                for i in (1..parts.len()).step_by(2) {
                    pairs.push((parts[i].to_string(), parts[i+1].to_string()));
                }
                match store.mset(pairs) {
                    Ok(_) => println!("OK"),
                    Err(error) => println!("Error: {:?}", error),
                }
            }
            "get" if parts.len() >= 2 => {
                match store.get(parts[1]) {
                    Some(val) => println!("\n{}", val),
                    None => println!("(nil)"),
                }
            }
            "mget" if parts.len() >= 2 => {
                for key in &parts[1..] {
                    match store.get(key) {
                        Some(val) => println!("\n{}: {}", key, val),
                        None => println!("\n{}: (nil)", key),
                    }
                }
            }
            "exists" if parts.len() >= 2 => {
                println!("{}", store.exists(parts[1]));
            }
            "ttl" if parts.len() >= 2 => {
                match store.get_ttl(parts[1]) {
                    Some(Some(t)) => println!("\nTTL: {}s", t),
                    Some(None) => println!("\nNo TTL set"),
                    None => println!("(nil)"),
                }
            }
            "expire" if parts.len() >= 3 => {
                if let Ok(ttl) = parts[2].parse::<u64>() {
                    match store.expire(parts[1], Some(ttl)) {
                        Ok(true) => println!("OK"),
                        Ok(false) => println!("(nil)"),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Invalid ttl");
                }
            }
            "persist" if parts.len() >= 2 => {
                match store.expire(parts[1], None) {
                    Ok(true) => println!("OK"),
                    Ok(false) => println!("(nil)"),
                    Err(e) => println!("Error: {:?}", e),
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
                match store.delete(parts[1]) {
                    Ok(_) => println!("OK"),
                    Err(error) => println!("Error: {:?}", error),
                }
            }
            "append" if parts.len() >= 3 => {
                let key = parts[1];
                let value = parts[2..].join(" ");
                let current = store.get(key).cloned().unwrap_or_default();
                let new_value = format!("{}{}", current, value);
                let ttl = store.get_ttl(key);
                match store.set(key, &new_value) {
                    Ok(_) => {
                        if let Some(Some(ttl_seconds)) = ttl {
                            let _ = store.expire(key, Some(ttl_seconds));
                        }
                        println!("{}", new_value.len());
                    }
                    Err(error) => println!("Error: {:?}", error),
                }
            }
            "strlen" if parts.len() >= 2 => {
                match store.get(parts[1]) {
                    Some(v) => println!("{}", v.len()),
                    None => println!("0"),
                }
            }
            "getset" if parts.len() >= 3 => {
                let key = parts[1];
                let value = parts[2..].join(" ");
                let old = store.get(key).cloned();
                let ttl = store.get_ttl(key);
                match store.set(key, &value) {
                    Ok(_) => {
                        if let Some(Some(ttl_seconds)) = ttl {
                            let _ = store.expire(key, Some(ttl_seconds));
                        }
                        match old {
                            Some(previous) => println!("{}", previous),
                            None => println!("(nil)"),
                        }
                    }
                    Err(error) => println!("Error: {:?}", error),
                }
            }
            "setex" if parts.len() >= 4 => {
                if let Ok(ttl) = parts[2].parse::<u64>() {
                    let key = parts[1];
                    let value = parts[3..].join(" ");
                    match store.set_with_ttl(key, &value, Some(ttl)) {
                        Ok(_) => println!("OK"),
                        Err(error) => println!("Error: {:?}", error),
                    }
                } else {
                    println!("Invalid ttl");
                }
            }
            "rename" if parts.len() == 3 => {
                let old_key = parts[1];
                let new_key = parts[2];
                if old_key == new_key {
                    if store.exists(old_key) {
                        println!("OK");
                    } else {
                        println!("(nil)");
                    }
                    continue;
                }
                match store.get(old_key).cloned() {
                    Some(value) => {
                        let ttl = store.get_ttl(old_key);
                        match store.set(new_key, &value) {
                            Ok(_) => {
                                if let Some(Some(ttl_seconds)) = ttl {
                                    let _ = store.expire(new_key, Some(ttl_seconds));
                                }
                                match store.delete(old_key) {
                                    Ok(_) => println!("OK"),
                                    Err(error) => println!("Error: {:?}", error),
                                }
                            }
                            Err(error) => println!("Error: {:?}", error),
                        }
                    }
                    None => println!("(nil)"),
                }
            }
            "mdelete" if parts.len() >= 2 => {
                let mut failed = false;
                for key in &parts[1..] {
                    if let Err(error) = store.delete(key) {
                        println!("Error: {:?}", error);
                        failed = true;
                    }
                }
                if !failed {
                    println!("OK");
                }
            }
            "update" if parts.len() >= 3 => {
                let key = parts[1];
                let val = parts[2..].join(" ");
                match store.update(key, &val) {
                    Ok(true) => println!("OK"),
                    Ok(false) => println!("(nil)"),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "incr" if parts.len() >= 3 => {
                if let Ok(amount) = parts[2].parse::<i64>() {
                    match store.incr(parts[1], amount) {
                        Ok(result) => println!("\n{}", result),
                        Err(error) => println!("Error: {:?}", error),
                    }
                } else {
                    println!("Invalid amount");
                }
            }
            "decr" if parts.len() >= 3 => {
                if let Ok(amount) = parts[2].parse::<i64>() {
                    match store.decr(parts[1], amount) {
                        Ok(result) => println!("\n{}", result),
                        Err(error) => println!("Error: {:?}", error),
                    }
                } else {
                    println!("Invalid amount");
                }
            }
            "incr-float" if parts.len() >= 3 => {
                if let Ok(amount) = parts[2].parse::<f64>() {
                    match store.incr_float(parts[1], amount) {
                        Ok(result) => println!("\n{}", result),
                        Err(error) => println!("Error: {:?}", error),
                    }
                } else {
                    println!("Invalid amount");
                }
            }
            "decr-float" if parts.len() >= 3 => {
                if let Ok(amount) = parts[2].parse::<f64>() {
                    match store.incr_float(parts[1], -amount) {
                        Ok(result) => println!("\n{}", result),
                        Err(error) => println!("Error: {:?}", error),
                    }
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
            "info" => {
                let s = store.stats();
                println!("\n--- RustKV Store Info ---");
                println!("Version: 0.1.0");
                println!("Role: Persistent Key-Value Store");
                println!("Implementation: Write-Ahead Log (WAL)");
                println!("\n[Current State]");
                println!("  Total Keys: {}", s.key_count);
                println!("  Log File Size: {} bytes", s.log_size);
                println!("  Total Ops Since Start: {}", s.ops_count);
                println!("--------------------------");
            }
            "flush" => {
                match store.flush() {
                    Ok(_) => println!("OK"),
                    Err(error) => println!("Error: {:?}", error),
                }
            }
            "backup" if parts.len() >= 2 => {
                if same_store_path(parts[1], store_path) {
                    println!("Error: backup path must not be the open store file");
                } else {
                    match store.backup(parts[1]) {
                        Ok(_) => println!("OK"),
                        Err(error) => println!("Error: {:?}", error),
                    }
                }
            }
            "restore" if parts.len() >= 2 => {
                if same_store_path(parts[1], store_path) {
                    println!("Error: restore path must not be the open store file");
                } else {
                    match store.restore(parts[1]) {
                        Ok(_) => println!("OK"),
                        Err(error) => println!("Error: {:?}", error),
                    }
                }
            }
            "dump" if parts.len() >= 2 => {
                let path = parts[1];
                if same_store_path(path, store_path) {
                    println!("Error: dump path must not be the open store file");
                } else {
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
                        Err(error) => println!("Error: {:?}", error),
                    }
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
                match store.clear() {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error clearing store: {:?}", e),
                }
            }
            "version" => {
                println!("\nRustKV Store v0.1.0");
            }
            "ping" => {
                println!("PONG");
            }
            "help" => print_help(),
            "exit" | "quit" => break,
            _ => println!("Unknown command or wrong arguments. Type 'help' for usage"),
        }
    }
}

fn print_usage() {
    println!("Usage: cargo run [--bench | --demo | --repl [path] | --help]");
    println!("  (default: --demo)");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "--bench" => {
                let mut store = KvStore::open("demo.db").expect("Failed to open store");
                run_benchmarks(&mut store);
            }
            "--demo" => {
                let mut store = KvStore::open("demo.db").expect("Failed to open store");
                run_demo(&mut store);
                run_benchmarks(&mut store);
            }
            "--repl" => {
                let path = args.get(2).cloned().unwrap_or_else(|| "store.db".to_string());
                let store_path = path.clone();
                let mut store = KvStore::open(&path).expect("Failed to open store");
                run_repl(&mut store, &store_path);
            }
            "--help" | "-h" => print_usage(),
            _ => {
                println!("Unknown option: {}", args[1]);
                print_usage();
            }
        }
    } else {
        let mut store = KvStore::open("demo.db").expect("Failed to open store");
        run_demo(&mut store);
        run_benchmarks(&mut store);
    }
}
