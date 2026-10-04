use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions, rename, metadata};
use std::io::{self, BufRead, BufReader, Write};
use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};
use crc32fast::Hasher;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Set { key: String, value: String, ttl: Option<u64> },
    SetAt { key: String, value: String, expires_at: SystemTime },
    Delete { key: String },
    Incr { key: String, delta: i64 },
    IncrFloat { key: String, delta: f64 },
    Expire { key: String, ttl: Option<u64> },
    ExpireAt { key: String, expires_at: SystemTime },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogEntry {
    checksum: u32,
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreEvent {
    Set,
    Delete,
    Expire,
}

#[derive(Debug)]
pub struct StoreStats {
    pub key_count: usize,
    pub ops_count: usize,
    pub corrupted_entries: usize,
}

#[derive(Debug)]
pub struct StoreHealth {
    pub log_size_bytes: u64,
    pub fragmentation_ratio: f64,
    pub is_healthy: bool,
    pub suggested_action: Option<String>,
}

struct StoreValue {
    value: String,
    expires_at: Option<SystemTime>,
}

pub struct KvStore {
    data: HashMap<String, StoreValue>,
    sorted_keys: Vec<String>,
    log: File,
    path: String,
    ops_count: usize,
    corrupted_entries: usize,
    observers: HashMap<String, Vec<Box<dyn Fn(&str, StoreEvent) + Send + Sync>>>,
}

impl KvStore {
    pub fn open(path: &str) -> io::Result<Self> {
        let mut data = HashMap::new();
        let mut ops_count = 0;
        let mut corrupted_entries = 0;
        
        let file = File::open(path);
        if let Ok(f) = file {
            let reader = BufReader::new(f);
            for line in reader.lines() {
                let line = line?;
                if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                    if Self::verify_checksum(&entry) {
                        ops_count += 1;
                        match entry.command {
                            Command::Set { key, value, ttl } => {
                                let expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                                data.insert(key, StoreValue { value, expires_at });
                            }
                            Command::SetAt { key, value, expires_at } => {
                                data.insert(key, StoreValue { value, expires_at: Some(expires_at) });
                            }
                            Command::Delete { key } => {
                                data.remove(&key);
                            }
                            Command::Incr { key, delta } => {
                                let current_val = data.get(&key).map(|sv| sv.value.parse::<i64>().unwrap_or(0)).unwrap_or(0);
                                let new_val = current_val + delta;
                                data.insert(key, StoreValue { value: new_val.to_string(), expires_at: None });
                            }
                            Command::IncrFloat { key, delta } => {
                                let current_val = data.get(&key).map(|sv| sv.value.parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
                                let new_val = current_val + delta;
                                data.insert(key, StoreValue { value: new_val.to_string(), expires_at: None });
                            }
                            Command::Expire { key, ttl } => {
                                if let Some(sv) = data.get_mut(&key) {
                                    sv.expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                                }
                            }
                            Command::ExpireAt { key, expires_at } => {
                                if let Some(sv) = data.get_mut(&key) {
                                    sv.expires_at = Some(expires_at);
                                }
                            }
                        }
                    } else {
                        corrupted_entries += 1;
                    }
                } else {
                    corrupted_entries += 1;
                }
            }
        }

        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let mut store = KvStore {
            data,
            sorted_keys: Vec::new(),
            log,
            path: path.to_string(),
            ops_count,
            corrupted_entries,
            observers: HashMap::new(),
        };
        store.rebuild_index();
        Ok(store)
    }

    fn verify_checksum(entry: &LogEntry) -> bool {
        let cmd_json = serde_json::to_string(&entry.command).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(cmd_json.as_bytes());
        hasher.finalize() == entry.checksum
    }

    fn compute_checksum(cmd: &Command) -> u32 {
        let cmd_json = serde_json::to_string(cmd).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(cmd_json.as_bytes());
        hasher.finalize()
    }

    pub fn add_observer<F>(&mut self, key: &str, callback: F)
    where
        F: Fn(&str, StoreEvent) + Send + Sync + 'static,
    {
        self.observers.entry(key.to_string()).or_default().push(Box::new(callback));
    }

    fn notify(&self, key: &str, event: StoreEvent) {
        if let Some(callbacks) = self.observers.get(key) {
            for cb in callbacks {
                cb(key, event);
            }
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.set_with_ttl(key, value, None)
    }

    pub fn set_with_ttl(&mut self, key: &str, value: &str, ttl: Option<u64>) -> io::Result<()> {
        self.batch(vec![Command::Set { 
            key: key.to_string(), 
            value: value.to_string(),
            ttl
        }])
    }

    pub fn set_at(&mut self, key: &str, value: &str, expires_at: SystemTime) -> io::Result<()> {
        self.batch(vec![Command::SetAt { 
            key: key.to_string(), 
            value: value.to_string(),
            expires_at
        }])
    }

    pub fn set_if_not_exists(&mut self, key: &str, value: &str, ttl: Option<u64>) -> io::Result<bool> {
        if self.exists(key) {
            return Ok(false);
        }
        self.set_with_ttl(key, value, ttl)?;
        Ok(true)
    }

    pub fn get_or_set(&mut self, key: &str, value: &str, ttl: Option<u64>) -> io::Result<String> {
        if let Some(existing) = self.get(key) {
            return Ok(existing.clone());
        }
        self.set_with_ttl(key, value, ttl)?;
        Ok(value.to_string())
    }

    pub fn update(&mut self, key: &str, value: &str) -> io::Result<bool> {
        if !self.exists(key) {
            return Ok(false);
        }
        self.set(key, value)?;
        Ok(true)
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        self.batch(vec![Command::Delete { key: key.to_string() }])
    }

    pub fn mdelete(&mut self, keys: &[&str]) -> io::Result<usize> {
        let mut deleted_count = 0;
        let mut commands = Vec::new();

        for &key in keys {
            if self.exists(key) {
                deleted_count += 1;
                commands.push(Command::Delete { key: key.to_string() });
            }
        }

        if !commands.is_empty() {
            self.batch(commands)?;
        }

        Ok(deleted_count)
    }

    pub fn rename(&mut self, old_key: &str, new_key: &str) -> io::Result<bool> {
        if !self.exists(old_key) {
            return Ok(false);
        }

        let value = self.get(old_key).unwrap().clone();
        let mut commands = Vec::with_capacity(2);
        commands.push(Command::Set { 
            key: new_key.to_string(), 
            value, 
            ttl: None 
        });
        commands.push(Command::Delete { 
            key: old_key.to_string() 
        });

        self.batch(commands)?;
        Ok(true)
    }

    pub fn bulk_import(&mut self, pairs: &[(&str, &str)]) -> io::Result<()> {
        let commands = pairs
            .iter()
            .map(|(k, v)| Command::Set { key: k.to_string(), value: v.to_string(), ttl: None })
            .collect();
        self.batch(commands)
    }

    pub fn mset(&mut self, pairs: Vec<(String, String)>) -> io::Result<()> {
        let commands = pairs
            .into_iter()
            .map(|(key, value)| Command::Set { key, value, ttl: None })
            .collect();
        self.batch(commands)
    }

    pub fn batch(&mut self, commands: Vec<Command>) -> io::Result<()> {
        let mut buffer = Vec::new();
        for cmd in &commands {
            let entry = LogEntry {
                checksum: Self::compute_checksum(cmd),
                command: cmd.clone(),
            };
            let serialized = serde_json::to_string(&entry).unwrap();
            buffer.extend_from_slice(serialized.as_bytes());
            buffer.extend_from_slice(b"\n");
        }
        
        self.log.write_all(&buffer)?;
        self.log.flush()?;

        let mut index_changed = false;
        for cmd in commands {
            self.ops_count += 1;
            match cmd {
                Command::Set { key, value, ttl } => {
                    let expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                    if !self.data.contains_key(&key) {
                        index_changed = true;
                    }
                    self.data.insert(key.clone(), StoreValue { value, expires_at });
                    self.notify(&key, StoreEvent::Set);
                }
                Command::SetAt { key, value, expires_at } => {
                    if !self.data.contains_key(&key) {
                        index_changed = true;
                    }
                    self.data.insert(key.clone(), StoreValue { value, expires_at: Some(expires_at) });
                    self.notify(&key, StoreEvent::Set);
                }
                Command::Delete { key } => {
                    if self.data.remove(&key).is_some() {
                        index_changed = true;
                    }
                    self.notify(&key, StoreEvent::Delete);
                }
                Command::Incr { key, delta } => {
                    let current_val = self.data.get(&key).map(|sv| sv.value.parse::<i64>().unwrap_or(0)).unwrap_or(0);
                    let new_val = current_val + delta;
                    if !self.data.contains_key(&key) {
                        index_changed = true;
                    }
                    self.data.insert(key.clone(), StoreValue { value: new_val.to_string(), expires_at: None });
                    self.notify(&key, StoreEvent::Set);
                }
                Command::IncrFloat { key, delta } => {
                    let current_val = self.data.get(&key).map(|sv| sv.value.parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
                    let new_val = current_val + delta;
                    if !self.data.contains_key(&key) {
                        index_changed = true;
                    }
                    self.data.insert(key.clone(), StoreValue { value: new_val.to_string(), expires_at: None });
                    self.notify(&key, StoreEvent::Set);
                }
                Command::Expire { key, ttl } => {
                    if let Some(sv) = self.data.get_mut(&key) {
                        sv.expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                        self.notify(&key, StoreEvent::Expire);
                    }
                }
                Command::ExpireAt { key, expires_at } => {
                    if let Some(sv) = self.data.get_mut(&key) {
                        sv.expires_at = Some(expires_at);
                        self.notify(&key, StoreEvent::Expire);
                    }
                }
            }
        }
        
        if index_changed {
            self.rebuild_index();
        }
        Ok(())
    }

    pub fn incr(&mut self, key: &str, delta: i64) -> io::Result<i64> {
        let current_val = self.data.get(key).map(|sv| sv.value.parse::<i64>().unwrap_or(0)).unwrap_or(0);
        let new_val = current_val + delta;
        self.batch(vec![Command::Incr { key: key.to_string(), delta }])?;
        Ok(new_val)
    }

    pub fn decr(&mut self, key: &str, delta: i64) -> io::Result<i64> {
        self.incr(key, -delta)
    }

    pub fn incr_float(&mut self, key: &str, delta: f64) -> io::Result<f64> {
        let current_val = self.data.get(key).map(|sv| sv.value.parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
        let new_val = current_val + delta;
        self.batch(vec![Command::IncrFloat { key: key.to_string(), delta }])?;
        Ok(new_val)
    }

    pub fn transaction(&mut self, f: impl FnOnce(&mut Transaction) -> bool) -> io::Result<bool> {
        let mut tx = Transaction {
            pending: Vec::new(),
        };
        
        if f(&mut tx) {
            let cmds = std::mem::take(&mut tx.pending);
            self.batch(cmds)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&String> {
        if let Some(sv) = self.data.get(key) {
            if let Some(expiry) = sv.expires_at {
                if SystemTime::now() > expiry {
                    self.data.remove(key);
                    self.sorted_keys.retain(|k| k != key);
                    self.notify(key, StoreEvent::Delete);
                    return None;
                }
            }
            return Some(&sv.value);
        }
        None
    }

    pub fn get_if_exists(&mut self, key: &str) -> Option<&String> {
        self.get(key)
    }

    pub fn get_ttl(&mut self, key: &str) -> Option<Option<u64>> {
        if let Some(sv) = self.data.get(key) {
            if let Some(expiry) = sv.expires_at {
                let now = SystemTime::now();
                if now > expiry {
                    self.data.remove(key);
                    self.sorted_keys.retain(|k| k != key);
                    self.notify(key, StoreEvent::Delete);
                    return None;
                }
                return Some(Some(expiry.duration_since(now).unwrap().as_secs()));
            }
            return Some(None);
        }
        None
    }

    pub fn get_many(&mut self, keys: &[&str]) -> Vec<(String, String)> {
        keys.iter()
            .filter_map(|&k| self.get(k).map(|v| (k.to_string(), v.clone())))
            .collect()
    }

    pub fn mget(&mut self, keys: &[&str]) -> Vec<Option<String>> {
        keys.iter().map(|&k| self.get(k).cloned()).collect()
    }

    pub fn get_with_default(&mut self, key: &str, default: &str) -> String {
        self.get(key).cloned().unwrap_or_else(|| default.to_string())
    }

    pub fn exists(&mut self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn mexists(&mut self, keys: &[&str]) -> Vec<bool> {
        keys.iter().map(|&k| self.exists(k)).collect()
    }

    pub fn keys(&mut self) -> Vec<String> {
        let current_keys: Vec<String> = self.data.keys().cloned().collect();
        let mut active_keys = Vec::with_capacity(current_keys.len());
        for k in current_keys {
            if self.exists(&k) {
                active_keys.push(k);
            }
        }
        active_keys
    }

    pub fn get_all_cloned(&mut self) -> Vec<(String, String)> {
        let keys = self.keys();
        let mut results = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(v) = self.get(&k) {
                results.push((k, v.clone()));
            }
        }
        results
    }

    pub fn get_all_with_ttl(&mut self) -> Vec<(String, String, Option<u64>)> {
        let keys = self.keys();
        let mut results = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(v) = self.get(&k) {
                let ttl = self.get_ttl(&k).unwrap_or(None);
                results.push((k, v.clone(), ttl));
            }
        }
        results
    }

    pub fn scan(&mut self, prefix: &str) -> Vec<(String, String)> {
        let mut results = Vec::new();
        let start_idx = self.sorted_keys.binary_search_by(|k| k.cmp(prefix)).unwrap_or_else(|e| e);
        
        for &k in &self.sorted_keys[start_idx..] {
            if !k.starts_with(prefix) {
                break;
            }
            if let Some(v) = self.get(&k) {
                results.push((k.to_string(), v.clone()));
            }
        }
        results
    }

    pub fn get_all_with_prefix(&mut self, prefix: &str) -> Vec<(String, String)> {
        self.scan(prefix)
    }

    pub fn range(&mut self, start: &str, end: &str) -> Vec<(String, String)> {
        let mut results = Vec::new();
        let start_idx = self.sorted_keys.binary_search_by(|k| k.cmp(start)).unwrap_or_else(|e| e);
        
        for &k in &self.sorted_keys[start_idx..] {
            if k > end {
                break;
            }
            if let Some(v) = self.get(&k) {
                results.push((k.to_string(), v.clone()));
            }
        }
        results
    }

    pub fn cursor(&mut self) -> StoreCursor {
        StoreCursor {
            iter: self.data.iter(),
        }
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.data.clear();
        self.sorted_keys.clear();
        self.ops_count = 0;
        self.corrupted_entries = 0;
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.log = file;
        Ok(())
    }

    pub fn clear_expired(&mut self) -> io::Result<usize> {
        let now = SystemTime::now();
        let expired_keys: Vec<String> = self.data
            .iter()
            .filter(|(_, sv)| sv.expires_at.map_or(false, |exp| now > exp))
            .map(|(k, _)| k.clone())
            .collect();

        let count = expired_keys.len();
        for k in expired_keys {
            self.data.remove(&k);
            self.sorted_keys.retain(|key| key != &k);
            self.notify(&k, StoreEvent::Delete);
        }

        if count > 0 {
            self.compact()?;
        }

        Ok(count)
    }

    pub fn compact(&mut self) -> io::Result<()> {
        let compact_path = format!("{}.compact", self.path);
        let mut new_log = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&compact_path)?;

        let keys = self.keys();
        for k in keys {
            if let Some(sv) = self.data.get(&k) {
                let ttl = sv.expires_at.and_then(|expiry| {
                    let now = SystemTime::now();
                    if expiry > now {
                        Some(expiry.duration_since(now).unwrap().as_secs())
                    } else {
                        None
                    }
                });

                let cmd = Command::Set {
                    key: k.clone(),
                    value: sv.value.clone(),
                    ttl,
                };
                let entry = LogEntry {
                    checksum: Self::compute_checksum(&cmd),
                    command: cmd,
                };
                let serialized = serde_json::to_string(&entry).unwrap();
                new_log.write_all(serialized.as_bytes())?;
                new_log.write_all(b"\n")?;
            }
        }
        new_log.flush()?;
        
        rename(&compact_path, &self.path)?;
        
        let log = OpenOptions::new()
            .append(true)
            .open(&self.path)?;
        
        self.log = log;
        self.ops_count = self.data.len();
        self.corrupted_entries = 0;
        self.rebuild_index();
        Ok(())
    }

    pub fn stats(&mut self) -> StoreStats {
        let keys = self.keys();
        StoreStats {
            key_count: keys.len(),
            ops_count: self.ops_count,
            corrupted_entries: self.corrupted_entries,
        }
    }

    pub fn health_check(&self) -> io::Result<StoreHealth> {
        let meta = metadata(&self.path)?;
        let log_size_bytes = meta.len();
        
        let live_keys = self.data.len();
        let fragmentation_ratio = if self.ops_count > 0 {
            1.0 - (live_keys as f64 / self.ops_count as f64)
        } else {
            0.0
        };

        let mut is_healthy = true;
        let mut suggested_action = None;

        if fragmentation_ratio > 0.5 {
            is_healthy = false;
            suggested_action = Some("Run compact() to reduce log size".to_string());
        } else if self.corrupted_entries > 0 {
            is_healthy = false;
            suggested_action = Some("Run validate_integrity() to check for corruption".to_string());
        }

        Ok(StoreHealth {
            log_size_bytes,
            fragmentation_ratio,
            is_healthy,
            suggested_action,
        })
    }

    pub fn backup(&mut self, backup_path: &str) -> io::Result<()> {
        self.snapshot(backup_path)
    }

    pub fn snapshot(&mut self, snapshot_path: &str) -> io::Result<()> {
        let mut file = File::create(snapshot_path)?;
        let keys = self.keys();
        for k in keys {
            if let Some(sv) = self.data.get(&k) {
                let ttl = sv.expires_at.and_then(|expiry| {
                    let now = SystemTime::now();
                    if expiry > now {
                        Some(expiry.duration_since(now).unwrap().as_secs())
                    } else {
                        None
                    }
                });
                let cmd = Command::Set {
                    key: k,
                    value: sv.value.clone(),
                    ttl,
                };
                let entry = LogEntry {
                    checksum: Self::compute_checksum(&cmd),
                    command: cmd,
                };
                let serialized = serde_json::to_string(&entry).unwrap();
                file.write_all(serialized.as_bytes())?;
                file.write_all(b"\n")?;
            }
        }
        file.flush()
    }

    pub fn restore(&mut self, backup_path: &str) -> io::Result<()> {
        let file = File::open(backup_path)?;
        let reader = BufReader::new(file);
        let mut commands = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                if Self::verify_checksum(&entry) {
                    commands.push(entry.command);
                }
            }
        }

        self.clear()?;
        self.batch(commands)
    }

    pub fn expire(&mut self, key: &str, ttl: Option<u64>) -> io::Result<bool> {
        if !self.exists(key) {
            return Ok(false);
        }
        self.batch(vec![Command::Expire { 
            key: key.to_string(), 
            ttl 
        }])?;
        Ok(true)
    }

    pub fn expire_at(&mut self, key: &str, expires_at: SystemTime) -> io::Result<bool> {
        if !self.exists(key) {
            return Ok(false);
        }
        self.batch(vec![Command::ExpireAt { 
            key: key.to_string(), 
            expires_at 
        }])?;
        Ok(true)
    }

    pub fn persist(&mut self) -> io::Result<()> {
        self.log.sync_all()
    }

    pub fn validate_integrity(&mut self) -> io::Result<Vec<String>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut expected_state: HashMap<String, String> = HashMap::new();
        let mut corrupted_keys = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                if Self::verify_checksum(&entry) {
                    match entry.command {
                        Command::Set { key, value, .. } => {
                            expected_state.insert(key, value);
                        }
                        Command::SetAt { key, value, .. } => {
                            expected_state.insert(key, value);
                        }
                        Command::Delete { key } => {
                            expected_state.remove(&key);
                        }
                        Command::Incr { key, delta } => {
                            let current = expected_state.get(&key).map(|v| v.parse::<i64>().unwrap_or(0)).unwrap_or(0);
                            expected_state.insert(key, (current + delta).to_string());
                        }
                        Command::IncrFloat { key, delta } => {
                            let current = expected_state.get(&key).map(|v| v.parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
                            expected_state.insert(key, (current + delta).to_string());
                        }
                        Command::Expire { .. } | Command::ExpireAt { .. } =>{}
                    }
                }
            }
        }

        for (k, v) in &expected_state {
            if self.get(k) != Some(v) {
                corrupted_keys.push(k.clone());
            }
        }

        for k in self.data.keys() {
            if !expected_state.contains_key(k) {
                corrupted_keys.push(k.clone());
            }
        }

        Ok(corrupted_keys)
    }

    pub fn validate_consistency(&mut self) -> io::Result<bool> {
        let corrupted = self.validate_integrity()?;
        Ok(corrupted.is_empty())
    }

    pub fn validate_log_checksums(&self) -> io::Result<usize> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut corrupted_count = 0;

        for line in reader.lines() {
            let line = line?;
            if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                if !Self::verify_checksum(&entry) {
                    corrupted_count += 1;
                }
            } else {
                corrupted_count += 1;
            }
        }
        Ok(corrupted_count)
    }

    fn rebuild_index(&mut self) {
        self.sorted_keys = self.data.keys().cloned().collect();
        self.sorted_keys.sort();
    }
}

pub struct Transaction {
    pending: Vec<Command>,
}

impl Transaction {
    pub fn set(&mut self, key: String, value: String) {
        self.set_with_ttl(key, value, None);
    }

    pub fn set_with_ttl(&mut self, key: String, value: String, ttl: Option<u64>) {
        self.pending.push(Command::Set { key, value, ttl });
    }

    pub fn set_at(&mut self, key: String, value: String, expires_at: SystemTime) {
        self.pending.push(Command::SetAt { key, value, expires_at });
    }

    pub fn delete(&mut self, key: String) {
        self.pending.push(Command::Delete { key });
    }

    pub fn incr(&mut self, key: String, delta: i64) {
        self.pending.push(Command::Incr { key, delta });
    }

    pub fn incr_float(&mut self, key: String, delta: f64) {
        self.pending.push(Command::IncrFloat { key, delta });
    }

    pub fn expire(&mut self, key: String, ttl: Option<u64>) {
        self.pending.push(Command::Expire { key, ttl });
    }

    pub fn expire_at(&mut self, key: String, expires_at: SystemTime) {
        self.pending.push(Command::ExpireAt { key, expires_at });
    }
}

pub struct ConcurrentKvStore {
    inner: Arc<RwLock<KvStore>>,
}

impl ConcurrentKvStore {
    pub fn open(path: &str) -> io::Result<Self> {
        let store = KvStore::open(path)?;
        Ok(ConcurrentKvStore {
            inner: Arc::new(RwLock::new(store)),
        })
    }

    pub fn set(&self, key: &str, value: &str) -> io::Result<()> {
        let mut store = self.inner.write().unwrap();
        store.set(key, value)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut store = self.inner.write().unwrap();
        store.get(key).cloned()
    }

    pub fn exists(&self, key: &str) -> bool {
        let mut store = self.inner.write().unwrap();
        store.exists(key)
    }

    pub fn delete(&self, key: &str) -> io::Result<()> {
        let mut store = self.inner.write().unwrap();
        store.delete(key)
    }

    pub fn stats(&self) -> StoreStats {
        let mut store = self.inner.write().unwrap();
        store.stats()
    }

    pub fn clear_expired(&self) -> io::Result<usize> {
        let mut store = self.inner.write().unwrap();
        store.clear_expired()
    }
}

pub struct AsyncKvStore {
    inner: Arc<RwLock<KvStore>>,
}

impl AsyncKvStore {
    pub async fn open(path: String) -> io::Result<Self> {
        let store = tokio::task::spawn_blocking(move || KvStore::open(&path))
            .await
            .expect("Panic in spawn_blocking")?;
        
        let inner = Arc::new(RwLock::new(store));
        
        let inner_clone = Arc::clone(&inner);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let inner_task = Arc::clone(&inner_clone);
                let _ = tokio::task::spawn_blocking(move || {
                    let mut store = inner_task.write().unwrap();
                    let _ = store.clear_expired();
                }).await;
            }
        });

        Ok(AsyncKvStore {
            inner,
        })
    }

    pub async fn set(&self, key: String, value: String) -> io::Result<()> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut store = inner.write().unwrap();
            store.set(&key, &value)
        }).await.expect("Panic in spawn_blocking")
    }

    pub async fn get(&self, key: String) -> Option<String> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut store = inner.write().unwrap();
            store.get(&key).cloned()
        }).await.expect("Panic in spawn_blocking")
    }

    pub async fn clear_expired(&self) -> io::Result<usize> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut store = inner.write().unwrap();
            store.clear_expired()
        }).await.expect("Panic in spawn_blocking")
    }
}

#[derive(Debug, Clone, Copy)]
struct CacheEntry {
    value: String,
}

pub struct CachedKvStore {
    store: KvStore,
    cache: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    capacity: usize,
    hits: u64,
    misses: u64,
}

impl CachedKvStore {
    pub fn new(store: KvStore, capacity: usize) -> Self {
        Self {
            store,
            cache: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if self.cache.contains_key(key) {
            self.hits += 1;
            self.touch(key);
            return Some(self.cache.get(key).unwrap().value.clone());
        }

        self.misses += 1;
        if let Some(val) = self.store.get(key) {
            let val_cloned = val.clone();
            self.put_cache(key.to_string(), val_cloned.clone());
            return Some(val_cloned);
        }
        None
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.store.set(key, value)?;
        self.put_cache(key.to_string(), value.to_string());
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        self.store.delete(key)?;
        self.cache.remove(key);
        self.order.retain(|k| k != key);
        Ok(())
    }

    pub fn cache_stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    fn touch(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
            self.order.push_back(key.to_string());
        }
    }

    fn put_cache(&mut self, key: String, value: String) {
        if self.cache.contains_key(&key) {
            self.touch(&key);
            self.cache.insert(key, CacheEntry { value });
        } else {
            if self.cache.len() >= self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.cache.remove(&oldest);
                }
            }
            self.order.push_back(key.clone());
            self.cache.insert(key, CacheEntry { value });
        }
    }
}

pub struct StoreCursor<'a> {
    iter: std::collections::hash_map::Iter<'a, String, StoreValue>,
}

impl<'a> Iterator for StoreCursor<'a> {
    type Item = (&'a String, &'a String);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k, &v.value))
    }
}
