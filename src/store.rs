use std::collections::HashMap;
use std::fs::{File, OpenOptions, rename};
use std::io::{self, BufRead, BufReader, Write};
use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Set { key: String, value: String, ttl: Option<u64> },
    Delete { key: String },
    Incr { key: String, delta: i64 },
    Expire { key: String, ttl: Option<u64> },
}

#[derive(Debug)]
pub struct StoreStats {
    pub key_count: usize,
    pub ops_count: usize,
}

struct StoreValue {
    value: String,
    expires_at: Option<SystemTime>,
}

pub struct KvStore {
    data: HashMap<String, StoreValue>,
    log: File,
    path: String,
    ops_count: usize,
}

impl KvStore {
    pub fn open(path: &str) -> io::Result<Self> {
        let mut data = HashMap::new();
        let mut ops_count = 0;
        
        let file = File::open(path);
        if let Ok(f) = file {
            let reader = BufReader::new(f);
            for line in reader.lines() {
                let line = line?;
                if let Ok(cmd) = serde_json::from_str::<Command>(&line) {
                    ops_count += 1;
                    match cmd {
                        Command::Set { key, value, ttl } => {
                            let expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                            data.insert(key, StoreValue { value, expires_at });
                        }
                        Command::Delete { key } => {
                            data.remove(&key);
                        }
                        Command::Incr { key, delta } => {
                            let current_val = data.get(&key).map(|sv| sv.value.parse::<i64>().unwrap_or(0)).unwrap_or(0);
                            let new_val = current_val + delta;
                            data.insert(key, StoreValue { value: new_val.to_string(), expires_at: None });
                        }
                        Command::Expire { key, ttl } => {
                            if let Some(sv) = data.get_mut(&key) {
                                sv.expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                            }
                        }
                    }
                }
            }
        }

        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(KvStore {
            data,
            log,
            path: path.to_string(),
            ops_count,
        })
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
            let serialized = serde_json::to_string(cmd).unwrap();
            buffer.extend_from_slice(serialized.as_bytes());
            buffer.extend_from_slice(b"\n");
        }
        
        self.log.write_all(&buffer)?;
        self.log.flush()?;

        for cmd in commands {
            self.ops_count += 1;
            match cmd {
                Command::Set { key, value, ttl } => {
                    let expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                    self.data.insert(key, StoreValue { value, expires_at });
                }
                Command::Delete { key } => {
                    self.data.remove(&key);
                }
                Command::Incr { key, delta } => {
                    let current_val = self.data.get(&key).map(|sv| sv.value.parse::<i64>().unwrap_or(0)).unwrap_or(0);
                    let new_val = current_val + delta;
                    self.data.insert(key, StoreValue { value: new_val.to_string(), expires_at: None });
                }
                Command::Expire { key, ttl } => {
                    if let Some(sv) = self.data.get_mut(&key) {
                        sv.expires_at = ttl.map(|secs| SystemTime::now() + Duration::from_secs(secs));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn incr(&mut self, key: &str, delta: i64) -> io::Result<i64> {
        let current_val = self.data.get(key).map(|sv| sv.value.parse::<i64>().unwrap_or(0)).unwrap_or(0);
        let new_val = current_val + delta;
        self.batch(vec![Command::Incr { key: key.to_string(), delta }])?;
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

    pub fn get_all_cloned(&mut self) -> Vec<(String, String)> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        let mut results = Vec::new();
        for k in keys {
            if let Some(v) = self.get(&k) {
                results.push((k, v.clone()));
            }
        }
        results
    }

    pub fn get_all_with_ttl(&mut self) -> Vec<(String, String, Option<u64>)> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        let mut results = Vec::new();
        for k in keys {
            if let Some(v) = self.get(&k) {
                let ttl = self.get_ttl(&k).unwrap_or(None);
                results.push((k, v.clone(), ttl));
            }
        }
        results
    }

    pub fn scan(&mut self, prefix: &str) -> Vec<(String, String)> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        let mut results = Vec::new();
        for k in keys {
            if k.starts_with(prefix) {
                if let Some(v) = self.get(&k) {
                    results.push((k, v.clone()));
                }
            }
        }
        results
    }

    pub fn get_all_with_prefix(&mut self, prefix: &str) -> Vec<(String, String)> {
        self.scan(prefix)
    }

    pub fn range(&mut self, start: &str, end: &str) -> Vec<(String, String)> {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        let mut results = Vec::new();
        for k in keys {
            if k >= start && k <= end {
                if let Some(v) = self.get(&k) {
                    results.push((k, v.clone()));
                }
            }
        }
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    pub fn cursor(&mut self) -> Vec<(String, String)> {
        self.get_all_cloned()
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.data.clear();
        self.ops_count = 0;
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

        let keys: Vec<String> = self.data.keys().cloned().collect();
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
                let serialized = serde_json::to_string(&cmd).unwrap();
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
        Ok(())
    }

    pub fn stats(&mut self) -> StoreStats {
        let keys: Vec<String> = self.data.keys().cloned().collect();
        for k in keys {
            self.exists(&k);
        }
        StoreStats {
            key_count: self.data.len(),
            ops_count: self.ops_count,
        }
    }

    pub fn backup(&mut self, backup_path: &str) -> io::Result<()> {
        let mut file = File::create(backup_path)?;
        let keys: Vec<String> = self.data.keys().cloned().collect();
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
                let serialized = serde_json::to_string(&cmd).unwrap();
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
            if let Ok(cmd) = serde_json::from_str::<Command>(&line) {
                commands.push(cmd);
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

    pub fn persist(&mut self) -> io::Result<()> {
        self.log.sync_all()
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

    pub fn delete(&mut self, key: String) {
        self.pending.push(Command::Delete { key });
    }

    pub fn incr(&mut self, key: String, delta: i64) {
        self.pending.push(Command::Incr { key, delta });
    }

    pub fn expire(&mut self, key: String, ttl: Option<u64>) {
        self.pending.push(Command::Expire { key, ttl });
    }
}