use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug)]
pub struct StoreStats {
    pub key_count: usize,
    pub ops_count: usize,
}

pub struct KvStore {
    data: HashMap<String, String>,
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
                        Command::Set { key, value } => {
                            data.insert(key, value);
                        }
                        Command::Delete { key } => {
                            data.remove(&key);
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
        self.batch(vec![Command::Set { key: key.to_string(), value: value.to_string() }])
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
                Command::Set { key, value } => {
                    self.data.insert(key, value);
                }
                Command::Delete { key } => {
                    self.data.remove(&key);
                }
            }
        }
        Ok(())
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

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn get_with_default(&self, key: &str, default: &str) -> String {
        self.get(key).cloned().unwrap_or_else(|| default.to_string())
    }

    pub fn exists(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    pub fn get_all(&self) -> Vec<(&String, &String)> {
        self.data.iter().collect()
    }

    pub fn scan(&self, prefix: &str) -> Vec<(&String, &String)> {
        self.data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect()
    }

    pub fn range(&self, start: &str, end: &str) -> Vec<(&String, &String)> {
        let mut results: Vec<(&String, &String)> = self.data
            .iter()
            .filter(|(k, _)| k >= start && k <= end)
            .collect();
        results.sort_by(|a, b| a.0.cmp(b.0));
        results
    }

    pub fn cursor(&self) -> KvCursor<'_> {
        KvCursor {
            iter: self.data.iter(),
        }
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

    pub fn compact(&mut self) -> io::Result<()> {
        let mut new_log = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        for (key, value) in &self.data {
            let cmd = Command::Set {
                key: key.clone(),
                value: value.clone(),
            };
            let serialized = serde_json::to_string(&cmd).unwrap();
            new_log.write_all(serialized.as_bytes())?;
            new_log.write_all(b"\n")?;
        }
        new_log.flush()?;
        
        self.log = new_log;
        self.ops_count = self.data.len();
        Ok(())
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            key_count: self.data.len(),
            ops_count: self.ops_count,
        }
    }
}

pub struct Transaction {
    pending: Vec<Command>,
}

impl Transaction {
    pub fn set(&mut self, key: String, value: String) {
        self.pending.push(Command::Set { key, value });
    }

    pub fn delete(&mut self, key: String) {
        self.pending.push(Command::Delete { key });
    }
}

pub struct KvCursor<'a> {
    iter: std::collections::hash_map::Iter<'a, String, String>,
}

impl<'a> Iterator for KvCursor<'a> {
    type Item = (&'a String, &'a String);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
