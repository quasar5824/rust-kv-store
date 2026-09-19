use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

pub struct KvStore {
    data: HashMap<String, String>,
    log: File,
    path: String,
}

impl KvStore {
    pub fn open(path: &str) -> io::Result<Self> {
        let mut data = HashMap::new();
        
        let file = File::open(path);
        if let Ok(f) = file {
            let reader = BufReader::new(f);
            for line in reader.lines() {
                let line = line?;
                if let Ok(cmd) = serde_json::from_str::<Command>(&line) {
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
        })
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.batch(vec![Command::Set { key: key.to_string(), value: value.to_string() }])
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        self.batch(vec![Command::Delete { key: key.to_string() }])
    }

    pub fn batch(&mut self, commands: Vec<Command>) -> io::Result<()> {
        for cmd in &commands {
            let serialized = serde_json::to_string(cmd).unwrap();
            self.log.write_all(serialized.as_bytes())?;
            self.log.write_all(b"\n")?;
        }
        self.log.flush()?;

        for cmd in commands {
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

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
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
        Ok(())
    }
}