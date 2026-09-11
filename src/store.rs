use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

pub struct KvStore {
    data: HashMap<String, String>,
    log: File,
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
        })
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        let cmd = Command::Set {
            key: key.to_string(),
            value: value.to_string(),
        };
        let serialized = serde_json::to_string(&cmd).unwrap();
        
        self.log.write_all(serialized.as_bytes())?;
        self.log.write_all(b"\n")?;
        self.log.flush()?;

        self.data.insert(key.to_string(), value.to_string());
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        let cmd = Command::Delete {
            key: key.to_string(),
        };
        let serialized = serde_json::to_string(&cmd).unwrap();

        self.log.write_all(serialized.as_bytes())?;
        self.log.write_all(b"\n")?;
        self.log.flush()?;

        self.data.remove(key);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn get_all(&self) -> Vec<(&String, &String)> {
        self.data.iter().collect()
    }
}