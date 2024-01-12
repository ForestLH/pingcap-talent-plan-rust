use crate::{error::Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::Path, collections::HashMap,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct Entry {
    position: u64,
    offset: usize,
}
impl Entry {
    pub fn new(position: u64, offset: usize) -> Entry {
        Entry { position, offset }
    }
    pub fn get_string(file: &mut File, entry: &Entry) -> Result<String> {
        match file.seek(SeekFrom::Start(entry.position)) {
            Ok(_) => {
                let mut buf = vec![0; entry.offset];
                file.read_exact(&mut buf).unwrap();
                Ok(String::from_utf8(buf).unwrap())
            }
            Err(_) => Err(Error::FileSeekErr),
        }
    }
    pub fn get_entry(file: &File, val: &[u8]) -> Entry {
        let position = file.metadata().unwrap().len();
        let offset = val.len();
        Entry { position, offset }
    }
}

#[test]
fn test() {
    let path = Path::new("");
    let mut file = OpenOptions::new().create(true).open(path).unwrap();
    file.seek(SeekFrom::Start(89)).unwrap();
}
#[test]
fn test_serialize() {
    let index: HashMap<String, Entry> = HashMap::new();
    serde_json::to_string(&index).unwrap();
}
