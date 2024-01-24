use crate::entry::Entry;
use crate::error::Error;
use crate::error::Result;
use log::debug;
use log::info;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};
use tempfile::TempDir;
use std::clone;
use std::fs;
use std::io::{Read, SeekFrom};
use std::io::Seek;
use std::mem::size_of;
use std::mem::swap;
use std::vec;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};
use std::fs::write;

const DB_NAME: &str = "db.db";
const INDEX_NAME: &str = "index.db";
const COMPACT_SIZE: u64 = 1024 * 512;

// should use bitcask model to organize data
// hashmap(in memory) K(String) V:(offset)
//
// #[derive(Serialize, Deserialize, Debug)]
pub struct KvStore {
    index: HashMap<String, Entry>,
	back_index: HashMap<String, Entry>,
    db: File,
    path: String,
    compact_times: u16,
}

impl KvStore {
    pub fn new(path: &Path) -> KvStore {
        let db_file = OpenOptions::new()
            .create(true)
			.read(true)
			.write(true)
            .open(path.join(Path::new(DB_NAME)))
            .unwrap();
        KvStore {
            index: HashMap::new(),
			back_index: HashMap::new(),
            db: db_file,
            path: path.to_string_lossy().to_string(),
            compact_times: 0,
        }
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
		self.db.seek(std::io::SeekFrom::End(0)).unwrap();
		let val_bytes = val.as_bytes();
		self.index.insert(key, Entry::get_entry(&self.db, val_bytes));
		self.db.write_all(val_bytes).unwrap();
		if self.db.seek(std::io::SeekFrom::End(0)).unwrap() > COMPACT_SIZE {
			self.compact();
		}
        Ok(())
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            None => Ok(None),
            Some(entry) => {
				let val = Entry::get_string(&mut self.db, entry)?;
				Ok(Some(val))
			}
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(Error::KeyNotExistErr);
        }
        self.index.remove(&key);
        Ok(())
    }
    pub fn get_db_path(&self) -> String {
        let mut p= self.path.clone();
		p.push_str("/");
		p.push_str(DB_NAME);
		p
    }
	pub fn get_index_path(&self) -> String {
        let mut p= self.path.clone();
		p.push_str("/");
		p.push_str(INDEX_NAME);
		p
	}
	// use 
	pub fn compact(&mut self) {
        let binding = TempDir::new().unwrap();
        let back_path = binding.path();
        self.snapshot(back_path).unwrap();
        let mut back_store = KvStore::open(back_path).unwrap();
		let mut keys:Vec<String> = vec![];
		for (key, _) in self.index.iter() {
			keys.push(key.clone());
		}
        self.db.set_len(0).unwrap();
        for key in keys {
            let mut val = back_store.get(key.clone()).unwrap().unwrap();
            self.set(key, val).unwrap();
        }
	}
    pub fn snapshot(&mut self, path: &Path) -> Result<()> {
        let mut index_pb = path.join(INDEX_NAME);
        let index_path = index_pb.as_path();
        let mut db_pb = path.join(DB_NAME);
        let db_path = db_pb.as_path();
        let mut db_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(db_path).unwrap();
        let file_size = self.db.metadata().unwrap().len();
        let mut index_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(index_path).unwrap();

        let serialized = serde_json::to_string(&self.index).unwrap();
        index_file.write_all(serialized.as_bytes()).unwrap();

        let mut buffer = Vec::with_capacity(file_size as usize);
        self.db.seek(SeekFrom::Start(0)).unwrap();
        self.db.read_to_end(&mut buffer).unwrap();
        db_file.write_all(&buffer).unwrap();
        Ok(())
    }
    pub fn compare(&mut self, other: &mut Self) -> bool {
        let mut keys = vec![];
        for (key, _) in &self.index {
            keys.push(key.clone());
        }
        for k in keys {
            let val = self.get(k.clone()).unwrap().unwrap();
            let back_val = other.get(k.clone()).unwrap().unwrap();
            if val != back_val {
                return false;
            }
        }
        true
    }
}

impl KvStore {
    fn load(index: &mut File, path: &Path) -> Result<KvStore> {
        let mut buf = String::new();
        index.read_to_string(&mut buf).unwrap_or_else(|err| {
            println!("failed to read_to_string");
            0
        });
        if buf.is_empty() {
            Ok(KvStore::new(path))
        } else {
            let index: HashMap<String, Entry> = serde_json::from_str(buf.as_str()).unwrap();
            let db = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path.join(Path::new(DB_NAME)).as_path())
                .unwrap();
            Ok(KvStore {
                index,
				back_index: HashMap::new(),
                db: db,
                path: path.to_string_lossy().to_string(),
                compact_times: 0,
            })
        }
    }
    pub fn open(path: &Path) -> Result<KvStore> {
        let index_path = path.join(Path::new(INDEX_NAME));
        let index_res = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create the file if it doesn't exist
            .open(index_path.as_path());

        match index_res {
            Ok(mut index) => {
                let store = KvStore::load(&mut index, path)?;
                Ok(store)
            }
            Err(e) => {
                println!("open file: {:?} error: {:?}", index_path.as_path(), e);
                Err(Error::OpenFileErr)
            }
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        println!("drop called and index file is {:?}", self.get_index_path());
        let serialized = serde_json::to_string(&self.index).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.get_index_path())
            .unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
    }
}

#[cfg(test)]
mod test {
    use walkdir::WalkDir;

    use crate::KvStore;
    use std::path::Path;
    use serde_json::to_string;
    use serde_json::Value::String;

    #[test]
    fn test_open() {
		let path_string = "/Users/lee/Code/RustLearn/pingcap-talent-plan/project-1";
		let p = Path::new(path_string);
        {
            let mut store = KvStore::open(p).unwrap();
			store.set("key".to_string(), "val".to_string()).unwrap();
            let val = store.get("key".to_owned()).unwrap().unwrap();
            assert_eq!(val, "val".to_owned());
        }
		let mut store = KvStore::open(p).unwrap();
		let val = store.get("key".to_owned()).unwrap().unwrap();
		assert_eq!(val, "val".to_owned());
    }
	fn get_dir_size(path: &Path) -> u64 {
		let entries = WalkDir::new(path).into_iter();
		let len: walkdir::Result<u64> = entries
			.map(|res| {
				res.and_then(|entry| entry.metadata())
					.map(|metadata| metadata.len())
			})
			.sum();
		len.expect("fail to get directory size")
	}
	#[test]
	fn test_compaction() {
		let path_string = "/Users/lee/Code/RustLearn/pingcap-talent-plan/project-1";
		let p = Path::new(path_string);
		let mut store = KvStore::open(p).unwrap();
		store.set("key".to_string(), "val".to_string()).unwrap();
		store.set("key".to_string(), "va2".to_string()).unwrap();
		store.set("key".to_string(), "val3".to_string()).unwrap();
		store.set("key".to_string(), "val4".to_string()).unwrap();
		let before_size = get_dir_size(p);
		store.compact();
		let after_size = get_dir_size(p);
		assert!(before_size > after_size);
	}
    #[test]
    fn test_snapshot() {
        let path = Path::new("/Users/lee/Code/RustLearn/pingcap-talent-plan/project-1");
        let mut store = KvStore::new(path);
        for i in 0..100 {
            let iter = i.to_string();
            store.set(iter.clone(), iter).unwrap();
        }
        let back_path = Path::new("/Users/lee/Code/RustLearn/pingcap-talent-plan/project-1/back");
        store.snapshot(back_path).unwrap();
        let mut back_store = KvStore::open(back_path).unwrap();
        for i in 0..100 {
            let iter = i.to_string();
            let res = back_store.get(iter.clone()).unwrap().unwrap();
            assert_eq!(res, iter);
        }
    }
}
