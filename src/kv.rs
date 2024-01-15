use crate::entry::Entry;
use crate::error::Error;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tempfile::TempDir;
use std::clone;
use std::fs;
use std::io::Read;
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
	fn append_entry(&mut self, key: String, val: String, file: &mut File) -> Result<()>{
		file.seek(std::io::SeekFrom::End(0)).unwrap();
		let val_bytes = val.as_bytes();
		self.back_index.insert(key, Entry::get_entry(file, val_bytes));
		file.write_all(val_bytes).unwrap();
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
	fn get_from_file(&mut self, key: String, file: &mut File) ->Result<Option<String>> {
		match self.back_index.get(&key) {
            None => Ok(None),
            Some(entry) => {
				let val = Entry::get_string(file, entry)?;
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
		let mut keys:Vec<String> = vec![];
		for iter in self.index.iter() {
			keys.push(iter.0.clone());
		}
		self.back_index.clear();
		let mut named_temp_file = NamedTempFile::new().unwrap();
		let file = named_temp_file.as_file_mut();
		for key in keys {
			let val = self.get(key.clone()).unwrap().unwrap();
			self.append_entry(key.clone(), val.clone(), file).unwrap();
			
			/*just for debug */
			let val_back = self.get_from_file(key, file).unwrap().unwrap();
			if val_back != val {
				let debug = 12;
				panic!()
			}
			/***************************** */
		}

		
		fs::rename(named_temp_file.path(), self.get_db_path()).unwrap();
		swap(&mut self.index, &mut self.back_index);
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
}
