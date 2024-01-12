use crate::entry::Entry;
use crate::error::Error;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::io::Seek;
use std::os::unix::fs::MetadataExt;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

const DB_NAME: &str = "db.db";
const INDEX_NAME: &str = "index.db";

// should use bitcask model to organize data
// hashmap(in memory) K(String) V:(offset)
//
// #[derive(Serialize, Deserialize, Debug)]
pub struct KvStore {
    index: HashMap<String, Entry>,
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
            db: db_file,
            path: path.to_string_lossy().to_string(),
        }
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
		self.db.seek(std::io::SeekFrom::End(0)).unwrap();
		let val_bytes = val.as_bytes();
		self.index.insert(key, Entry::get_entry(&self.db, val_bytes));
		self.db.write_all(val_bytes).unwrap();
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
}
