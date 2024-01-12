use crate::error::Error;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

const DB_NAME: &str = "db.db";

#[derive(Serialize, Deserialize, Debug)]
pub struct KvStore {
    mp: HashMap<String, String>,
    path: String,
}

impl KvStore {
    pub fn new(path: &Path) -> KvStore {
        KvStore {
            mp: HashMap::new(),
            path: path.to_string_lossy().to_string(),
        }
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.mp.insert(key, val);
        Ok(())
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.mp.get(&key) {
            None => Ok(None),
            Some(val) => Ok(Some(val.clone())),
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.mp.contains_key(&key) {
            return Err(Error::KeyNotExist);
        }
        self.mp.remove(&key);
        Ok(())
    }
    pub fn get_path(&self) -> String {
		self.path.clone()
    }
}
impl KvStore {
    fn load(file: &mut File, path: &Path) -> Result<KvStore> {
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap_or_else(|err| {
			println!("failed to read_to_string");
			0
		});
		if buf.is_empty() {
			Ok(KvStore::new(path))
		} else {
			let store: KvStore = serde_json::from_str(buf.as_str()).unwrap();
			Ok(store)
		}
    }
    pub fn open(path: &Path) -> Result<KvStore> {
		let file_path = path.join(Path::new(DB_NAME));
        let file_res = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create the file if it doesn't exist
            .open(file_path.as_path());

        match file_res {
            Ok(mut file) => {
                let store = KvStore::load(&mut file, file_path.as_path())?;
                Ok(store)
            }
            Err(e) => {
				println!("open file: {:?} error: {:?}", file_path.as_path(), e);
				Err(Error::OpenFileErr)
			}
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        println!("drop called and file is {:?}", self.path);
        let serialized = serde_json::to_string(self).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)
            .unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use crate::KvStore;

    #[test]
    fn test_open() {
        {
            let p = Path::new("/var/folders/gy/k0y4ffss31j74jk4rnr2pcm40000gn/T/.tmpHNZdzE");
            let mut store = KvStore::open(p).unwrap();
            let val = store.get("key".to_owned()).unwrap().unwrap();
            assert_eq!(val, "val".to_owned());
        }
    }
}
