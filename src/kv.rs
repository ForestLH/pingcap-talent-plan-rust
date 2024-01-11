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
use tempfile::TempDir;

#[derive(Serialize, Deserialize, Debug)]
pub struct KvStore {
    mp: HashMap<String, String>,
    path: String,
}

impl KvStore {
    pub fn new() -> KvStore {
        let temp_dir = TempDir::new().unwrap();
        KvStore {
            mp: HashMap::new(),
            path: temp_dir.path().to_string_lossy().to_string(),
        }
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.mp.insert(key, val);
        Ok(())
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.mp.get(&key) {
            None => Err(Error::GetErr),
            Some(val) => Ok(Some(val.clone())),
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.mp.remove(&key);
        Ok(())
    }
}
impl KvStore {
    fn load(file: &mut File) -> Result<KvStore> {
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();
        let store: KvStore = serde_json::from_str(buf.as_str()).unwrap();
        Ok(store)
    }
    pub fn open(path: &Path) -> Result<KvStore> {
        let file_res = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create the file if it doesn't exist
            .open(path);

        match file_res {
            Ok(mut file) => {
                let store = KvStore::load(&mut file)?;
                Ok(store)
            }
            Err(_) => Err(Error::OpenFileErr),
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
