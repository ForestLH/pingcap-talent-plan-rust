use std::{collections::HashMap, clone};

pub struct KvStore {
  mp: HashMap<String, String>,
}
impl KvStore {
  pub fn new() -> KvStore {
    KvStore {
      mp: HashMap::new(),
    }
  }
  pub fn set(&mut self, key: String, val: String) {
    self.mp.insert(key, val);
  }
  pub fn get(&mut self, key: String) -> Option<String> {
    self.mp.get(&key).cloned()
  }
  pub fn remove(&mut self, key: String) {
    self.mp.remove(&key);
  }
}