pub trait KvsEngine {
    fn get(&mut self, key: String) -> String;
    fn set(&mut self, key: String, val: String);
    fn remove(&mut self, key: String);
}