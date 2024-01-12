
pub struct DeferDrop<F: FnOnce()> {
  // 用于存储待执行的闭包
  closure: Option<F>,
}

impl<F: FnOnce()> DeferDrop<F> {
  // 创建一个新的 DeferDrop 实例
  pub fn new(closure: F) -> Self {
      DeferDrop {
          closure: Some(closure),
      }
  }
}

impl<F: FnOnce()> Drop for DeferDrop<F> {
  // 在结构体实例离开作用域时执行的逻辑
  fn drop(&mut self) {
      // 取出闭包并执行
      if let Some(closure) = self.closure.take() {
          closure();
      }
  }
}