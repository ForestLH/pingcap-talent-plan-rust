use std::env;
use kvs::Result;

fn main() -> Result<()> {
  let mut args:Vec<String> = env::args().collect();
  args.remove(0);
  if args.is_empty() {
    panic!();
  }
  println!("args: {:?}", args);
  let mut i = 0;
  while i < args.len() {
    match args[i].as_str() {
      "-V" => {
        println!("{:?}", env!("CARGO_PKG_VERSION"));
      },
      "get" => {
        let key_index = i + 1;
        if key_index >= args.len() {
          panic!();
        } else {
          let get_key = &args[key_index];
          panic!("unimplemented");
        }
      },
      "set" => {
        let key_index = i + 1;
        let val_index = key_index + 1;
        if val_index >= args.len() {
          panic!();
        } else {
          panic!("unimplemented");
        }
      },
      "rm" => {
        let key_index = i + 1;
        if key_index >= args.len() {
          panic!();
        } else {
          panic!("unimplemented");
        }
      },
      _ => {
        panic!();
      }
    }
    i += 1;
  }
  Ok(())
}