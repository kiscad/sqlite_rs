#![feature(allocator_api)]

use bumpalo::Bump;

fn main() {
  let bump = Bump::new();

  let mut vec = Vec::new_in(&bump);
  vec.push(1);
  vec.push(2);
  vec.push(3);
  println!("{vec:?}");
}
