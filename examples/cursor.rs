use std::io::{Cursor, Write};

fn main() {
    let mut w: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    w.write(&1u32.to_be_bytes()).unwrap();
    assert_eq!(w.position(), 4);
    let arr = [0u8; 32];
    w.write(&arr).unwrap();
    assert_eq!(w.position(), 36);
}