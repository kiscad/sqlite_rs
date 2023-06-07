use crate::pager2::Page;
use std::io::{Cursor, Read, Write};

pub fn read_u32_from(reader: &mut Cursor<&Page>) -> Option<u32> {
  let mut buf = [0; 4];
  reader.read_exact(&mut buf).unwrap();
  let res = u32::from_be_bytes(buf);
  if res == 0 {
    None
  } else {
    Some(res)
  }
}

pub fn read_bool_from(reader: &mut Cursor<&Page>) -> bool {
  let mut buf = [0];
  reader.read_exact(&mut buf).unwrap();
  buf[0] != 0
}

pub fn write_bool_to(writer: &mut Cursor<&mut [u8]>, val: bool) {
  writer.write_all(&[u8::from(val)]).unwrap();
}

pub fn write_opt_u32_to(writer: &mut Cursor<&mut [u8]>, val: Option<u32>) {
  writer.write_all(&val.unwrap_or(0).to_be_bytes()).unwrap();
}
