use crate::error::PrepareErr;
use std::io::{Read, Write};
use std::{fmt, io, str};

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub type RowBytes = [u8; ROW_SIZE];

#[derive(Debug)]
pub struct Row {
  pub key: u32,
  username: [u8; USERNAME_SIZE],
  email: [u8; EMAIL_SIZE],
}

impl Row {
  pub fn build(key: u32, name: &str, mail: &str) -> Result<Self, PrepareErr> {
    if name.len() > USERNAME_SIZE || mail.len() > EMAIL_SIZE {
      return Err(PrepareErr::StringTooLong("String too long".to_string()));
    }
    let mut username = [0u8; USERNAME_SIZE];
    username[..name.len()].copy_from_slice(name.as_bytes());
    let mut email = [0u8; EMAIL_SIZE];
    email[..mail.len()].copy_from_slice(mail.as_bytes());
    Ok(Self {
      key,
      username,
      email,
    })
  }

  pub fn deserialize_from(buf: RowBytes) -> Self {
    let mut reader = io::Cursor::new(&buf[..]);
    let key = {
      let mut buf = [0; ID_SIZE];
      reader.read_exact(&mut buf).unwrap();
      u32::from_be_bytes(buf)
    };
    let mut username = [0; USERNAME_SIZE];
    reader.read_exact(&mut username).unwrap();
    let mut email = [0; EMAIL_SIZE];
    reader.read_exact(&mut email).unwrap();
    Self {
      key,
      username,
      email,
    }
  }

  pub fn serialize(&self) -> RowBytes {
    let mut buf = [0u8; ROW_SIZE];
    let mut writer = io::Cursor::new(&mut buf[..]);
    writer.write_all(&self.key.to_be_bytes()).unwrap();
    writer.write_all(&self.username).unwrap();
    writer.write_all(&self.email).unwrap();
    buf
  }
}

impl fmt::Display for Row {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let username = str::from_utf8(&self.username)
      .unwrap()
      .trim_end_matches('\0');
    let email = str::from_utf8(&self.email).unwrap().trim_end_matches('\0');
    write!(f, "({}, {username:?}, {email:?})", self.key)
  }
}
