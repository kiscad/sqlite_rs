use super::PrepareErr;
use crate::cursor::Cursor;
use crate::error::ExecErr;
use std::fmt::Formatter;
use std::io::{self, Read, Write};

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub type RowBytes = [u8; ROW_SIZE];

#[derive(Debug)]
pub struct Row {
    pub id: u32,
    username: [u8; USERNAME_SIZE],
    email: [u8; EMAIL_SIZE],
}

impl Row {
    pub fn build(id: u32, name: &str, mail: &str) -> Result<Self, PrepareErr> {
        if name.len() > USERNAME_SIZE || mail.len() > EMAIL_SIZE {
            return Err(PrepareErr::StringTooLong("String is too long.".to_string()));
        }
        let mut username = [0u8; USERNAME_SIZE];
        username[..name.len()].copy_from_slice(name.as_bytes());
        let mut email = [0u8; EMAIL_SIZE];
        email[..mail.len()].copy_from_slice(mail.as_bytes());
        Ok(Self {
            id,
            username,
            email,
        })
    }

    pub fn default() -> Self {
        Self {
            id: 0,
            username: [0; USERNAME_SIZE],
            email: [0; EMAIL_SIZE],
        }
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let username = std::str::from_utf8(&self.username)
            .unwrap()
            .trim_end_matches('\0');
        let email = std::str::from_utf8(&self.email)
            .unwrap()
            .trim_end_matches('\0');
        write!(f, "({}, {:?}, {:?})", self.id, username, email)
    }
}

impl Row {
    pub fn write_to(&self, cursor: &mut Cursor) -> Result<(), ExecErr> {
        cursor.update_row(self.id, &self.serialize())
    }

    pub fn insert_to(&self, cursor: &mut Cursor) -> Result<(), ExecErr> {
        cursor.insert_row(self.id, &self.serialize())
    }

    pub fn read_from(&mut self, cursor: &mut Cursor) -> Result<(), ExecErr> {
        let mut buf = [0u8; ROW_SIZE];
        cursor.read_row(&mut buf)?;

        let mut reader = io::Cursor::new(&buf[..]);
        let mut id = [0u8; ID_SIZE];
        reader.read_exact(&mut id).unwrap();
        self.id = u32::from_be_bytes(id);
        reader.read_exact(&mut self.username).unwrap();
        reader.read_exact(&mut self.email).unwrap();
        Ok(())
    }

    fn serialize(&self) -> RowBytes {
        let mut buf = [0u8; ROW_SIZE];
        let mut writer = io::Cursor::new(&mut buf[..]);
        writer.write_all(&self.id.to_be_bytes()).unwrap();
        writer.write_all(&self.username).unwrap();
        writer.write_all(&self.email).unwrap();
        buf
    }
}
