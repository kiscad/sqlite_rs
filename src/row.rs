use super::PrepareErr;
use crate::cursor::Cursor;
use std::fmt::Formatter;
use std::fs::read;
use std::io::{Read, Write};

const COL_USERNAME_SIZE: usize = 32;
const COL_EMAIL_SIZE: usize = 255;

#[derive(Debug)]
pub struct Row {
    pub id: u32,
    username: [u8; COL_USERNAME_SIZE],
    email: [u8; COL_EMAIL_SIZE],
}

impl Row {
    pub fn build(id: u32, name: &str, mail: &str) -> Result<Self, PrepareErr> {
        if name.len() > COL_USERNAME_SIZE || mail.len() > COL_EMAIL_SIZE {
            return Err(PrepareErr::StringTooLong("String is too long.".to_string()));
        }
        let mut username = [0u8; COL_USERNAME_SIZE];
        username[..name.len()].copy_from_slice(name.as_bytes());
        let mut email = [0u8; COL_EMAIL_SIZE];
        email[..mail.len()].copy_from_slice(mail.as_bytes());
        Ok(Self {
            id,
            username,
            email,
        })
    }

    fn new(id: u32, username: [u8; COL_USERNAME_SIZE], email: [u8; COL_EMAIL_SIZE]) -> Self {
        Self {
            id,
            username,
            email,
        }
    }

    pub fn default() -> Self {
        Self {
            id: 0,
            username: [0; COL_USERNAME_SIZE],
            email: [0; COL_EMAIL_SIZE],
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

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = COL_USERNAME_SIZE;
const EMAIL_SIZE: usize = COL_EMAIL_SIZE;
// const ID_OFFSET: usize = 0;
// const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
// const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub struct RowBytes<'a>(pub &'a mut [u8]);

impl Row {
    pub fn serialize_to(&self, mut writer: std::io::Cursor<&mut [u8]>) {
        writer.write(&self.id.to_be_bytes()).unwrap();
        writer.write(&self.username).unwrap();
        writer.write(&self.email).unwrap();
    }

    pub fn deserialize_from(&mut self, mut reader: std::io::Cursor<&mut &mut [u8]>) {
        let mut id = [0u8; ID_SIZE];
        reader.read(&mut id).unwrap();
        self.id = u32::from_be_bytes(id);
        reader.read(&mut self.username).unwrap();
        reader.read(&mut self.email).unwrap();
    }
}

pub fn serialize_row(row: &Row, row_bytes: RowBytes) {
    let mut w = std::io::Cursor::new(row_bytes.0);
    let id_bytes: [u8; 4] = row.id.to_be_bytes();
    w.write(&id_bytes).unwrap();
    w.write(&row.username).unwrap();
    w.write(&row.email).unwrap();
}

pub fn deserialize_row(row_bytes: RowBytes) -> Row {
    let mut w = std::io::Cursor::new(row_bytes.0);

    let mut id = [0u8; ID_SIZE];
    w.read(&mut id).unwrap();
    let mut username = [0u8; USERNAME_SIZE];
    w.read(&mut username).unwrap();
    let mut email = [0u8; EMAIL_SIZE];
    w.read(&mut email).unwrap();

    Row::new(u32::from_be_bytes(id), username, email)
}
