use super::PrepareErr;
use crate::cursor::Cursor;
use std::fmt::Formatter;
use std::io::{self, Read, Write};

const COL_USERNAME_SIZE: usize = 32;
const COL_EMAIL_SIZE: usize = 255;

pub type RowBytes = [u8; ROW_SIZE];

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

impl Row {
    pub fn write_to(&self, cursor: &mut Cursor) {
        let mut buf = [0u8; ROW_SIZE];
        let mut writer = io::Cursor::new(&mut buf[..]);
        writer.write_all(&self.id.to_be_bytes()).unwrap();
        writer.write_all(&self.username).unwrap();
        writer.write_all(&self.email).unwrap();
        cursor.write_row_bytes(&buf);
    }

    pub fn read_from(&mut self, cursor: &mut Cursor) {
        let mut buf = [0u8; ROW_SIZE];
        cursor.read_row_bytes(&mut buf);

        let mut reader = io::Cursor::new(&buf[..]);
        let mut id = [0u8; ID_SIZE];
        reader.read_exact(&mut id).unwrap();
        self.id = u32::from_be_bytes(id);
        reader.read_exact(&mut self.username).unwrap();
        reader.read_exact(&mut self.email).unwrap();
    }
}
