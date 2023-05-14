use super::PrepareErr;
use std::fmt::Formatter;
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
            return Err(PrepareErr::StringTooLong);
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
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub struct RowB<'a>(pub &'a mut [u8]);

pub fn serialize_row(row: &Row, rowb: RowB) {
    let mut w = std::io::Cursor::new(rowb.0);
    let id_bytes: [u8; 4] = row.id.to_be_bytes();
    w.write(&id_bytes).unwrap();
    w.write(&row.username).unwrap();
    w.write(&row.email).unwrap();
}

pub fn deserialize_row(rowb: RowB) -> Row {
    let mut w = std::io::Cursor::new(rowb.0);
    let mut buf = [0u8; 4];
    w.read(&mut buf).unwrap();
    let id = u32::from_be_bytes(buf);
    let mut name_buf = [0u8; 32];
    w.read(&mut name_buf).unwrap();
    let mut email_buf = [0u8; 255];
    w.read(&mut email_buf).unwrap();
    Row::new(id, name_buf, email_buf)
}
