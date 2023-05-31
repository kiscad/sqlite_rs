use crate::btree::node::{Node, NodeWk};
use crate::error::ExecErr;
use crate::table::TABLE_MAX_PAGES;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE];

/// Pager is a in-memory cache for database file.
pub struct Pager {
    file: File,
    pub num_pages: usize,
    pub pages: [Option<NodeWk>; TABLE_MAX_PAGES],
}

impl Pager {
    /// create a Pager by opening a Database file.
    pub fn open_database(filename: impl AsRef<Path>) -> Result<Self, ExecErr> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(filename)
            .map_err(|_| ExecErr::IoError("Unable to open file.".to_string()))?;
        let file_len = file.metadata().unwrap().len() as usize;

        let num_pages = file_len / PAGE_SIZE;
        if file_len % PAGE_SIZE != 0 {
            return Err(ExecErr::IoError(
                "Db file is not a whole number of pages. Corrupt file.".to_string(),
            ));
        }
        const INIT: Option<NodeWk> = None;
        let pages = [INIT; TABLE_MAX_PAGES];

        Ok(Self {
            file,
            num_pages,
            pages,
        })
    }

    pub fn load_node_from_page(&mut self, page_idx: usize) -> Result<Node, ExecErr> {
        let mut buf = [0; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64))
            .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;
        self.file
            .read_exact(&mut buf)
            .map_err(|_| ExecErr::IoError("Error: Fail reading.".to_string()))?;
        Ok(Node::new_from_page(&buf))
    }

    pub fn write_page(&mut self, page_idx: usize, page: &Page) -> Result<(), ExecErr> {
        self.file
            .seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64))
            .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;
        self.file
            .write_all(page)
            .map_err(|_| ExecErr::IoError("Error: Fail writing.".to_string()))
    }

    pub fn read_page(&mut self, page_idx: usize) -> Result<Page, ExecErr> {
        let mut buf = [0; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64))
            .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;
        self.file
            .read_exact(&mut buf)
            .map_err(|_| ExecErr::IoError("Error: Fail reading.".to_string()))?;
        Ok(buf)
    }
}
