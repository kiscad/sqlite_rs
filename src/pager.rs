use crate::btree::{LeafNode, Node};
use crate::table::TABLE_MAX_PAGES;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE];

pub struct Pager {
    file: File,
    pub file_len: usize,
    pub num_pages: usize,
    pub pages: [Option<Box<Node>>; TABLE_MAX_PAGES],
}

impl Pager {
    pub fn open_pager(filename: impl AsRef<Path>) -> Result<Self, String> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(filename)
            .map_err(|_| {
                println!("Unable to open file.");
                "ExitFailure".to_string()
            })?;
        let file_len = file.metadata().unwrap().len() as usize;

        let num_pages = file_len / PAGE_SIZE;
        if file_len % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            return Err("ExitFailure".to_string());
        }

        const INIT: Option<Box<Node>> = None;
        let pages = [INIT; TABLE_MAX_PAGES];
        Ok(Self {
            file,
            file_len,
            num_pages,
            pages,
        })
    }

    pub fn get_page(&mut self, page_num: usize) -> Result<&mut Box<Node>, String> {
        if page_num > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}");
            return Err("ExitFailure".to_string());
        }
        let page = &mut self.pages[page_num];

        // if the requested Page is not buffered, we need retrieve from file.
        if page.is_none() {
            let mut buffer = [0; PAGE_SIZE];
            if page_num < self.num_pages {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .unwrap();
                self.file.read(&mut buffer).map_err(|e| {
                    println!("Error reading file: {e}");
                    "ExitFailure".to_string()
                })?;
            }
            let mut node = LeafNode::default();
            node.read_page(&buffer);
            let _ = page.insert(Box::new(Node::LeafNode(node)));
        }

        if page_num >= self.num_pages {
            self.num_pages += 1;
        }

        Ok(page.as_mut().unwrap())
    }

    pub fn flush_pager(&mut self, page_num: usize) -> Result<(), String> {
        if self.pages[page_num].is_none() {
            println!("Tried to flush null page.");
            return Err("ExitFailure".to_string());
        }
        self.file
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .map_err(|_| {
                println!("Error seeking.");
                "ExitFailure".to_string()
            })?;

        let node = self.pages[page_num].as_ref().unwrap().as_ref();
        let mut buf = [0; PAGE_SIZE];
        match node {
            Node::LeafNode(nd) => nd.write_page(&mut buf),
            _ => unreachable!(),
        };

        self.file.write(&buf).map_err(|_| {
            println!("Error writing.");
            "ExitFailure".to_string()
        })?;
        Ok(())
    }
}
