use crate::table::TABLE_MAX_PAGES;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    file: File,
    pub file_len: usize,
    pub pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
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
        const INIT: Option<Box<Page>> = None;
        let pages = [INIT; TABLE_MAX_PAGES];
        Ok(Self {
            file,
            file_len,
            pages,
        })
    }

    pub fn get_page(&mut self, page_num: usize) -> Result<&mut Box<Page>, String> {
        if page_num > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}");
            return Err("ExitFailure".to_string());
        }
        let page = &mut self.pages[page_num];

        // if the requested Page is not buffered, we need retrieve from file.
        if page.is_none() {
            let mut num_pages = self.file_len / PAGE_SIZE;
            if self.file_len % PAGE_SIZE > 0 {
                num_pages += 1;
            }
            let mut buffer = [0; PAGE_SIZE];
            if page_num < num_pages {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .unwrap();
                self.file.read(&mut buffer).map_err(|e| {
                    println!("Error reading file: {e}");
                    "ExitFailure".to_string()
                })?;
            }
            let _ = page.insert(Box::new(Page(buffer)));
        }
        Ok(page.as_mut().unwrap())
    }

    pub fn flush_pager(&mut self, page_num: usize, size: usize) -> Result<(), String> {
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
        let buf = &self.pages[page_num].as_ref().unwrap().as_ref().0[..size];
        self.file.write(buf).map_err(|_| {
            println!("Error writing.");
            "ExitFailure".to_string()
        })?;
        Ok(())
    }
}

pub struct Page(pub [u8; PAGE_SIZE]);
