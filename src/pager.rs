use crate::btree::Node;
use crate::error::ExecErr;
use crate::table::TABLE_MAX_PAGES;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE];

/// Pager 是磁盘上的数据库文件，在内存上的缓存
pub struct Pager {
    file: File,
    // file_len: usize,
    pub num_pages: usize,
    pages: [Option<Box<Node>>; TABLE_MAX_PAGES],
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

        const INIT: Option<Box<Node>> = None;
        let pages = [INIT; TABLE_MAX_PAGES];
        Ok(Self {
            file,
            // file_len,
            num_pages,
            pages,
        })
    }

    /// Insert a new page-node and return the pager-idx.
    pub fn insert_node(&mut self, node: Node) -> Result<usize, ExecErr> {
        if self.num_pages >= TABLE_MAX_PAGES {
            return Err(ExecErr::PagerFull("Error: Pager full.".to_string()));
        }
        self.pages[self.num_pages] = Some(Box::new(node));
        self.num_pages += 1;
        Ok(self.num_pages - 1)
    }

    pub fn get_node(&mut self, page_idx: usize) -> Result<&Node, ExecErr> {
        self.validate_page_idx(page_idx)?;
        self.try_load_page(page_idx)?;
        Ok(self.pages[page_idx].as_ref().unwrap().as_ref())
    }

    pub fn get_node_mut(&mut self, page_idx: usize) -> Result<&mut Node, ExecErr> {
        self.validate_page_idx(page_idx)?;
        self.try_load_page(page_idx)?;
        Ok(self.pages[page_idx].as_mut().unwrap().as_mut())
    }

    pub fn swap_pages(&mut self, index1: usize, index2: usize) -> Result<(), ExecErr> {
        self.validate_page_idx(index1)?;
        self.validate_page_idx(index2)?;
        assert_ne!(index1, index2);
        self.pages.swap(index1, index2);
        Ok(())
    }

    fn validate_page_idx(&self, page_idx: usize) -> Result<(), ExecErr> {
        if page_idx >= self.num_pages {
            Err(ExecErr::PageNumOutBound(
                "Error: PageNum overflow.".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn try_load_page(&mut self, page_idx: usize) -> Result<(), ExecErr> {
        if self.pages[page_idx].is_none() {
            self.load_page(page_idx)?;
        }
        Ok(())
    }

    /// load page-node from disk-file to memory
    fn load_page(&mut self, page_idx: usize) -> Result<(), ExecErr> {
        self.validate_page_idx(page_idx)?;
        assert!(self.pages[page_idx].is_none());

        let mut cache = [0; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start((page_idx * PAGE_SIZE) as u64))
            .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;
        self.file
            .read(&mut cache)
            .map_err(|_| ExecErr::IoError("Error: Fail reading.".to_string()))?;
        let node = Node::new_from_page(&cache);
        let _ = self.pages[page_idx].insert(Box::new(node));

        Ok(())
    }

    /// write page-node from memory to disk-file
    pub fn flush_pager(&mut self, page_num: usize) -> Result<(), ExecErr> {
        if self.pages[page_num].is_none() {
            return Ok(());
        }
        let node = self.pages[page_num].as_ref().unwrap().as_ref();
        // if table is empty
        if node.is_root() && node.is_leaf() {
            let Node::Leaf(nd) = node else { unreachable!() };
            if nd.cells.is_empty() {
                return Ok(());
            }
        }
        let cache = node.serialize();

        self.file
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;

        self.file
            .write_all(&cache)
            .map_err(|_| ExecErr::IoError("Error: Fail writing.".to_string()))
    }
}
