use crate::btree::{InternalNode, LeafNode, Node, PageKey};
use crate::error::ExecErr;
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

    pub fn split_root_node(&mut self, root_idx: usize, right: Node) -> Result<(), ExecErr> {
        let old_root = self.pages[root_idx].as_mut().unwrap().as_mut();
        match &old_root {
            Node::InternalNode(nd) => todo!(),
            Node::LeafNode(_) => {
                let mut new_root = Node::InternalNode(InternalNode::new(
                    self.num_pages as u32,
                    old_root.get_max_key(),
                    self.num_pages as u32 + 1,
                ));
                old_root.set_root(false);
                new_root.set_root(true);

                self.insert_node(new_root)?;
                self.pages.swap(root_idx, self.num_pages - 1);
                self.insert_node(right)?;
                Ok(())
            }
        }
    }

    fn insert_node(&mut self, node: Node) -> Result<&mut Node, ExecErr> {
        if self.num_pages >= TABLE_MAX_PAGES {
            return Err(ExecErr::PagerFull("Error: Pager full.".to_string()));
        }
        self.pages[self.num_pages] = Some(Box::new(node));
        self.num_pages += 1;
        Ok(self.pages[self.num_pages - 1].as_mut().unwrap())
    }

    pub fn create_leaf_node(&mut self) -> Result<&mut Node, ExecErr> {
        let node = Node::LeafNode(LeafNode::new(false));
        self.insert_node(node)
    }

    pub fn find_leaf_node(&mut self, page: usize, cell_key: u32) -> usize {
        let node = self.get_node(page).unwrap();
        match node {
            Node::LeafNode(_) => page,
            Node::InternalNode(nd) => {
                let mut page_tag: Option<u32> = None;
                for PageKey { page, key } in &nd.children {
                    if cell_key <= *key {
                        page_tag = Some(*page);
                        break;
                    }
                }
                let page = page_tag.unwrap_or(nd.right_child_page);
                self.find_leaf_node(page as usize, cell_key)
            }
        }
    }

    pub fn get_node(&mut self, page_num: usize) -> Result<&mut Node, ExecErr> {
        // if table is empty
        if self.num_pages == 0 {
            let node = Node::LeafNode(LeafNode::new(true));
            return self.insert_node(node);
        }

        if page_num >= self.num_pages {
            return Err(ExecErr::PageNumOutBound(
                "Error: PageNum overflow.".to_string(),
            ));
        }

        let node_box = &mut self.pages[page_num];
        // if the requested Page is not buffered, we need retrieve from file.
        if node_box.is_none() {
            let mut buffer = [0; PAGE_SIZE];

            self.file
                .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;
            self.file
                .read(&mut buffer)
                .map_err(|_| ExecErr::IoError("Error: Fail reading.".to_string()))?;

            let node = Node::new_from_page(&buffer);
            let _ = node_box.insert(Box::new(node));
            // let leaf = LeafNode::new_from_page(&buffer);
            // let _ = node_box.insert(Box::new(Node::LeafNode(leaf)));
        }

        Ok(node_box.as_mut().unwrap())
    }

    pub fn flush_pager(&mut self, page_num: usize) -> Result<(), ExecErr> {
        if self.pages[page_num].is_none() {
            eprintln!("Tried to flush null page.");
            return Ok(());
        }
        self.file
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .map_err(|_| ExecErr::IoError("Error: Fail seeking.".to_string()))?;

        let node = self.pages[page_num].as_ref().unwrap().as_ref();
        let mut buf = [0; PAGE_SIZE];
        match node {
            Node::LeafNode(nd) => nd.write_page(&mut buf),
            _ => unreachable!(),
        };

        self.file
            .write(&buf)
            .map_err(|_| ExecErr::IoError("Error: Fail writing.".to_string()))?;
        Ok(())
    }

    pub fn stringfy_btree(&self, root: usize) -> String {
        match self.pages[root].as_ref().unwrap().as_ref() {
            Node::LeafNode(nd) => format!("{}", nd),
            Node::InternalNode(nd) => self.stringfy_internal_node(nd),
        }
    }

    fn stringfy_internal_node(&self, node: &InternalNode) -> String {
        let mut res = String::new();
        res.push_str(&format!("{}", node));
        for PageKey { page, .. } in &node.children {
            let node = self.pages[*page as usize].as_ref().unwrap().as_ref();
            match node {
                Node::LeafNode(nd) => {
                    let s: String = format!("{}\n", nd)
                        .lines()
                        .map(|s| format!("  {}\n", s))
                        .collect();
                    res.push_str(&s);
                }
                Node::InternalNode(nd) => {
                    let s: String = self
                        .stringfy_internal_node(nd)
                        .lines()
                        .map(|s| format!("  {}\n", s))
                        .collect();
                    res.push_str(&s);
                }
            }
        }
        res
    }
}
