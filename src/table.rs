use crate::btree::Node;
use crate::pager::{Pager, PAGE_SIZE};
use crate::row::ROW_SIZE;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE; // TODO: remove
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES; // TODO: remove

pub struct Table {
    pub pager: Pager,
    pub root_page_num: usize,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, String> {
        let mut pager = Pager::open_pager(filename)?;
        // TODO: 放在这里没必要
        if pager.num_pages == 0 {
            let node = match Node::new(pager.get_page(0).unwrap()) {
                Node::LeafNode(nd) => nd,
                _ => unreachable!(),
            };
            node.initialize();
        }

        Ok(Self {
            root_page_num: 0,
            pager,
        })
    }

    pub fn close_db(&mut self) {
        for i in 0..self.pager.pages.len() {
            if self.pager.pages[i].is_some() {
                self.pager
                    .flush_pager(i)
                    .unwrap_or_else(|_| std::process::exit(1));
            }
        }
    }
}
