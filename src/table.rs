use crate::btree;
use crate::btree::Node;
use crate::error::ExecErr;
use crate::pager::{self, Pager};
use crate::row;
use crate::row::RowBytes;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;
const CELL_SIZE: usize = row::ROW_SIZE + btree::CELL_KEY_SIZE;
pub const PAGE_MAX_ROWS: usize = (pager::PAGE_SIZE - btree::LEAF_NODE_HEADER_SIZE) / CELL_SIZE;

pub struct Table {
    pub pager: Pager,
    pub root_page_num: usize,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, String> {
        let pager = Pager::open_pager(filename)?;

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

    pub fn insert_row(&mut self, row: &RowBytes) -> Result<(), ExecErr> {
        let node = self.pager.get_page(self.root_page_num).unwrap();
        match node.as_mut() {
            Node::LeafNode(nd) => {
                if nd.cells.len() < PAGE_MAX_ROWS {
                    nd.append_cell_value(row);
                } else {
                    println!("Error: Table full.");
                    return Err(ExecErr::TableFull("Error: Table full.".to_string()));
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
