use crate::pager::Pager;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;

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
}
