use crate::pager::{Pager, PAGE_SIZE};
use crate::row::{RowBytes, ROW_SIZE};
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    pub num_rows: usize,
    pager: Pager,
}

impl Table {
    pub fn row_slot(&mut self, row_num: usize) -> RowBytes {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num).unwrap();
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        RowBytes(&mut page.0[byte_offset..byte_offset + ROW_SIZE])
    }

    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, String> {
        let pager = Pager::open_pager(filename)?;
        let num_rows = pager.file_len / ROW_SIZE;
        Ok(Self { num_rows, pager })
    }

    pub fn close_db(&mut self) {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            if self.pager.pages[i].is_some() {
                let res = self.pager.flush_pager(i, PAGE_SIZE);
                if res.is_err() {
                    std::process::exit(1);
                }
            }
        }
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if self.pager.pages[page_num].is_some() {
                let res = self
                    .pager
                    .flush_pager(page_num, num_additional_rows * ROW_SIZE);
                if res.is_err() {
                    std::process::exit(1);
                }
            }
        }
    }
}
