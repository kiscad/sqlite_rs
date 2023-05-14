use crate::row::{RowB, ROW_SIZE};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Page([u8; PAGE_SIZE]);

impl Page {
    fn new() -> Self {
        Self([0; PAGE_SIZE])
    }
}

pub struct Table {
    pub num_rows: usize,
    pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
}

impl Table {
    pub fn row_slot(&mut self, row_num: usize) -> RowB {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = &mut self.pages[page_num];
        let page = page.get_or_insert_with(|| Box::new(Page::new()));
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        RowB(&mut page.0[byte_offset..byte_offset + ROW_SIZE])
    }

    pub fn new() -> Self {
        const INIT: Option<Box<Page>> = None;
        Self {
            num_rows: 0,
            pages: [INIT; TABLE_MAX_PAGES],
        }
    }
}
