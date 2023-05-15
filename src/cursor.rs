use crate::row::{RowBytes, ROW_SIZE};
use crate::table::ROWS_PER_PAGE;
use crate::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn new_at_table_start(table: &'a mut Table) -> Self {
        let row_num = 0;
        let end_of_table = table.num_rows == row_num;
        Self {
            table,
            row_num,
            end_of_table,
        }
    }

    pub fn new_at_table_end(table: &'a mut Table) -> Self {
        let row_num = table.num_rows;
        let end_of_table = true;
        Self {
            table,
            row_num,
            end_of_table,
        }
    }

    pub fn get_row_bytes(&mut self) -> RowBytes {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.table.pager.get_page(page_num).unwrap();

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        RowBytes(&mut page.0[byte_offset..byte_offset + ROW_SIZE])
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
        self.end_of_table = self.row_num >= self.table.num_rows;
    }
}
