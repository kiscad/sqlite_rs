use crate::row::RowBytes;
use crate::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    pub page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn new_at_table_start(table: &'a mut Table) -> Self {
        let node = table.pager.get_leaf_node(table.root_page_num).unwrap();
        let page_num = table.root_page_num;
        let end_of_table = node.get_num_cells() == 0;
        Self {
            table,
            page_num,
            cell_num: 0,
            end_of_table,
        }
    }

    pub fn new_at_table_end(table: &'a mut Table) -> Self {
        let node = table.pager.get_leaf_node(table.root_page_num).unwrap();
        let page_num = table.root_page_num;
        let cell_num = node.get_num_cells() as usize;
        Self {
            table,
            page_num,
            cell_num,
            end_of_table: true,
        }
    }

    // pub fn get_row_bytes(&mut self) -> &RowBytes {
    //     let node = self.table.pager.get_leaf_node(self.page_num).unwrap();
    //     let cell = &node.cells[self.cell_num as usize];
    //     &cell.1
    // }

    pub fn advance(&mut self) {
        self.cell_num += 1;
        let node = self.table.pager.get_leaf_node(self.page_num).unwrap();
        self.end_of_table = self.cell_num >= node.get_num_cells() as usize;
    }
}
