use crate::btree::Node;
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
        let node = table.pager.get_page(table.root_page_num).unwrap();
        let page_num = table.root_page_num;
        let end_of_table = match node.as_ref() {
            Node::LeafNode(nd) => nd.num_cells == 0,
            _ => unreachable!(),
        };
        Self {
            table,
            page_num,
            cell_num: 0,
            end_of_table,
        }
    }

    pub fn new_at_table_end(table: &'a mut Table) -> Self {
        let node = table.pager.get_page(table.root_page_num).unwrap();
        let cell_num = match node.as_ref() {
            Node::LeafNode(nd) => nd.num_cells,
            _ => unreachable!(),
        };
        let page_num = table.root_page_num;
        Self {
            table,
            page_num,
            cell_num: cell_num as usize,
            end_of_table: true,
        }
    }

    pub fn read_row_bytes(&mut self, buf: &mut RowBytes) {
        let node = self.table.pager.get_page(self.page_num).unwrap();
        match node.as_ref() {
            Node::LeafNode(nd) => nd.read_cell_value(self.cell_num as u32, buf),
            _ => unreachable!(),
        }
    }

    pub fn write_row_bytes(&mut self, buf: &RowBytes) {
        let node = self.table.pager.get_page(self.page_num).unwrap();
        match node.as_mut() {
            Node::LeafNode(nd) => {
                if self.end_of_table {
                    nd.append_cell_value(buf);
                } else {
                    nd.write_cell_value(self.cell_num as u32, buf)
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn advance(&mut self) {
        self.cell_num += 1;
        let node = self.table.pager.get_page(self.page_num).unwrap();
        self.end_of_table = match node.as_ref() {
            Node::LeafNode(nd) => self.cell_num >= nd.num_cells as usize,
            _ => unreachable!(),
        };
    }
}
