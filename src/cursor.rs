use crate::btree::Node;
use crate::row::RowBytes;
use crate::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    pub page_idx: usize,
    pub cell_idx: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn new_at_table_start(table: &'a mut Table) -> Self {
        let node = table.pager.get_page(table.root_page_num).unwrap();
        let page_idx = table.root_page_num;
        let end_of_table = match node.as_ref() {
            Node::LeafNode(nd) => nd.cells.is_empty(),
            _ => unreachable!(),
        };
        Self {
            table,
            page_idx,
            cell_idx: 0,
            end_of_table,
        }
    }

    pub fn new_at_table_end(table: &'a mut Table) -> Self {
        let node = table.pager.get_page(table.root_page_num).unwrap();
        let cell_idx = match node.as_ref() {
            Node::LeafNode(nd) => nd.cells.len(),
            _ => unreachable!(),
        };
        let page_idx = table.root_page_num;
        Self {
            table,
            page_idx,
            cell_idx,
            end_of_table: true,
        }
    }

    pub fn read_row_bytes(&mut self, buf: &mut RowBytes) {
        let node = self.table.pager.get_page(self.page_idx).unwrap();
        match node.as_ref() {
            Node::LeafNode(nd) => nd.read_cell_value(self.cell_idx, buf),
            _ => unreachable!(),
        }
    }

    pub fn write_row_bytes(&mut self, buf: &RowBytes) {
        let node = self.table.pager.get_page(self.page_idx).unwrap();
        match node.as_mut() {
            Node::LeafNode(nd) => {
                if self.end_of_table {
                    self.table.insert_row(buf).unwrap();
                } else {
                    nd.write_cell_value(self.cell_idx, buf)
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn advance(&mut self) {
        self.cell_idx += 1;
        let node = self.table.pager.get_page(self.page_idx).unwrap();
        self.end_of_table = match node.as_ref() {
            Node::LeafNode(nd) => self.cell_idx >= nd.cells.len(),
            _ => unreachable!(),
        };
    }
}
