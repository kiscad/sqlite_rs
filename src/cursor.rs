use crate::btree::Node;
use crate::error::ExecErr;
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

    pub fn find(table: &'a mut Table, key: u32) -> Self {
        let (page_idx, cell_idx) = table.find_cell(key);
        Self {
            table,
            page_idx,
            cell_idx,
            end_of_table: true,
        }
    }

    pub fn read_row(&mut self, buf: &mut RowBytes) {
        let node = self.table.pager.get_page(self.page_idx).unwrap();
        match node.as_ref() {
            Node::LeafNode(nd) => nd.read_cell(self.cell_idx, buf),
            _ => unreachable!(),
        }
    }

    pub fn update_row(&mut self, key: u32, buf: &RowBytes) -> Result<(), ExecErr> {
        let node = self.table.pager.get_page(self.page_idx).unwrap();
        match node.as_mut() {
            Node::LeafNode(nd) => {
                assert!(nd.get_cell_key(self.cell_idx).is_some_and(|k| k == key));
                nd.update_cell(self.cell_idx, buf);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let node = self.table.pager.get_page(self.page_idx).unwrap();
        match node.as_mut() {
            Node::LeafNode(nd) => {
                if nd.get_cell_key(self.cell_idx).is_some_and(|k| k == key) {
                    return Err(ExecErr::DuplicateKey("Error: Duplicate key.".to_string()));
                }
                nd.insert_cell(self.cell_idx, key, row)?;
            }
            _ => unreachable!(),
        }
        Ok(())
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
