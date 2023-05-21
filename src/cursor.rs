use crate::btree::{Node, LEAF_SPLIT_IDX};
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
        let page_idx = table.find_start_leaf_node(table.root_idx);
        let Node::LeafNode(node) = table.get_node_mut(page_idx).unwrap() else { unreachable!() };
        let end_of_table = node.cells.is_empty();
        Self {
            table,
            page_idx,
            cell_idx: 0,
            end_of_table,
        }
    }

    pub fn find(table: &'a mut Table, key: u32) -> Self {
        let (page_idx, cell_idx) = table.locate_page_and_cell(table.root_idx, key);
        Self {
            table,
            page_idx,
            cell_idx,
            end_of_table: true,
        }
    }

    pub fn read_row(&mut self, buf: &mut RowBytes) -> Result<(), ExecErr> {
        let Node::LeafNode(node) = self.table.get_node(self.page_idx) else { unreachable!() };
        node.read_cell(self.cell_idx, buf);
        Ok(())
    }

    pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let Node::LeafNode(node) = self.table.get_node_mut(self.page_idx)? else { unreachable!() };

        if node.get_cell_key(self.cell_idx).is_some_and(|k| k == key) {
            return Err(ExecErr::DuplicateKey("Error: Duplicate key.".to_string()));
        }

        match node.insert_cell(self.cell_idx, key, row) {
            Ok(()) => Ok(()),
            Err(ExecErr::LeafNodeFull(_)) => self.split_leaf_node_and_insert(key, row),
            _ => todo!(),
        }
    }

    fn split_leaf_node_and_insert(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let Node::LeafNode(old_node) = self.table.get_node_mut(self.page_idx)? else { unreachable!() };
        let new_node = old_node.insert_and_split(self.cell_idx, key, row);

        if old_node.is_root {
            self.table.split_root_node(Node::LeafNode(new_node))?;
        } else {
            todo!("Need to implement updating parent after split.")
        }

        Ok(())
    }

    pub fn advance(&mut self) -> Result<(), ExecErr> {
        self.cell_idx += 1;
        // let node = self.table.get_node_mut(self.page_idx)?;
        let Node::LeafNode(node) = self.table.get_node_mut(self.page_idx)? else { unreachable!() };
        self.end_of_table = self.cell_idx >= node.cells.len();
        Ok(())
    }
}
