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
        let node = table.pager.get_node(table.root_page_num).unwrap();
        let page_idx = table.root_page_num;
        let end_of_table = match node {
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

    pub fn read_row(&mut self, buf: &mut RowBytes) -> Result<(), ExecErr> {
        let Node::LeafNode(node) = self.table.pager.get_node(self.page_idx)? else { unreachable!() };
        node.read_cell(self.cell_idx, buf);
        Ok(())
    }

    pub fn update_row(&mut self, key: u32, buf: &RowBytes) -> Result<(), ExecErr> {
        let Node::LeafNode(node) = self.table.pager.get_node(self.page_idx)? else { unreachable!() };

        assert!(node.get_cell_key(self.cell_idx).is_some_and(|k| k == key));
        node.update_cell(self.cell_idx, buf);
        Ok(())
    }

    pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let Node::LeafNode(node) = self.table.pager.get_node(self.page_idx)? else { unreachable!() };
        // let Node::LeafNode(node) = self.table.pager.get_node(self.page_idx)?;

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
        let Node::LeafNode(old_node) = self.table.pager.get_node(self.page_idx)? else { unreachable!() };
        let new_node = old_node.insert_and_split(self.cell_idx, key, row);

        if old_node.is_root {
            self.table
                .pager
                .split_root_node(self.page_idx, Node::LeafNode(new_node))?;
        } else {
            todo!("Need to implement updating parent after split.")
        }

        Ok(())
    }

    pub fn advance(&mut self) -> Result<(), ExecErr> {
        self.cell_idx += 1;
        let Node::LeafNode(node) = self.table.pager.get_node(self.page_idx)? else { unreachable!() };
        self.end_of_table = self.cell_idx >= node.cells.len();
        Ok(())
    }
}
