use crate::btree::node::{Node, NodeRc2};
use crate::error::ExecErr;
use crate::row::RowBytes;
use crate::Table;

pub struct Cursor<'a> {
    table: &'a Table,
    pub node: NodeRc2,
    pub cell_idx: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn new_at_table_start(table: &'a Table) -> Self {
        Self {
            table,
            node: table.find_start_leaf_node().unwrap(),
            cell_idx: 0,
            end_of_table: table.is_empty(),
        }
    }

    pub fn new_by_key(table: &'a Table, key: usize) -> Self {
        let node = table.find_leaf_by_key(key);
        let cell_idx = node.do_with_inner(|nd| nd.to_leaf_ref().find_place_for_new_cell(key));
        Self {
            table,
            node,
            cell_idx,
            end_of_table: true, // TODO
        }
    }

    pub fn read_row(&self, buf: &mut RowBytes) -> Result<(), ExecErr> {
        self.node
            .do_with_inner(|nd| nd.to_leaf_ref().read_cell(self.cell_idx, buf));
        Ok(())
    }

    pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let res = self
            .node
            .modify_inner_with(|nd| nd.to_leaf_mut().insert_cell(self.cell_idx, key, row));
        match res {
            Err(ExecErr::LeafNodeFull(_)) => self.split_leaf_and_insert_row(key, row),
            other => other,
        }
    }

    fn split_leaf_and_insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let leaf_new = self.node.modify_inner_with(|nd| {
            nd.to_leaf_mut()
                .insert_cell_and_split(self.cell_idx, key, row)
        });

        self.table.insert_leaf_node(
            NodeRc2::clone(&self.node),
            NodeRc2::new(Node::Leaf(leaf_new)),
        )?;
        Ok(())
    }

    pub fn advance(&mut self) -> Result<(), ExecErr> {
        self.cell_idx += 1;
        let cell_nums = self.node.do_with_inner(|nd| nd.to_leaf_ref().cells.len());
        self.end_of_table = self.cell_idx >= cell_nums;
        Ok(())
    }
}
