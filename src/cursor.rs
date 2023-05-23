use crate::error::ExecErr;
use crate::row::RowBytes;
use crate::Table;

pub struct Cursor<'a> {
    table: &'a mut Table,
    pub node_idx: usize,
    pub cell_idx: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn new_at_table_start(table: &'a mut Table) -> Self {
        // let page_idx = table.find_start_leaf_node(table.root_idx);
        // let Node::LeafNode(node) = table.get_node_mut(page_idx).unwrap() else { unreachable!() };
        // let end_of_table = node.cells.is_empty();
        let node_idx = 0;
        let end_of_table = false;
        Self {
            table,
            node_idx,
            cell_idx: 0,
            end_of_table,
        }
    }

    pub fn new_by_key(table: &'a mut Table, key: u32) -> Self {
        let (page_idx, cell_idx) = table.find_page_and_cell_by_key(key);
        Self {
            table,
            node_idx: page_idx,
            cell_idx,
            end_of_table: true,
        }
    }

    pub fn read_row(&mut self, buf: &mut RowBytes) -> Result<(), ExecErr> {
        self.table.get_leaf_node(self.node_idx, |nd| {
            nd.read_cell(self.cell_idx, buf);
            Ok(())
        })
    }

    pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
        let node_idx = self.node_idx;
        let cell_idx = self.cell_idx;
        let res = self.table.get_leaf_node_mut(node_idx, |nd| {
            if nd.get_cell_key(cell_idx).is_some_and(|k| k == key) {
                return Err(ExecErr::DuplicateKey("Error: Duplicate key.".to_string()));
            }
            nd.insert_cell(cell_idx, key, row)
        });
        match res {
            Ok(()) => Ok(()),
            Err(ExecErr::DuplicateKey(s)) => Err(ExecErr::DuplicateKey(s)),
            Err(ExecErr::LeafNodeFull(_)) => self
                .table
                .split_leaf_and_insert_row(node_idx, cell_idx, key, row),
            _ => todo!(),
        }
    }

    pub fn advance(&mut self) -> Result<(), ExecErr> {
        self.cell_idx += 1;
        self.table.get_leaf_node(self.node_idx, |nd| {
            self.end_of_table = self.cell_idx >= nd.cells.len();
            Ok(())
        })
    }
}
