use crate::btree::node::Node;
use crate::btree::NodeRc;
use crate::error::ExecErr;
use crate::row::RowBytes;
use crate::Table;

pub struct Cursor<'a> {
  table: &'a mut Table,
  pub node: NodeRc,
  pub cell_idx: usize,
  pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
  pub fn new_at_table_start(table: &'a mut Table) -> Self {
    let node = table.find_start_leaf_node().unwrap();
    let end_of_table = table.is_empty();
    Self { table,
           node,
           cell_idx: 0,
           end_of_table }
  }

  pub fn new_by_key(table: &'a mut Table, key: usize) -> Self {
    let node = table.find_leaf_by_key(key);
    let cell_idx = node.get_with(|nd| nd.to_leaf_ref().find_place_for_new_cell(key));
    Self {
            table,
            node,
            cell_idx,
            end_of_table: true, // TODO
        }
  }

  pub fn read_row(&self, buf: &mut RowBytes) -> Result<(), ExecErr> {
    self.node
        .get_with(|nd| nd.to_leaf_ref().read_cell(self.cell_idx, buf));
    Ok(())
  }

  pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
    let res = self.node
                  .set_with(|nd| nd.to_leaf_mut().insert_cell(self.cell_idx, key, row));
    match res {
      Err(ExecErr::LeafNodeFull(_)) => self.split_leaf_and_insert_row(key, row),
      other => other,
    }
  }

  fn split_leaf_and_insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
    let leaf_new = self.node.set_with(|nd| {
                              nd.to_leaf_mut()
                                .insert_cell_and_split(self.cell_idx, key, row)
                            });

    self.table
        .insert_leaf_node(NodeRc::clone(&self.node), NodeRc::new(Node::Leaf(leaf_new)))?;
    Ok(())
  }

  pub fn advance(&mut self) -> Result<(), ExecErr> {
    self.cell_idx += 1;
    let cell_nums = self.node.get_with(|nd| nd.to_leaf_ref().cells.len());
    if self.cell_idx == cell_nums {
      let x = self.node.get_with(|nd| {
                         if let Some(next) = nd.to_leaf_ref().next_leaf.as_ref() {
                           let x = next.node.as_ref().unwrap();
                           let x = x.upgrade().unwrap();
                           Some(x)
                         } else {
                           None
                         }
                       });
      if let Some(nd) = x {
        self.node = nd;
        self.cell_idx = 0;
      }
    }
    self.end_of_table = self.cell_idx >= self.node.get_with(|nd| nd.to_leaf_ref().cells.len());
    Ok(())
  }
}
