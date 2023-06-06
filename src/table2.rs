use crate::btree::leaf2::Leaf;
use crate::btree::node2::Node;
use crate::error::ExecErr;
use crate::pager2::Pager;
use std::path::Path;

const ROOT: usize = 0_usize;

pub struct Table {
  pager: Pager,
}

impl Table {
  pub fn open_db(fname: impl AsRef<Path>) -> Result<Self, ExecErr> {
    let mut pager = Pager::new(fname)?;

    if pager.size() == 0 {
      let root = Node::Leaf(Leaf::new(true, 0, None, None));
      pager.push_node(root)?;
    }

    Ok(Self { pager })
  }
}
