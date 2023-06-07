// pub mod intern;
pub mod intern2;
// pub mod leaf;
pub mod leaf2;
// pub mod node;
pub mod node2;
mod utils;
// mod wrapper;

// pub use wrapper::{NodeRc, NodeWk};

use std::fmt;

#[allow(unused)]
#[derive(Debug)]
pub enum BtreeErr {
  EmptyNodeRc,
}

impl fmt::Display for BtreeErr {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{self:?}")
  }
}

impl std::error::Error for BtreeErr {}
