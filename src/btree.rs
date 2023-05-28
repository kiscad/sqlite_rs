pub mod intern;
pub mod leaf;
pub mod node;

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
