use super::intern2::Intern;
use super::leaf2::Leaf;
use crate::btree::utils;
use crate::error::ExecErr;
use crate::pager::Page;
use std::io;

pub enum Node {
  Leaf(Leaf),
  Intern(Intern),
}

impl Node {
  pub fn get_is_root(&self) -> bool {
    match self {
      Self::Intern(nd) => nd.is_root,
      Self::Leaf(nd) => nd.is_root,
    }
  }

  pub fn set_is_root(&mut self, is_root: bool) {
    match self {
      Self::Intern(nd) => nd.is_root = is_root,
      Self::Leaf(nd) => nd.is_root = is_root,
    }
  }

  pub fn get_pg_idx(&self) -> usize {
    match self {
      Self::Intern(nd) => nd.pg_idx,
      Self::Leaf(nd) => nd.pg_idx,
    }
  }

  pub fn set_pg_idx(&mut self, pg_idx: usize) {
    match self {
      Self::Intern(nd) => nd.pg_idx = pg_idx,
      Self::Leaf(nd) => nd.pg_idx = pg_idx,
    }
  }

  pub fn get_parent(&self) -> Option<usize> {
    match self {
      Self::Intern(nd) => nd.parent,
      Self::Leaf(nd) => nd.parent,
    }
  }

  pub fn set_parent(&mut self, parent: Option<usize>) {
    match self {
      Self::Intern(nd) => nd.parent = parent,
      Self::Leaf(nd) => nd.parent = parent,
    }
  }

  pub fn new_from_page(pg_idx: usize, page: &Page) -> Self {
    let mut reader = io::Cursor::new(page);
    let is_leaf = utils::read_bool_from(&mut reader);
    if is_leaf {
      Self::Leaf(Leaf::new_from_page(pg_idx, page))
    } else {
      Self::Intern(Intern::new_from_page(pg_idx, page))
    }
  }

  pub fn serialize(&self) -> Page {
    match self {
      Self::Intern(nd) => nd.serialize(),
      Self::Leaf(nd) => nd.serialize(),
    }
  }

  pub fn as_leaf(&self) -> Result<&Leaf, ExecErr> {
    match self {
      Self::Intern(_) => Err(ExecErr::NodeError("Not Leaf".to_string())),
      Self::Leaf(nd) => Ok(nd),
    }
  }

  pub fn as_leaf_mut(&mut self) -> Result<&mut Leaf, ExecErr> {
    match self {
      Self::Intern(_) => Err(ExecErr::NodeError("Not Leaf".to_string())),
      Self::Leaf(nd) => Ok(nd),
    }
  }

  pub fn as_intern(&self) -> Result<&Intern, ExecErr> {
    match self {
      Self::Leaf(_) => Err(ExecErr::NodeError("Not Intern".to_string())),
      Self::Intern(nd) => Ok(nd),
    }
  }

  pub fn as_intern_mut(&mut self) -> Result<&mut Intern, ExecErr> {
    match self {
      Self::Leaf(_) => Err(ExecErr::NodeError("Not Intern".to_string())),
      Self::Intern(nd) => Ok(nd),
    }
  }
}
