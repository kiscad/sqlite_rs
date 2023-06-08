use super::intern::Intern;
use super::leaf::Leaf;
use crate::btree::utils;
use crate::error::ExecErr;
use crate::pager::Page;
use std::{fmt, io};

pub enum Node {
  Leaf(Leaf),
  Intern(Intern),
}

impl Node {
  pub fn is_leaf(&self) -> bool {
    match self {
      Self::Intern(_) => false,
      Self::Leaf(_) => true,
    }
  }

  #[allow(unused)]
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

  #[allow(unused)]
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

  pub fn new_from_page(page: &Page) -> Self {
    let mut reader = io::Cursor::new(page);
    let is_leaf = utils::read_bool_from(&mut reader);
    if is_leaf {
      Self::Leaf(Leaf::new_from_page(page))
    } else {
      Self::Intern(Intern::new_from_page(page))
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

impl fmt::Display for Node {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Intern(nd) => write!(f, "{}", nd),
      Self::Leaf(nd) => write!(f, "{}", nd),
    }
  }
}
