use super::intern::Intern;
use super::leaf::Leaf;
use super::wrapper::{NodeRc, NodeWk};
use crate::error::ExecErr;
use crate::pager::Page;
use std::fmt;
use std::io::{self, Read};

#[derive(Default, Clone, Debug)]
pub struct Parent {
  pub page: u32,
  pub node: NodeWk,
}

impl Parent {
  pub fn new(page: u32) -> Self {
    Self { page,
           node: NodeWk::default() }
  }
}

#[derive(Debug)]
pub enum Node {
  Intern(Intern),
  Leaf(Leaf),
}

impl Node {
  pub fn get_page_idx(&self) -> usize {
    match self {
      Self::Leaf(nd) => nd.page_idx,
      Self::Intern(nd) => nd.page_idx,
    }
  }

  pub fn set_page_idx(&mut self, val: usize) {
    match self {
      Self::Leaf(nd) => nd.page_idx = val,
      Self::Intern(nd) => nd.page_idx = val,
    }
  }

  pub fn get_parent(&self) -> Option<NodeRc> {
    match self {
      Self::Intern(nd) => nd.parent.as_ref().map(|x| x.node.upgrade().unwrap()),
      Self::Leaf(nd) => nd.parent.as_ref().map(|x| x.node.upgrade().unwrap()),
    }
  }

  pub fn set_parent(&mut self, parent: Parent) {
    match self {
      Self::Intern(nd) => nd.parent.insert(parent),
      Self::Leaf(nd) => nd.parent.insert(parent),
    };
  }

  pub fn is_root(&self) -> bool {
    match self {
      Self::Leaf(nd) => nd.is_root,
      Self::Intern(nd) => nd.is_root,
    }
  }

  pub fn is_leaf(&self) -> bool {
    match self {
      Self::Leaf(_) => true,
      Self::Intern(_) => false,
    }
  }

  pub fn set_root(&mut self, is_root: bool) {
    match self {
      Self::Intern(nd) => nd.is_root = is_root,
      Self::Leaf(nd) => nd.is_root = is_root,
    }
  }

  pub fn serialize(&self) -> Page {
    match self {
      Node::Leaf(nd) => nd.serialize(),
      Node::Intern(nd) => nd.serialize(),
    }
  }

  pub fn new_from_page(page: &Page) -> Self {
    let mut reader = io::Cursor::new(page);
    let mut is_leaf = [0; 1];
    reader.read_exact(&mut is_leaf).unwrap();
    let is_leaf = is_leaf[0] != 0;

    if is_leaf {
      Self::Leaf(Leaf::new_from_page(page))
    } else {
      Self::Intern(Intern::new_from_page(page))
    }
  }

  pub fn get_max_key(&self) -> u32 {
    match self {
      Self::Intern(nd) => nd.children[nd.children.len() - 1].key,
      Self::Leaf(nd) => nd.cells[nd.cells.len() - 1].key,
    }
  }

  pub fn try_into_leaf(&self) -> Result<&Leaf, ExecErr> {
    match self {
      Self::Leaf(nd) => Ok(nd),
      Self::Intern(_) => Err(ExecErr::NodeError("Error: It's a Internal node.".to_string())),
    }
  }

  pub fn try_into_leaf_mut(&mut self) -> Result<&mut Leaf, ExecErr> {
    match self {
      Self::Leaf(nd) => Ok(nd),
      Self::Intern(_) => Err(ExecErr::NodeError("Error: It's a Internal node.".to_string())),
    }
  }

  pub fn to_leaf_ref(&self) -> &Leaf {
    match self {
      Self::Leaf(nd) => nd,
      Self::Intern(_) => panic!(),
    }
  }

  pub fn to_leaf_mut(&mut self) -> &mut Leaf {
    match self {
      Self::Leaf(nd) => nd,
      Self::Intern(_) => panic!(),
    }
  }

  pub fn to_intern_ref(&self) -> &Intern {
    match self {
      Self::Intern(nd) => nd,
      Self::Leaf(_) => panic!(),
    }
  }

  pub fn to_intern_mut(&mut self) -> &mut Intern {
    match self {
      Self::Intern(nd) => nd,
      Self::Leaf(_) => panic!(),
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
