use super::node::{Node, Parent};
use crate::pager::Page;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::rc::{Rc, Weak};

#[derive(Debug, Clone)]
pub struct NodeRc(Rc<RefCell<Node>>);

#[derive(Default, Clone, Debug)]
pub struct NodeWk(Weak<RefCell<Node>>);

impl NodeRc {
  pub fn new(node: Node) -> Self {
    Self(Rc::new(RefCell::new(node)))
  }

  pub fn is_root(&self) -> bool {
    self.0.borrow().is_root()
  }
  pub fn is_leaf(&self) -> bool {
    self.get_with(|nd| nd.is_leaf())
  }
  pub fn get_page_idx(&self) -> usize {
    self.0.borrow().get_page_idx()
  }
  pub fn set_page_idx(&mut self, page_idx: usize) {
    self.0.borrow_mut().set_page_idx(page_idx);
  }

  pub fn get_parent(&self) -> Option<NodeRc> {
    self.get_with(|nd| nd.get_parent())
  }

  pub fn downgrade(node: &Self) -> NodeWk {
    NodeWk(Rc::downgrade(&node.0))
  }

  pub fn new_parent_from_self(&self) -> Parent {
    Parent { page: self.get_page_idx() as u32,
             node: NodeRc::downgrade(self) }
  }

  pub fn get_with<F, T>(&self, mut f: F) -> T
    where F: FnMut(&Node) -> T
  {
    f(self.0.borrow().deref())
  }

  pub fn set_with<F, T>(&self, f: F) -> T
    where F: FnOnce(&mut Node) -> T
  {
    f(self.0.borrow_mut().deref_mut())
  }

  pub fn serialize(&self) -> Page {
    self.0.borrow().serialize()
  }

  pub fn clone(node: &Self) -> Self {
    Self(Rc::clone(&node.0))
  }
}

impl NodeWk {
  pub fn upgrade(&self) -> Option<NodeRc> {
    Some(NodeRc(self.0.upgrade()?))
  }
}
