use super::intern::Intern;
use super::leaf::Leaf;
use crate::error::ExecErr;
use crate::pager::Page;
use std::cell::RefCell;
use std::fmt;
use std::io::{self, Read};
use std::rc::{Rc, Weak};

#[derive(Debug, Clone)]
pub struct NodeRc(Rc<RefCell<Option<Node>>>);

impl NodeRc {
    pub fn default() -> Self {
        Self(Rc::new(RefCell::new(None)))
    }
    pub fn new(node: Node) -> Self {
        Self(Rc::new(RefCell::new(Some(node))))
    }
    pub fn is_none(&self) -> bool {
        self.0.borrow().is_none()
    }
    pub fn is_root(&self) -> bool {
        self.0.borrow().as_ref().map(|x| x.is_root()).unwrap()
    }
    pub fn is_leaf(&self) -> bool {
        self.get_with(|nd| nd.is_leaf())
    }
    pub fn get_page_idx(&self) -> usize {
        self.0.borrow().as_ref().map(|x| x.get_page_idx()).unwrap()
    }
    pub fn set_page_idx(&mut self, page_idx: usize) {
        self.0
            .borrow_mut()
            .as_mut()
            .map(|x| x.set_page_idx(page_idx))
            .unwrap();
    }

    pub fn get_parent(&self) -> NodeRc {
        self.get_with(|nd| nd.get_parent())
    }

    pub fn downgrade(node: &Self) -> NodeWk {
        NodeWk(Rc::downgrade(&node.0))
    }

    pub fn new_parent_from_self(&self) -> Parent {
        Parent {
            page: self.get_page_idx() as u32,
            node: NodeRc::downgrade(self),
        }
    }

    pub fn take(self) -> Node {
        self.0.take().unwrap()
    }

    pub fn get_with<F, T>(&self, mut f: F) -> T
    where
        F: FnMut(&Node) -> T,
    {
        f(self.0.borrow().as_ref().unwrap())
    }

    pub fn set_with<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut Node) -> T,
    {
        f(self.0.borrow_mut().as_mut().unwrap())
    }

    pub fn set_inner(&mut self, node: Node) {
        let _ = self.0.borrow_mut().insert(node);
    }

    pub fn serialize(&self) -> Page {
        self.0.borrow().as_ref().map(|x| x.serialize()).unwrap()
    }

    pub fn clone(node: &Self) -> Self {
        Self(Rc::clone(&node.0))
    }
}

#[derive(Default, Clone, Debug)]
pub struct NodeWk(Weak<RefCell<Option<Node>>>);

impl NodeWk {
    pub fn upgrade(&self) -> Option<NodeRc> {
        Some(NodeRc(self.0.upgrade()?))
    }
}

#[derive(Default, Clone, Debug)]
pub struct Parent {
    pub page: u32,
    pub node: NodeWk,
}

impl Parent {
    pub fn new(page: u32) -> Self {
        Self {
            page,
            node: NodeWk::default(),
        }
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

    fn get_parent(&self) -> NodeRc {
        match self {
            Self::Intern(nd) => nd.parent.node.upgrade().unwrap(),
            Self::Leaf(nd) => nd.parent.node.upgrade().unwrap(),
        }
    }

    pub fn set_parent(&mut self, parent: Parent) {
        match self {
            Self::Intern(nd) => nd.parent = parent,
            Self::Leaf(nd) => nd.parent = parent,
        }
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
            Self::Intern(_) => Err(ExecErr::NodeError(
                "Error: It's a Internal node.".to_string(),
            )),
        }
    }

    pub fn try_into_leaf_mut(&mut self) -> Result<&mut Leaf, ExecErr> {
        match self {
            Self::Leaf(nd) => Ok(nd),
            Self::Intern(_) => Err(ExecErr::NodeError(
                "Error: It's a Internal node.".to_string(),
            )),
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
