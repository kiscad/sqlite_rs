use super::intern::Intern;
use super::leaf::Leaf;
use crate::error::ExecErr;
use crate::pager::Page;
use std::cell::RefCell;
use std::fmt;
use std::io::{self, Read};
use std::rc::{Rc, Weak};

#[derive(Clone)]
pub struct NodeRc2(Rc<RefCell<Option<Node>>>);

impl NodeRc2 {
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
        self.do_with_inner(|nd| nd.is_leaf())
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

    pub fn get_parent(&self) -> NodeRc2 {
        self.do_with_inner(|nd| nd.get_parent())
    }

    pub fn downgrade(node: &Self) -> NodeWk2 {
        NodeWk2(Rc::downgrade(&node.0))
    }

    pub fn new_parent_from_self(&self) -> Parent {
        Parent {
            page: self.get_page_idx() as u32,
            node: NodeRc2::downgrade(self),
        }
    }

    pub fn take(self) -> Node {
        self.0.take().unwrap()
    }

    pub fn do_with_inner<F, T>(&self, mut f: F) -> T
    where
        F: FnMut(&Node) -> T,
    {
        f(self.0.borrow().as_ref().unwrap())
    }

    pub fn modify_inner_with<F, T>(&self, f: F) -> T
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

#[derive(Default, Clone)]
pub struct NodeWk2(Weak<RefCell<Option<Node>>>);

impl NodeWk2 {
    pub fn upgrade(&self) -> Option<NodeRc2> {
        Some(NodeRc2(self.0.upgrade()?))
    }
}

#[derive(Default)]
pub struct Parent {
    pub page: u32,
    pub node: NodeWk2,
}

impl Parent {
    pub fn new(page: u32) -> Self {
        Self {
            page,
            node: NodeWk2::default(),
        }
    }
}

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

    fn get_parent(&self) -> NodeRc2 {
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

    // pub fn set_parent(&mut self, parent: usize) {
    //     match self {
    //         Self::Leaf(nd) => nd.parent = parent as u32,
    //         Self::Intern(nd) => nd.parent = parent as u32,
    //     }
    // }

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

    // pub fn get_children(&self) -> Vec<u32> {
    //     match self {
    //         Self::Leaf(_) => vec![],
    //         Self::Intern(nd) => {
    //             let mut pages: Vec<u32> = nd.children.iter().map(|x| x.page).collect();
    //             pages.push(nd.right_child);
    //             pages
    //         }
    //     }
    // }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Intern(nd) => write!(f, "{}", nd),
            Self::Leaf(nd) => write!(f, "{}", nd),
        }
    }
}
