use super::intern::Intern;
use super::leaf::Leaf;
use crate::error::ExecErr;
use crate::pager::Page;
use std::fmt;
use std::io::{self, Read};

pub enum Node {
    Intern(Intern),
    Leaf(Leaf),
}

impl Node {
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

    pub fn set_parent(&mut self, parent: usize) {
        match self {
            Self::Leaf(nd) => nd.parent = parent as u32,
            Self::Intern(nd) => nd.parent = parent as u32,
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

    pub fn get_children(&self) -> Vec<u32> {
        match self {
            Self::Leaf(_) => vec![],
            Self::Intern(nd) => {
                let mut pages: Vec<u32> = nd.children.iter().map(|x| x.page).collect();
                pages.push(nd.right_child_page);
                pages
            }
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
