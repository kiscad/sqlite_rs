use crate::btree::{InternalNode, LeafNode, Node, PageKey};
use crate::error::ExecErr;
use crate::pager::Pager;
use crate::row::RowBytes;
use std::cell::RefCell;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;
// const CELL_SIZE: usize = row::ROW_SIZE + btree::CELL_KEY_SIZE;
// pub const PAGE_MAX_ROWS: usize = (pager::PAGE_SIZE - btree::LEAF_NODE_HEADER_SIZE) / CELL_SIZE;

pub struct Table {
    pub pager: RefCell<Pager>,
    pub root_idx: usize,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, ExecErr> {
        let mut pager = Pager::open_database(filename)?;

        if pager.num_pages == 0 {
            let node = Node::LeafNode(LeafNode::new(true));
            pager.insert_node(node)?;
        }

        Ok(Self {
            root_idx: 0,
            pager: RefCell::new(pager),
        })
    }

    pub fn close_db(&mut self) {
        let num_pages = self.pager.borrow().num_pages;
        let mut _pager = self.pager.borrow_mut();
        for i in 0..num_pages {
            _pager
                .flush_pager(i)
                .unwrap_or_else(|_| std::process::exit(1));
        }
    }

    pub fn split_leaf_and_insert_row(
        &mut self,
        node_idx: usize,
        cell_idx: usize,
        key: u32,
        row: &RowBytes,
    ) -> Result<(), ExecErr> {
        let new_node = {
            let mut _pager = self.pager.borrow_mut();
            let Node::LeafNode(old_node) = _pager.get_node_mut(node_idx).unwrap() else { unreachable!() };
            old_node.insert_and_split(cell_idx, key, row)
        };
        self.split_and_insert_node(node_idx, Node::LeafNode(new_node))
    }

    pub fn split_and_insert_node(
        &mut self,
        node_idx: usize,
        new_node: Node,
    ) -> Result<(), ExecErr> {
        if node_idx == self.root_idx {
            // base case
            self.split_root_and_insert_node(new_node)
        } else {
            // recursive case
            todo!("Need to implement updating parent after split.")
        }
    }

    pub fn split_root_and_insert_node(&mut self, mut right: Node) -> Result<(), ExecErr> {
        let page_idx = self.pager.borrow().num_pages as u32;

        let new_root = {
            let mut binding = self.pager.borrow_mut();
            let old_root = binding.get_node_mut(self.root_idx).unwrap();

            match old_root {
                Node::InternalNode(_) => todo!(),
                Node::LeafNode(_) => {
                    let mut new_root = Node::InternalNode(InternalNode::new(
                        page_idx,
                        old_root.get_max_key(),
                        page_idx + 1,
                    ));
                    old_root.set_root(false);
                    old_root.set_parent(self.root_idx);
                    new_root.set_root(true);
                    new_root
                }
            }
        };
        let index = self.pager.borrow_mut().insert_node(new_root)?;
        self.pager.borrow_mut().swap_pages(self.root_idx, index)?;
        right.set_parent(self.root_idx);
        self.pager.borrow_mut().insert_node(right)?;
        Ok(())
    }

    pub fn find_page_and_cell_by_key(&self, key: u32) -> (usize, usize) {
        let leaf_idx = self.locate_leaf_node(self.root_idx, key);
        let mut _binding = self.pager.borrow_mut();
        let Node::LeafNode(node) = _binding.get_node(leaf_idx).unwrap() else { unreachable!() };
        (leaf_idx, node.find_place_for_new_cell(key as usize))
    }

    fn locate_leaf_node(&self, node_idx: usize, cell_key: u32) -> usize {
        let mut child_page_idx = None;
        {
            let mut _pager = self.pager.borrow_mut();
            let node = _pager.get_node(node_idx).unwrap();
            match node {
                Node::LeafNode(_) => return node_idx,
                Node::InternalNode(nd) => {
                    for PageKey { page, key } in &nd.children {
                        if cell_key <= *key {
                            child_page_idx = Some(*page);
                        }
                    }
                    if child_page_idx.is_none() {
                        child_page_idx = Some(nd.right_child_page);
                    }
                }
            }
        }
        if let Some(page_idx) = child_page_idx {
            self.locate_leaf_node(page_idx as usize, cell_key)
        } else {
            unreachable!()
        }
    }

    pub fn get_leaf_node_mut<F>(&mut self, node_idx: usize, mut f: F) -> Result<(), ExecErr>
    where
        F: FnMut(&mut LeafNode) -> Result<(), ExecErr>,
    {
        match self.pager.borrow_mut().get_node_mut(node_idx).unwrap() {
            Node::LeafNode(nd) => f(nd),
            Node::InternalNode(_) => panic!(),
        }
    }

    pub fn get_leaf_node<F>(&self, node_idx: usize, mut f: F) -> Result<(), ExecErr>
    where
        F: FnMut(&LeafNode) -> Result<(), ExecErr>,
    {
        match self.pager.borrow_mut().get_node(node_idx).unwrap() {
            Node::LeafNode(nd) => f(nd),
            Node::InternalNode(_) => panic!(),
        }
    }

    pub fn btree_to_string(&self, page_idx: usize) -> String {
        let mut res = String::new();

        let children_pages: Vec<u32> = {
            let mut _binding = self.pager.borrow_mut();
            let node = _binding.get_node(page_idx).unwrap();
            res.push_str(&format!("{}", node));

            match node {
                Node::LeafNode(_) => vec![],
                Node::InternalNode(nd) => {
                    let mut pages: Vec<_> = nd.children.iter().map(|x| x.page).collect();
                    pages.push(nd.right_child_page);
                    pages
                }
            }
        };

        for page_idx in children_pages {
            let s: String = self
                .btree_to_string(page_idx as usize)
                .lines()
                .map(|s| format!("  {}\n", s))
                .collect();
            // println!("{}", s);
            res.push_str(&s);
        }

        res
    }
}
