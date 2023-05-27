use crate::btree::{Intern, Leaf, Node};
use crate::error::ExecErr;
use crate::pager::Pager;
use crate::row::RowBytes;
use std::cell::RefCell;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;

pub struct Table {
    pub pager: RefCell<Pager>,
    pub root_idx: usize,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, ExecErr> {
        let mut pager = Pager::open_database(filename)?;

        if pager.num_pages == 0 {
            let node = Node::Leaf(Leaf::new(true));
            pager.insert_node(node)?;
        }

        Ok(Self {
            root_idx: 0,
            pager: RefCell::new(pager),
        })
    }

    pub fn close_db(&self) {
        let num_pages = self.pager.borrow().num_pages;
        let mut _pager = self.pager.borrow_mut();
        for i in 0..num_pages {
            _pager
                .flush_pager(i)
                .unwrap_or_else(|_| std::process::exit(1));
        }
    }

    pub fn split_leaf_and_insert_row(
        &self,
        leaf_idx: usize,
        cell_idx: usize,
        key: u32,
        row: &RowBytes,
    ) -> Result<(), ExecErr> {
        let new_leaf = self
            .pager
            .borrow_mut()
            .get_node_mut(leaf_idx)?
            .try_into_leaf_mut()?
            .insert_and_split(cell_idx, key, row);

        self.insert_leaf(leaf_idx, new_leaf)
    }

    fn insert_leaf(&self, leaf_idx: usize, new_leaf: Leaf) -> Result<(), ExecErr> {
        if leaf_idx == self.root_idx {
            // base case
            self.split_root_and_insert_node(new_leaf)
        } else {
            // recursive case
            todo!("Need to implement updating parent after split.")
        }
    }

    // fn split_leaf_root(&self)

    fn split_root_and_insert_node(&self, right: Leaf) -> Result<(), ExecErr> {
        let page_idx = self.pager.borrow().num_pages as u32;

        let new_root = match self.pager.borrow_mut().get_node_mut(self.root_idx)? {
            Node::Intern(_) => todo!(),
            leaf_root => {
                let mut root =
                    Node::Intern(Intern::new(page_idx, leaf_root.get_max_key(), page_idx + 1));
                leaf_root.set_root(false);
                leaf_root.set_parent(self.root_idx);
                root.set_root(true);
                root
            }
        };
        let index = self.pager.borrow_mut().insert_node(new_root)?;
        self.pager.borrow_mut().swap_pages(self.root_idx, index)?;
        let mut right = Node::Leaf(right);
        right.set_parent(self.root_idx);
        self.pager.borrow_mut().insert_node(right)?;
        Ok(())
    }

    pub fn find_page_and_cell_by_key(&self, key: u32) -> Result<(usize, usize), ExecErr> {
        let leaf_idx = self.locate_leaf_node(self.root_idx, key);
        let cell_idx = self
            .pager
            .borrow_mut()
            .get_node(leaf_idx)?
            .try_into_leaf()?
            .find_place_for_new_cell(key as usize);
        Ok((leaf_idx, cell_idx))
    }

    fn locate_leaf_node(&self, node_idx: usize, cell_key: u32) -> usize {
        let page_idx = match self.pager.borrow_mut().get_node(node_idx).unwrap() {
            Node::Leaf(_) => return node_idx,
            Node::Intern(nd) => nd.get_child_by(cell_key),
        };

        self.locate_leaf_node(page_idx as usize, cell_key)
    }

    pub fn get_leaf_node_mut<F>(&self, node_idx: usize, mut f: F) -> Result<(), ExecErr>
    where
        F: FnMut(&mut Leaf) -> Result<(), ExecErr>,
    {
        f(self
            .pager
            .borrow_mut()
            .get_node_mut(node_idx)?
            .try_into_leaf_mut()?)
    }

    pub fn get_leaf_node<F>(&self, node_idx: usize, mut f: F) -> Result<(), ExecErr>
    where
        F: FnMut(&Leaf) -> Result<(), ExecErr>,
    {
        f(self
            .pager
            .borrow_mut()
            .get_node(node_idx)?
            .try_into_leaf()?)
    }

    pub fn find_start_leaf_node(&self) -> Result<usize, ExecErr> {
        self.find_start_leaf_node_recur(self.root_idx)
    }

    fn find_start_leaf_node_recur(&self, page_idx: usize) -> Result<usize, ExecErr> {
        let child_start = match self.pager.borrow_mut().get_node(page_idx)? {
            Node::Leaf(_) => return Ok(page_idx),
            Node::Intern(nd) => nd.get_start_child(),
        };
        self.find_start_leaf_node_recur(child_start as usize)
    }

    pub fn is_empty(&self) -> bool {
        match self.pager.borrow_mut().get_node(self.root_idx).unwrap() {
            Node::Intern(_) => false,
            Node::Leaf(nd) => nd.cells.is_empty(),
        }
    }

    pub fn btree_to_string(&self, page_idx: usize) -> String {
        let mut res = String::new();

        res.push_str(&format!(
            "{}",
            self.pager.borrow_mut().get_node(page_idx).unwrap()
        ));

        let children_pages = self
            .pager
            .borrow_mut()
            .get_node(page_idx)
            .unwrap()
            .get_children();

        for page_idx in children_pages {
            let s: String = self
                .btree_to_string(page_idx as usize)
                .lines()
                .map(|s| format!("  {}\n", s))
                .collect();
            res.push_str(&s);
        }

        res
    }
}
