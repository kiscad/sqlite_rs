use crate::btree::{InternalNode, LeafNode, Node, PageKey};
use crate::error::ExecErr;
use crate::pager::Pager;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;
// const CELL_SIZE: usize = row::ROW_SIZE + btree::CELL_KEY_SIZE;
// pub const PAGE_MAX_ROWS: usize = (pager::PAGE_SIZE - btree::LEAF_NODE_HEADER_SIZE) / CELL_SIZE;

pub struct Table {
    pub pager: Pager,
    pub root_idx: usize,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, ExecErr> {
        let mut pager = Pager::open_pager(filename)?;

        if pager.num_pages == 0 {
            let node = Node::LeafNode(LeafNode::new(true));
            pager.insert_node(node)?;
        }

        Ok(Self { root_idx: 0, pager })
    }

    pub fn close_db(&mut self) {
        for i in 0..self.pager.num_pages {
            self.pager
                .flush_pager(i)
                .unwrap_or_else(|_| std::process::exit(1));
        }
    }

    pub fn find_start_leaf_node(&mut self, page_idx: usize) -> usize {
        let node = self.get_node_mut(page_idx).unwrap();
        let page_idx_next;
        match node {
            Node::LeafNode(_) => return page_idx,
            Node::InternalNode(nd) => page_idx_next = nd.children[0].page,
        }
        self.find_start_leaf_node(page_idx_next as usize)
    }

    pub fn get_node_mut(&mut self, page_idx: usize) -> Result<&mut Node, ExecErr> {
        let node_opt = self.pager.get_node_mut(page_idx)?;
        if let Some(nd) = node_opt {
            return Ok(nd);
        }
        self.pager.load_node(page_idx)?;
        Ok(self.pager.get_node_mut(page_idx)?.unwrap())
        // match self.pager.get_node_mut(page_idx)? {
        //     Some(nd) => Ok(nd),
        //     None => {
        //         self.pager.load_node(page_idx)?;
        //         Ok(self.pager.get_node_mut(page_idx)?.unwrap())
        //     }
        // }
    }

    pub fn get_node(&self, page_idx: usize) -> &Node {
        self.pager.get_node(page_idx).unwrap().unwrap()
    }

    pub fn split_root_node(&mut self, mut right: Node) -> Result<(), ExecErr> {
        let old_root = self.get_node_mut(self.root_idx).unwrap();
        match old_root {
            Node::InternalNode(_) => todo!(),
            Node::LeafNode(_) => {
                let mut new_root = Node::InternalNode(InternalNode::new(
                    self.pager.num_pages as u32,
                    old_root.get_max_key(),
                    self.pager.num_pages as u32 + 1,
                ));
                old_root.set_root(false);
                old_root.set_parent(self.root_idx);
                new_root.set_root(true);
                self.pager.insert_node(new_root)?;
                self.pager
                    .pages
                    .swap(self.root_idx, self.pager.num_pages - 1);
                right.set_parent(self.root_idx);
                self.pager.insert_node(right)?;
                Ok(())
            }
        }
    }

    pub fn locate_page_and_cell(&mut self, page_idx: usize, key: u32) -> (usize, usize) {
        let leaf_idx = self.locate_leaf_node(page_idx, key);
        let Node::LeafNode(node) = self.get_node_mut(leaf_idx).unwrap() else { unreachable!() };
        (leaf_idx, node.find_place_for_new_cell(key as usize))
    }

    fn locate_leaf_node(&mut self, page_idx: usize, cell_key: u32) -> usize {
        let node = self.get_node_mut(page_idx).unwrap();
        match node {
            Node::LeafNode(_) => page_idx,
            Node::InternalNode(nd) => {
                for PageKey { page, key } in &nd.children {
                    if cell_key <= *key {
                        return self.locate_leaf_node(*page as usize, cell_key);
                    }
                }
                self.locate_leaf_node(nd.right_child_page as usize, cell_key)
            }
        }
    }

    pub fn btree_to_string(&self, page_idx: usize) -> String {
        let mut res = String::new();
        let node = self.pager.get_node(page_idx).unwrap().unwrap_or_else(|| {
            self.pager.load_node(page_idx).unwrap();
            self.pager.get_node(page_idx).unwrap().unwrap()
        });

        match node {
            Node::LeafNode(nd) => res.push_str(&format!("{}", nd)),
            Node::InternalNode(nd) => {
                res.push_str(&format!("{}", nd));
                for PageKey { page, .. } in &nd.children {
                    let s: String = self
                        .btree_to_string(*page as usize)
                        .lines()
                        .map(|s| format!("  {}\n", s))
                        .collect();
                    res.push_str(&s);
                }
            }
        }
        res
    }
}
