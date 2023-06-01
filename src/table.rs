// use crate::btree::{Child, Leaf, Node, NodeRc};
use crate::btree::intern::{Child, Intern};
use crate::btree::leaf::{Leaf, NextLeaf};
use crate::btree::node::{Node, Parent};
use crate::btree::NodeRc;
use crate::error::ExecErr;
use crate::pager::Pager;
use std::cell::RefCell;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;

pub struct Table {
    pub pager: RefCell<Pager>,
    pub root: NodeRc,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, ExecErr> {
        let mut pager = Pager::open_database(filename)?;

        let root = if pager.num_pages == 0 {
            pager.num_pages += 1;
            Node::Leaf(Leaf::new_root_leaf())
        } else {
            // the root node is always in page 0
            pager.load_node_from_page(0)?
        };
        let root = NodeRc::new(root);
        let _ = pager.pages[0].insert(NodeRc::clone(&root));

        Ok(Self {
            root,
            pager: RefCell::new(pager),
        })
    }

    pub fn close_db(&self) -> Result<(), ExecErr> {
        self.write_btree_rec(&self.root)
    }

    fn write_btree_rec(&self, node: &NodeRc) -> Result<(), ExecErr> {
        let buf = node.serialize();
        let page_idx = node.get_page_idx();
        self.pager.borrow_mut().write_page(page_idx, &buf)?;

        node.get_with(|nd| {
            if let Node::Intern(nd) = nd {
                for Child { node, .. } in &nd.children {
                    if let Some(node) = node {
                        self.write_btree_rec(node)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn insert_leaf_node_for_leaf_root(
        &mut self,
        leaf_prev: NodeRc,
        leaf_new: NodeRc,
    ) -> Result<(), ExecErr> {
        let page_idx_root = leaf_prev.get_page_idx();
        let page_idx_prev = self.pager.borrow().num_pages;
        self.pager.borrow_mut().num_pages += 1;
        let page_idx_new = self.pager.borrow().num_pages;
        self.pager.borrow_mut().num_pages += 1;

        // setup a new root internal node.
        let root_new = NodeRc::new(Node::Intern(Intern::new_root()));
        root_new.set_with(|nd| {
            nd.set_root(true);
            nd.set_page_idx(page_idx_root);
            let child_prev = {
                let page = page_idx_prev as u32;
                let key = leaf_prev.get_with(|nd| nd.to_leaf_ref().get_max_key());
                let node = Some(NodeRc::clone(&leaf_prev));
                Child { page, key, node }
            };
            nd.to_intern_mut().children.push(child_prev);
            let child_new = {
                let page = page_idx_new as u32;
                let key = 0; // dummy value for the rightmost child
                let node = Some(NodeRc::clone(&leaf_new));
                Child { page, key, node }
            };
            nd.to_intern_mut().children.push(child_new);
        });

        // update the leaf_new
        leaf_new.set_with(|nd| {
            let parent_new = root_new.new_parent_from_self();
            let next_leaf = leaf_prev.get_with(|nd| nd.to_leaf_ref().next_leaf.clone());
            nd.set_root(false);
            nd.set_page_idx(page_idx_new);
            nd.set_parent(parent_new);
            nd.to_leaf_mut().next_leaf = next_leaf;
        });

        // update the leaf_prev
        leaf_prev.set_with(|nd| {
            let parent_new = root_new.new_parent_from_self();
            let next_leaf = NextLeaf {
                page: leaf_new.get_page_idx() as u32,
                node: Some(NodeRc::downgrade(&leaf_new)),
            };
            nd.set_root(false);
            nd.set_page_idx(page_idx_prev);
            nd.set_parent(parent_new);
            let _ = nd.to_leaf_mut().next_leaf.insert(next_leaf);
        });

        self.root = root_new;
        Ok(())
    }

    pub fn insert_leaf_node(&mut self, leaf_prev: NodeRc, leaf_new: NodeRc) -> Result<(), ExecErr> {
        if leaf_prev.is_root() {
            return self.insert_leaf_node_for_leaf_root(leaf_prev, leaf_new);
        }
        // initialize the leaf_new
        leaf_new.set_with(|nd| {
            nd.set_page_idx(self.pager.borrow().num_pages);
            self.pager.borrow_mut().num_pages += 1;
            let next_leaf = leaf_prev.get_with(|nd| nd.to_leaf_ref().next_leaf.clone());
            nd.to_leaf_mut().next_leaf = next_leaf
        });

        // modify the next_leaf field of leaf_prev
        leaf_prev.set_with(|nd| {
            let next_leaf = NextLeaf {
                page: leaf_new.get_page_idx() as u32,
                node: Some(NodeRc::downgrade(&leaf_new)),
            };
            let _ = nd.to_leaf_mut().next_leaf.insert(next_leaf);
        });

        // insert leaf_new and update the parent.
        let parent = leaf_prev.get_parent().unwrap();
        parent.set_with(|nd| {
            let page = leaf_new.get_page_idx() as u32;
            let key = leaf_new.get_with(|nd| nd.to_leaf_ref().get_max_key());
            let child_new = Child {
                page,
                key,
                node: Some(NodeRc::clone(&leaf_new)),
            };
            let key_prev = leaf_prev.get_with(|nd| nd.to_leaf_ref().get_max_key());
            let intern = nd.to_intern_mut();
            let child_idx = intern.search_child_by_key(key_prev as usize);
            intern.children[child_idx].key = key_prev;
            intern.insert_child(child_idx + 1, &child_new)?;
            Ok(())
        })?;

        // update the parent of leaf_new
        leaf_new.set_with(|nd| {
            let parent = Parent {
                page: parent.get_page_idx() as u32,
                node: NodeRc::downgrade(&parent),
            };
            nd.set_parent(parent)
        });

        Ok(())
    }

    pub fn find_leaf_by_key(&self, key: usize) -> NodeRc {
        self.find_leaf_by_key_rec(key, &self.root)
    }

    fn find_leaf_by_key_rec(&self, key: usize, node: &NodeRc) -> NodeRc {
        if node.is_leaf() {
            NodeRc::clone(node)
        } else {
            let intern_page_idx = node.get_page_idx();
            node.set_with(|nd| {
                nd.to_intern_mut().set_child_by_key_with(key, |ch| {
                    if ch.node.is_none() {
                        let n = self.load_node(ch.page as usize, intern_page_idx).unwrap();
                        let _ = ch.node.insert(n);
                    }
                    self.find_leaf_by_key_rec(key, ch.node.as_ref().unwrap())
                })
            })
        }
    }

    pub fn find_start_leaf_node(&self) -> Result<NodeRc, ExecErr> {
        self.find_start_leaf_node_rec(&self.root)
    }

    fn find_start_leaf_node_rec(&self, node: &NodeRc) -> Result<NodeRc, ExecErr> {
        if node.is_leaf() {
            Ok(NodeRc::clone(node))
        } else {
            let intern_page = node.get_page_idx();
            node.set_with(|nd| {
                let Child { page, node, .. } = &mut nd.to_intern_mut().children[0];
                if node.is_none() {
                    let n = self.load_node(*page as usize, intern_page).unwrap();
                    let _ = node.insert(n);
                }
                self.find_start_leaf_node_rec(node.as_ref().unwrap())
            })
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root.get_with(|nd| match nd {
            Node::Intern(_) => false,
            Node::Leaf(n) => n.cells.is_empty(),
        })
    }

    fn load_node(&self, page_idx: usize, parent_page: usize) -> Result<NodeRc, ExecErr> {
        let node = self.pager.borrow_mut().read_node(page_idx)?;
        node.set_with(|nd| {
            nd.set_page_idx(page_idx);
            let parent = Parent {
                page: parent_page as u32,
                node: NodeRc::downgrade(self.pager.borrow().pages[parent_page].as_ref().unwrap()),
            };
            nd.set_parent(parent)
        });
        Ok(node)
    }

    pub fn btree_to_str(&self) -> String {
        self.btree_to_str_rec(&self.root)
    }

    fn btree_to_str_rec(&self, node: &NodeRc) -> String {
        let mut res = String::new();
        let node_str = node.get_with(|nd| format!("{}", nd));
        res.push_str(&node_str);

        if node.is_leaf() {
            return res;
        }

        let intern_page = node.get_page_idx();
        let s = node.set_with(|intern| {
            let mut string = String::new();
            for Child { page, node, .. } in &mut intern.to_intern_mut().children {
                if node.is_none() {
                    let n = self.load_node(*page as usize, intern_page).unwrap();
                    let _ = node.insert(n);
                }
                let s: String = self
                    .btree_to_str_rec(node.as_ref().unwrap())
                    .lines()
                    .map(|s| format!("  {}\n", s))
                    .collect();
                string.push_str(&s);
            }
            string
        });
        res.push_str(&s);
        res
    }
}
