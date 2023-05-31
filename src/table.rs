// use crate::btree::{Child, Leaf, Node, NodeRc};
use crate::btree::intern::{Child, Intern};
use crate::btree::leaf::{Leaf, NextLeaf};
use crate::btree::node::{Node, Parent};
use crate::btree::{NodeRc, NodeWk};
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
        let _ = pager.pages[0].insert(NodeRc::downgrade(&root));

        Ok(Self {
            root,
            pager: RefCell::new(pager),
        })
    }

    pub fn close_db(&self) -> Result<(), ExecErr> {
        self.write_btree_rec(&self.root)
    }

    fn write_btree_rec(&self, node: &NodeRc) -> Result<(), ExecErr> {
        if node.is_none() {
            return Ok(());
        }
        let buf = node.serialize();
        let page_idx = node.get_page_idx();
        self.pager.borrow_mut().write_page(page_idx, &buf)?;

        node.get_with(|nd| {
            if let Node::Intern(nd) = nd {
                for Child { node, .. } in &nd.children {
                    self.write_btree_rec(node)?;
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
                let node = NodeRc::clone(&leaf_prev);
                Child { page, key, node }
            };
            nd.to_intern_mut().children.push(child_prev);
            let child_new = {
                let page = page_idx_new as u32;
                let key = 0; // dummy value for the rightmost child
                let node = NodeRc::clone(&leaf_new);
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
                node: NodeRc::downgrade(&leaf_new),
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
                node: NodeRc::downgrade(&leaf_new),
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
                node: NodeRc::clone(&leaf_new),
            };
            let key_prev = leaf_prev.get_with(|nd| nd.to_leaf_ref().get_max_key());
            let inter = nd.to_intern_mut();
            let (child_idx, _) = inter.get_child_by_key(key_prev as usize);
            inter.children[child_idx].key = key_prev;
            inter.insert_child(child_idx + 1, &child_new)?;
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
        self.find_leaf_by_key_rec(key, NodeRc::clone(&self.root))
    }

    fn find_leaf_by_key_rec(&self, key: usize, node: NodeRc) -> NodeRc {
        if node.is_leaf() {
            node
        } else {
            node.get_with(|nd| {
                let (_, Child { page, node, .. }) = nd.to_intern_ref().get_child_by_key(key);
                if node.is_none() {
                    let parent_page = nd.get_page_idx();
                    let n = self.load_node(*page as usize, parent_page).unwrap();
                    NodeRc::clone(node).set_inner(n);
                }
                self.find_leaf_by_key_rec(key, NodeRc::clone(node))
            })
        }
    }

    pub fn find_start_leaf_node(&self) -> Result<NodeRc, ExecErr> {
        self.find_start_leaf_node_rec(NodeRc::clone(&self.root))
    }

    fn find_start_leaf_node_rec(&self, node: NodeRc) -> Result<NodeRc, ExecErr> {
        assert!(!node.is_none());
        if node.is_leaf() {
            Ok(node)
        } else {
            node.get_with(|nd| {
                let Child { page, node, .. } = &nd.to_intern_ref().children[0];
                if node.is_none() {
                    let parent_page = nd.get_page_idx();
                    let n = self.load_node(*page as usize, parent_page).unwrap();
                    NodeRc::clone(node).set_inner(n);
                }
                self.find_start_leaf_node_rec(NodeRc::clone(node))
            })
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root.get_with(|nd| match nd {
            Node::Intern(_) => false,
            Node::Leaf(n) => n.cells.is_empty(),
        })
    }

    fn load_node(&self, page_idx: usize, parent_page: usize) -> Result<Node, ExecErr> {
        let buf = self.pager.borrow_mut().read_page(page_idx)?;
        let mut node = Node::new_from_page(&buf);
        node.set_page_idx(page_idx);
        let parent = Parent {
            page: parent_page as u32,
            node: NodeWk::clone(self.pager.borrow().pages[parent_page].as_ref().unwrap()),
        };
        node.set_parent(parent);
        Ok(node) // TODO return NodeRc type
    }

    pub fn btree_to_str(&self) -> String {
        self.btree_to_str_rec(NodeRc::clone(&self.root))
    }

    fn btree_to_str_rec(&self, node: NodeRc) -> String {
        assert!(!node.is_none());
        let mut res = String::new();
        let node_str = node.get_with(|nd| format!("{}", nd));
        res.push_str(&node_str);

        if node.is_leaf() {
            return res;
        }
        let s = node.get_with(|intern| {
            let mut string = String::new();
            for Child { page, node, .. } in &intern.to_intern_ref().children {
                if node.is_none() {
                    let parent_page = intern.get_page_idx();
                    let n = self.load_node(*page as usize, parent_page).unwrap();
                    NodeRc::clone(node).set_inner(n);
                }
                let s: String = self
                    .btree_to_str_rec(NodeRc::clone(node))
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
