// use crate::btree::{Child, Leaf, Node, NodeRc};
use crate::btree::intern::{Child, Intern};
use crate::btree::leaf::{Leaf, NextLeaf};
use crate::btree::node::{Node, NodeRc2};
use crate::error::ExecErr;
use crate::pager::Pager;
use std::cell::RefCell;
use std::path::Path;

// pub const TABLE_MAX_PAGES: usize = 100;

pub struct Table {
    pub pager: RefCell<Pager>,
    pub root: NodeRc2,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, ExecErr> {
        let mut pager = Pager::open_database(filename)?;

        let root = if pager.num_pages == 0 {
            Node::Leaf(Leaf::new_root_leaf())
        } else {
            // the root node is always in page 0
            pager.load_node_from_page(0)?
        };

        Ok(Self {
            root: NodeRc2::new(root),
            pager: RefCell::new(pager),
        })
    }

    pub fn close_db(&self) -> Result<(), ExecErr> {
        self.write_btree_rec(&self.root)
    }

    fn write_btree_rec(&self, node: &NodeRc2) -> Result<(), ExecErr> {
        if node.is_none() {
            return Ok(());
        }
        let buf = node.serialize();
        let page_idx = node.get_page_idx();
        self.pager.borrow_mut().write_page(page_idx, &buf)?;

        node.do_with_inner(|nd| {
            if let Node::Intern(nd) = nd {
                for Child { node, .. } in &nd.children {
                    self.write_btree_rec(node)?;
                }
            }
            Ok(())
        })
    }

    pub fn insert_leaf_node(
        &self,
        leaf_prev: NodeRc2,
        mut leaf_new: NodeRc2,
    ) -> Result<(), ExecErr> {
        if leaf_prev.is_root() {
            // initialize a new root internal node.
            let mut root_new = Intern::new_root();
            let child_prev = {
                let page = leaf_prev.get_page_idx() as u32;
                let key = leaf_prev.do_with_inner(|nd| nd.to_leaf_ref().get_max_key());
                let node = NodeRc2::clone(&leaf_prev);
                Child { page, key, node }
            };
            root_new.children.push(child_prev);

            let child_new = {
                let page = leaf_new.get_page_idx() as u32;
                let key = 0; // dummy value for the rightmost child
                let node = NodeRc2::clone(&leaf_new);
                Child { page, key, node }
            };
            root_new.children.push(child_new);

            root_new.is_root = true;
            root_new.page_idx = leaf_prev.get_page_idx();
            let root_new = NodeRc2::new(Node::Intern(root_new));

            // modify leaf_new
            let new_page_idx = self.pager.borrow().num_pages;
            self.pager.borrow_mut().num_pages += 1;
            let parent_new = root_new.new_parent_from_self();
            let next_leaf = leaf_prev.do_with_inner(|nd| nd.to_leaf_ref().next_leaf.clone());
            leaf_new.modify_inner_with(|nd| {
                nd.set_root(false);
                nd.set_page_idx(new_page_idx);
                nd.set_parent(parent_new);
                nd.to_leaf_mut().next_leaf = next_leaf;
            });

            // modify leaf_prev
            let new_page_idx = self.pager.borrow().num_pages;
            self.pager.borrow_mut().num_pages += 1;
            let parent_new = root_new.new_parent_from_self();
            let next_leaf = NextLeaf {
                page: leaf_new.get_page_idx() as u32,
                node: NodeRc2::downgrade(&leaf_new),
            };
            leaf_prev.modify_inner_with(|nd| {
                nd.set_root(false);
                nd.set_page_idx(new_page_idx);
                nd.set_parent(parent_new);
                nd.to_leaf_mut().next_leaf = next_leaf;
            });
            self.root.modify_inner_with(|nd| *nd = root_new.take());
            return Ok(());
        }
        // initialize the page_idx field of leaf_new
        leaf_new.set_page_idx(self.pager.borrow().num_pages);
        self.pager.borrow_mut().num_pages += 1;

        // initialize the next_leaf field of leaf_new
        let next_leaf = leaf_prev.do_with_inner(|nd| nd.to_leaf_ref().next_leaf.clone());
        leaf_new.modify_inner_with(|nd| nd.to_leaf_mut().next_leaf = next_leaf);

        // modify the next_leaf field of leaf_prev
        let next_leaf = NextLeaf {
            page: leaf_new.get_page_idx() as u32,
            node: NodeRc2::downgrade(&leaf_new),
        };
        leaf_prev.modify_inner_with(|nd| nd.to_leaf_mut().next_leaf = next_leaf);

        // insert leaf_new as a child in parent node.
        let page = leaf_new.get_page_idx() as u32;
        let key = leaf_new.do_with_inner(|nd| nd.to_leaf_ref().get_max_key());
        let child_new = Child {
            page,
            key,
            node: leaf_new,
        };
        let key_prev = leaf_prev.do_with_inner(|nd| nd.to_leaf_ref().get_max_key());
        leaf_prev.modify_inner_with(|nd| {
            let parent = nd.to_intern_mut();
            let (child_idx, _) = parent.get_child_by_key(key_prev as usize);
            parent.children[child_idx].key = key_prev;
            parent.insert_child(child_idx, &child_new) // TODO: intern node full.
        })?;
        Ok(())
    }

    // fn split_root_and_insert_node(&self, right: Leaf) -> Result<(), ExecErr> {
    //     let page_idx = self.pager.borrow().num_pages as u32;
    //
    //     let new_root = match self.pager.borrow_mut().get_node_mut(self.root_idx)? {
    //         Node::Intern(_) => todo!(),
    //         leaf_root => {
    //             let mut root =
    //                 Node::Intern(Intern::new(page_idx, leaf_root.get_max_key(), page_idx + 1));
    //             leaf_root.set_root(false);
    //             leaf_root.set_parent(self.root_idx);
    //             root.set_root(true);
    //             root
    //         }
    //     };
    //     let index = self.pager.borrow_mut().insert_node(new_root)?;
    //     self.pager.borrow_mut().swap_pages(self.root_idx, index)?;
    //     let mut right = Node::Leaf(right);
    //     right.set_parent(self.root_idx);
    //     self.pager.borrow_mut().insert_node(right)?;
    //     Ok(())
    // }

    pub fn find_leaf_by_key(&self, key: usize) -> NodeRc2 {
        self.find_leaf_by_key_rec(key, NodeRc2::clone(&self.root))
    }

    fn find_leaf_by_key_rec(&self, key: usize, node: NodeRc2) -> NodeRc2 {
        if node.is_leaf() {
            node
        } else {
            node.do_with_inner(|nd| {
                let (_, Child { page, node, .. }) = nd.to_intern_ref().get_child_by_key(key);
                if node.is_none() {
                    let n = self.load_node(*page as usize).unwrap();
                    NodeRc2::clone(node).set_inner(n);
                }
                self.find_leaf_by_key_rec(key, NodeRc2::clone(node))
            })
        }
        // node.do_with_inner(|node| match node {
        //     Node::Leaf(_) => NodeRc2::clone(&node),
        //     Node::Intern(nd) => {
        //         let (_, Child { page, node, .. }) = nd.get_child_by_key(key);
        //         if node.is_none() {
        //             let n = self.load_node(*page as usize).unwrap();
        //             let _ = node.set_inner(n);
        //         }
        //         self.find_leaf_by_key_rec(key, NodeRc2::clone(node))
        //     }
        // })
        // match node.inner_ref() {
        // }
    }

    pub fn find_start_leaf_node(&self) -> Result<NodeRc2, ExecErr> {
        self.find_start_leaf_node_rec(NodeRc2::clone(&self.root))
    }

    fn find_start_leaf_node_rec(&self, node: NodeRc2) -> Result<NodeRc2, ExecErr> {
        assert!(!node.is_none());
        if node.is_leaf() {
            Ok(node)
        } else {
            node.do_with_inner(|nd| {
                let Child { page, node, .. } = &nd.to_intern_ref().children[0];
                if node.is_none() {
                    let n = self.load_node(*page as usize).unwrap();
                    NodeRc2::clone(node).set_inner(n);
                }
                self.find_start_leaf_node_rec(NodeRc2::clone(node))
            })
        }
        // match node.inner_ref() {
        //     Node::Leaf(_) => Ok(node),
        //     Node::Intern(nd) => {
        //         let Child { page, node, .. } = &nd.children[0];
        //         if node.is_none() {
        //             let n = self.load_node(*page as usize).unwrap();
        //             let _ = node.set_inner(n);
        //         }
        //         self.find_start_leaf_node_rec(NodeRc2::clone(node))
        //     }
        // }
    }

    pub fn is_empty(&self) -> bool {
        self.root.do_with_inner(|nd| match nd {
            Node::Intern(_) => false,
            Node::Leaf(n) => n.cells.is_empty(),
        })
    }

    fn load_node(&self, page_idx: usize) -> Result<Node, ExecErr> {
        let buf = self.pager.borrow_mut().read_page(page_idx)?;
        let node = Node::new_from_page(&buf);
        Ok(node)
    }

    pub fn btree_to_str(&self) -> String {
        self.btree_to_str_rec(NodeRc2::clone(&self.root))
    }

    fn btree_to_str_rec(&self, node: NodeRc2) -> String {
        assert!(!node.is_none());
        let mut res = String::new();
        let node_str = node.do_with_inner(|nd| format!("{}", nd));
        res.push_str(&node_str);

        if node.is_leaf() {
            return res;
        }

        let s = node.modify_inner_with(|intern| {
            let mut string = String::new();
            for Child { page, node, .. } in &intern.to_intern_ref().children {
                if node.is_none() {
                    let n = self.load_node(*page as usize).unwrap();
                    NodeRc2::clone(node).set_inner(n);
                }
                let s: String = self
                    .btree_to_str_rec(NodeRc2::clone(node))
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
