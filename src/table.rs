use crate::btree::{InternalNode, LeafNode, Node, PageKey};
use crate::error::ExecErr;
use crate::pager::Pager;
use std::path::Path;

pub const TABLE_MAX_PAGES: usize = 100;
// const CELL_SIZE: usize = row::ROW_SIZE + btree::CELL_KEY_SIZE;
// pub const PAGE_MAX_ROWS: usize = (pager::PAGE_SIZE - btree::LEAF_NODE_HEADER_SIZE) / CELL_SIZE;

pub struct Table {
    pub pager: Pager,
    pub root_page_num: usize,
}

impl Table {
    pub fn open_db(filename: impl AsRef<Path>) -> Result<Self, String> {
        let pager = Pager::open_pager(filename)?;

        Ok(Self {
            root_page_num: 0,
            pager,
        })
    }

    pub fn close_db(&mut self) {
        for i in 0..self.pager.pages.len() {
            if self.pager.pages[i].is_some() {
                self.pager
                    .flush_pager(i)
                    .unwrap_or_else(|_| std::process::exit(1));
            }
        }
    }

    pub fn find_cell(&mut self, key: u32) -> (usize, usize) {
        let page = self.pager.find_leaf_node(self.root_page_num, key);
        let Node::LeafNode(nd) = self.pager.get_node(page).unwrap() else { unreachable!() };
        (page, nd.find_place_for_new_cell(key as usize))

        // let root_node = self.pager.get_node(self.root_page_num).unwrap();
        // if root_node.is_leaf() {
        //     let Node::LeafNode(ref nd) = *root_node else { unreachable!()};
        //     Ok((self.root_page_num, nd.find_place_for_new_cell(key as usize)))
        // } else {
        //     let Node::InternalNode(ref nd) = *root_node else { unreachable!() };
        //     let (page, leaf) = self.search_leaf_node(key, nd)?;
        //     Ok((page as usize, leaf.find_place_for_new_cell(key as usize)))
        // }
        // match &root_node {
        //     Node::LeafNode(nd) => {
        //         Ok((self.root_page_num, nd.find_place_for_new_cell(key as usize)))
        //     }
        //     Node::InternalNode(nd) => {
        //         let (page, leaf) = self.search_leaf_node(key, nd)?;
        //         Ok((page as usize, leaf.find_place_for_new_cell(key as usize)))
        //         // unimplemented!("Need to implement searching an internal node.")
        //     }
        // }
    }

    // fn search_leaf_node<'a>(
    //     pager: &'a Pager,
    //     cell_key: u32,
    //     node: &'a InternalNode,
    // ) -> Result<(u32, &'a LeafNode), ExecErr> {
    //     let mut page_tag: Option<u32> = None;
    //     for PageKey { page, key } in &node.children {
    //         if cell_key <= *key {
    //             page_tag = Some(*page);
    //             break;
    //         }
    //     }
    //     let page = page_tag.unwrap_or(node.right_child_page);
    //     let nd = pager.get_node(page as usize)?;
    //     match &nd {
    //         Node::LeafNode(n) => Ok((page, n)),
    //         Node::InternalNode(n) => Self::search_leaf_node(pager, cell_key, n),
    //     }
    // }
}
