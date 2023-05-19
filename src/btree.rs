use crate::pager::Page;
use crate::row::{RowBytes, ROW_SIZE};
use std::io::{self, Read, Write};

/*
 * Common Node Header Layout
 */
// const NODE_TYPE_OFFSET: usize = 0;
// const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
// const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
// const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
// const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
// const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
// const COMMON_NODE_HEADER_SIZE: usize = PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
// const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
// const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
// const LEAF_NODE_HEADER_SIZE: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf Node Body Layout
 */
// const LEAF_NODE_KEY_OFFSET: usize = 0;
// const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
// const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
// const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
// const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
// const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
// pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

pub const LEAF_NODE_METADATA_SIZE: usize = 1 + 1 + 4 + 4;

pub enum Node {
    InternalNode(InternalNode),
    LeafNode(LeafNode),
}

pub struct InternalNode {}

// Each node corresponding to one page.
// Nodes need to store some metadata at the beginning of the page.
// Metadata: node type, is-root-node, pointer-to-parent.
#[derive(Default)]
pub struct LeafNode {
    is_leaf: bool,
    is_root: bool,
    parent_pointer: u32,
    // pub num_cells: u32, // to be remove
    pub cells: Vec<(u32, RowBytes)>,
}

impl Node {
    pub fn new(page: &Page) -> Self {
        let mut node = LeafNode::default();
        node.read_page(page);
        Node::LeafNode(node)
    }
}

impl LeafNode {
    pub fn read_page(&mut self, page: &Page) {
        let mut reader = io::Cursor::new(page);

        let mut is_leaf = [0; 1];
        reader.read_exact(&mut is_leaf).unwrap();
        self.is_leaf = is_leaf[0] != 0;

        let mut is_root = [0; 1];
        reader.read_exact(&mut is_root).unwrap();
        self.is_root = is_root[0] != 0;

        let mut parent_pointer = [0; 4];
        reader.read_exact(&mut parent_pointer).unwrap();
        self.parent_pointer = u32::from_be_bytes(parent_pointer);

        let mut num_cells = [0; 4];
        reader.read_exact(&mut num_cells).unwrap();
        let num_cells = u32::from_be_bytes(num_cells);

        self.cells.clear();
        for _ in 0..num_cells {
            let mut key = [0; 4];
            reader.read_exact(&mut key).unwrap();
            let mut val = [0; ROW_SIZE];
            reader.read_exact(&mut val).unwrap();
            self.cells.push((u32::from_be_bytes(key), val));
        }
    }

    pub fn write_page(&self, page: &mut Page) {
        let mut writer = io::Cursor::new(&mut page[..]);
        writer.write_all(&[u8::from(self.is_leaf)]).unwrap();
        writer.write_all(&[u8::from(self.is_root)]).unwrap();
        writer
            .write_all(&self.parent_pointer.to_be_bytes())
            .unwrap();
        let num_cells = self.cells.len() as u32;
        writer.write_all(&num_cells.to_be_bytes()).unwrap();
        for (key, val) in &self.cells {
            writer.write_all(&key.to_be_bytes()).unwrap();
            writer.write_all(val).unwrap();
        }
    }

    pub fn write_cell_value(&mut self, cell_idx: usize, cell_val: &[u8; ROW_SIZE]) {
        assert!(cell_idx < self.cells.len());
        let val = &mut self.cells[cell_idx].1;
        val.copy_from_slice(cell_val);
    }

    pub fn read_cell_value(&self, cell_idx: usize, cell_val: &mut [u8; ROW_SIZE]) {
        cell_val.copy_from_slice(&self.cells[cell_idx].1);
    }

    pub fn append_cell_value(&mut self, cell_val: &[u8; ROW_SIZE]) {
        let cell_key = 0; // currently, all cell keys are zeros.
        self.cells.push((cell_key, *cell_val));
    }
}
