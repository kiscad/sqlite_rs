use crate::pager::{Page, PAGE_SIZE};
use crate::row::{RowBytes, ROW_SIZE};
use std::io::Cursor;

/*
 * Common Node Header Layout
 */
const NODE_TYPE_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
const COMMON_NODE_HEADER_SIZE: usize = PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_HEADER_SIZE: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

pub enum Node<'a> {
    InternalNode(InternalNode<'a>),
    LeafNode(LeafNode<'a>),
}

pub struct InternalNode<'a> {
    page_nums: Vec<usize>,
    children: Vec<Node<'a>>,
}

// Each node corresponding to one page.
// Nodes need to store some metadata at the beginning of the page.
// Metadata: node type, is-root-node, pointer-to-parent.
pub struct LeafNode<'a> {
    node_type: &'a mut [u8; 1],
    is_root: &'a mut [u8; 1],
    parent_pointer: &'a mut [u8; 4],
    num_cells: &'a mut [u8; 4],
    pub cells: Vec<(&'a mut [u8; 4], RowBytes<'a>)>,
}

impl<'a> Node<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Node::LeafNode(LeafNode::new(page))
    }
}

impl<'a> LeafNode<'a> {
    fn new(page: &'a mut Page) -> Self {
        let arr = &mut page.0;
        let (node_type, arr) = arr.split_array_mut::<1>();
        let (is_root, arr) = arr.split_array_mut::<1>();
        let (parent_pointer, arr) = arr.split_array_mut::<4>();
        let (num_cells, arr) = arr.split_array_mut::<4>();

        let mut cells = vec![];
        let mut arr = arr;
        for _ in 0..u32::from_be_bytes(num_cells.clone()) as usize {
            let (key, arr_) = arr.split_array_mut::<4>();
            let (value, arr_) = arr_.split_array_mut::<ROW_SIZE>();
            arr = arr_;
            cells.push((key, RowBytes(value)));
        }

        Self {
            node_type,
            is_root,
            parent_pointer,
            num_cells,
            cells,
        }
    }

    pub fn get_num_cells(&self) -> u32 {
        u32::from_be_bytes(self.num_cells.clone())
    }

    pub fn set_num_cells(&mut self, val: u32) {
        self.num_cells.copy_from_slice(&val.to_be_bytes())
    }

    pub fn initialize(&mut self) {
        self.set_num_cells(0);
    }

    pub fn write_cell_value(
        &mut self,
        cell_key: u32,
        row_serializer: impl FnOnce(Cursor<&mut [u8]>),
    ) {
        let writer = std::io::Cursor::new(self.cells[cell_key as usize].1 .0);
        row_serializer(writer)
    }

    pub fn read_cell_value(
        &mut self,
        cell_key: u32,
        row_deserializer: impl FnOnce(Cursor<&mut &mut [u8]>),
    ) {
        let reader = std::io::Cursor::new(&mut self.cells[cell_key as usize].1 .0);
        row_deserializer(reader)
    }
}
