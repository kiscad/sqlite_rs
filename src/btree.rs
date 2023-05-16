use crate::pager::{Page, PAGE_SIZE};
use crate::row::{RowBytes, ROW_SIZE};
use std::io::Read;

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
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

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
    node_type: u8,
    is_root: u8,
    parent_pointer: u32,
    num_cells: u32,
    cells: Vec<(u32, RowBytes<'a>)>,
    page_cache: &'a mut Page,
}

impl<'a> Node<'a> {
    fn new(page: &'a mut Page) -> Self {
        let mut pg = std::io::Cursor::new(&page.0);

        let mut cache = [0u8; 1];
        pg.read(&mut cache).unwrap();
        let node_type = u8::from_be_bytes(cache);

        let mut cache = [0u8; 1];
        pg.read(&mut cache).unwrap();
        let is_root = u8::from_be_bytes(cache);

        let mut cache = [0u8; 4];
        pg.read(&mut cache).unwrap();
        let parent_pointer = u32::from_be_bytes(cache);

        let mut cache = [0u8; 4];
        pg.read(&mut cache).unwrap();
        let num_cells = u32::from_be_bytes(cache);

        let mut cells = vec![];
        // for i in 0..num_cells as usize {
        //     let mut cache_key = [0u8; LEAF_NODE_KEY_SIZE];
        //     let mut cache_val = [0u8; LEAF_NODE_VALUE_SIZE];
        //     pg.read(&mut cache_key).unwrap();
        //     let key = u32::from_be_bytes(cache_key);
        //     pg.read(&mut cache_val).unwrap();
        //     let val = deserialize_row(RowBytes(&mut cache_val));
        // }
        Self::LeafNode(LeafNode {
            node_type,
            is_root,
            parent_pointer,
            num_cells,
            cells,
            page_cache: page,
        })
    }
}
