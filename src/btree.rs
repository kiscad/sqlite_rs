use crate::error::ExecErr;
use crate::pager::{Page, PAGE_SIZE};
use crate::row::{RowBytes, ROW_SIZE};
use std::fmt::{Display, Formatter};
use std::io::{self, BufRead, Read, Write};

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

pub const LEAF_NODE_HEADER_SIZE: usize = 1 + 1 + 4 + 4;
pub const CELL_KEY_SIZE: usize = 4;
const CELL_SIZE: usize = CELL_KEY_SIZE + ROW_SIZE;
pub const LEAF_MAX_CELLS: usize = (crate::pager::PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / CELL_SIZE;
pub const LEAF_SPLIT_IDX: usize = LEAF_MAX_CELLS / 2 + 1;

pub enum Node {
    InternalNode(InternalNode),
    LeafNode(LeafNode),
}

pub struct PageKey {
    pub page: u32,
    pub key: u32,
}

impl PageKey {
    pub fn new(page: u32, key: u32) -> Self {
        Self { page, key }
    }
}

#[derive(Default)]
pub struct InternalNode {
    is_root: bool,
    parent: u32,
    pub children: Vec<PageKey>,
    pub right_child_page: u32,
}

impl InternalNode {
    pub fn new(left_page: u32, left_key: u32, right_page: u32) -> Self {
        let page_key = PageKey::new(left_page, left_key);
        Self {
            is_root: false,
            parent: 0,
            children: vec![page_key],
            right_child_page: right_page,
        }
    }

    pub fn get_child_by(&self, cell_key: u32) -> u32 {
        for PageKey { page, key } in &self.children {
            if cell_key <= *key {
                return *page;
            }
        }
        self.right_child_page
    }

    pub fn get_first_child(&self) -> u32 {
        let PageKey { page, .. } = &self.children[0];
        *page
    }

    fn new_from_page(page: &Page) -> Self {
        let mut node = Self::default();
        node.read_page(page);
        node
    }

    fn serialize(&self) -> Page {
        let mut cache = [0u8; PAGE_SIZE];
        let mut writer = io::Cursor::new(&mut cache[..]);
        // write node-type: is-leaf
        writer.write_all(&[u8::from(false)]).unwrap();
        writer.write_all(&[u8::from(self.is_root)]).unwrap();
        writer.write_all(&self.parent.to_be_bytes()).unwrap();
        let num_keys = self.children.len() as u32;
        writer.write_all(&num_keys.to_be_bytes()).unwrap();
        writer
            .write_all(&self.right_child_page.to_be_bytes())
            .unwrap();
        for PageKey { page, key } in &self.children {
            writer.write_all(&page.to_be_bytes()).unwrap();
            writer.write_all(&key.to_be_bytes()).unwrap();
        }
        cache
    }

    fn read_page(&mut self, page: &Page) {
        let mut reader = io::Cursor::new(page);
        reader.consume(1);
        let mut is_root = [0; 1];
        reader.read_exact(&mut is_root).unwrap();
        self.is_root = is_root[0] != 0;
        let mut parent = [0; 4];
        reader.read_exact(&mut parent).unwrap();
        self.parent = u32::from_be_bytes(parent);
        let mut num_keys = [0; 4];
        reader.read_exact(&mut num_keys).unwrap();
        let num_keys = u32::from_be_bytes(num_keys);
        let mut right = [0; 4];
        reader.read_exact(&mut right).unwrap();
        self.right_child_page = u32::from_be_bytes(right);

        self.children.clear();
        for _ in 0..num_keys {
            let mut page_num = [0u8; 4];
            reader.read_exact(&mut page_num).unwrap();
            let mut cell_key = [0u8; 4];
            reader.read_exact(&mut cell_key).unwrap();
            self.children.push(PageKey::new(
                u32::from_be_bytes(page_num),
                u32::from_be_bytes(cell_key),
            ))
        }
    }
}

// Each node corresponding to one page.
// Nodes need to store some metadata at the beginning of the page.
// Metadata: node type, is-root-node, pointer-to-parent.
#[derive(Debug)]
pub struct Cell {
    pub key: u32,
    pub row: RowBytes,
}

impl Cell {
    fn new(key: u32, row: RowBytes) -> Self {
        Self { key, row }
    }
}

pub struct LeafNode {
    pub is_root: bool,
    pub parent: u32,
    // pub num_cells: u32, // to be remove
    pub cells: Vec<Cell>,
}

impl Node {
    pub fn is_root(&self) -> bool {
        match self {
            Self::LeafNode(nd) => nd.is_root,
            Self::InternalNode(nd) => nd.is_root,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Self::LeafNode(_) => true,
            Self::InternalNode(_) => false,
        }
    }

    pub fn set_root(&mut self, is_root: bool) {
        match self {
            Self::InternalNode(nd) => nd.is_root = is_root,
            Self::LeafNode(nd) => nd.is_root = is_root,
        }
    }

    pub fn set_parent(&mut self, parent: usize) {
        match self {
            Self::LeafNode(nd) => nd.parent = parent as u32,
            Self::InternalNode(nd) => nd.parent = parent as u32,
        }
    }

    pub fn serialize(&self) -> Page {
        match self {
            Node::LeafNode(nd) => nd.serialize(),
            Node::InternalNode(nd) => nd.serialize(),
        }
    }

    pub fn new_from_page(page: &Page) -> Self {
        let mut reader = io::Cursor::new(page);
        let mut is_leaf = [0; 1];
        reader.read_exact(&mut is_leaf).unwrap();
        let is_leaf = is_leaf[0] != 0;

        if is_leaf {
            Self::LeafNode(LeafNode::new_from_page(page))
        } else {
            Self::InternalNode(InternalNode::new_from_page(page))
        }
    }

    pub fn get_max_key(&self) -> u32 {
        match self {
            Self::InternalNode(nd) => nd.children[nd.children.len() - 1].key,
            Self::LeafNode(nd) => nd.cells[nd.cells.len() - 1].key,
        }
    }

    pub fn try_into_leaf(&self) -> Result<&LeafNode, ExecErr> {
        match self {
            Self::LeafNode(nd) => Ok(nd),
            Self::InternalNode(_) => Err(ExecErr::NodeError(
                "Error: It's a Internal node.".to_string(),
            )),
        }
    }

    pub fn try_into_leaf_mut(&mut self) -> Result<&mut LeafNode, ExecErr> {
        match self {
            Self::LeafNode(nd) => Ok(nd),
            Self::InternalNode(_) => Err(ExecErr::NodeError(
                "Error: It's a Internal node.".to_string(),
            )),
        }
    }

    pub fn get_children(&self) -> Vec<u32> {
        match self {
            Self::LeafNode(_) => vec![],
            Self::InternalNode(nd) => {
                let mut pages: Vec<u32> = nd.children.iter().map(|x| x.page).collect();
                pages.push(nd.right_child_page);
                pages
            }
        }
    }
}

impl LeafNode {
    pub fn new(is_root: bool) -> Self {
        Self {
            is_root,
            parent: 0,
            cells: Vec::with_capacity(LEAF_MAX_CELLS + 1),
        }
    }

    pub fn new_from_page(page: &Page) -> Self {
        let mut node = Self::new(true);
        node.read_page(page);
        node
    }

    fn read_page(&mut self, page: &Page) {
        let mut reader = io::Cursor::new(page);
        reader.consume(1);

        let mut is_root = [0; 1];
        reader.read_exact(&mut is_root).unwrap();
        self.is_root = is_root[0] != 0;

        let mut parent_pointer = [0; 4];
        reader.read_exact(&mut parent_pointer).unwrap();
        self.parent = u32::from_be_bytes(parent_pointer);

        let mut num_cells = [0; 4];
        reader.read_exact(&mut num_cells).unwrap();
        let num_cells = u32::from_be_bytes(num_cells);

        self.cells.clear();
        for _ in 0..num_cells {
            let mut key = [0; 4];
            reader.read_exact(&mut key).unwrap();
            let mut val = [0; ROW_SIZE];
            reader.read_exact(&mut val).unwrap();
            self.cells.push(Cell::new(u32::from_be_bytes(key), val));
        }
    }

    fn serialize(&self) -> Page {
        let mut cache = [0u8; PAGE_SIZE];
        let mut writer = io::Cursor::new(&mut cache[..]);
        // write node-type: is_leaf
        writer.write_all(&[u8::from(true)]).unwrap();
        writer.write_all(&[u8::from(self.is_root)]).unwrap();
        writer.write_all(&self.parent.to_be_bytes()).unwrap();
        let num_cells = self.cells.len() as u32;
        writer.write_all(&num_cells.to_be_bytes()).unwrap();
        for Cell { key, row } in &self.cells {
            writer.write_all(&key.to_be_bytes()).unwrap();
            writer.write_all(row).unwrap();
        }
        cache
    }

    pub fn update_cell(&mut self, cell_idx: usize, cell_val: &RowBytes) {
        assert!(cell_idx < self.cells.len());
        let val = &mut self.cells[cell_idx].row;
        val.copy_from_slice(cell_val);
    }

    pub fn read_cell(&self, cell_idx: usize, cell_val: &mut RowBytes) {
        cell_val.copy_from_slice(&self.cells[cell_idx].row);
    }

    pub fn get_cell_key(&self, cell_idx: usize) -> Option<u32> {
        Some(self.cells.get(cell_idx)?.key)
    }

    pub fn insert_cell(&mut self, idx: usize, key: u32, val: &RowBytes) -> Result<(), ExecErr> {
        if self.cells.len() >= LEAF_MAX_CELLS {
            return Err(ExecErr::LeafNodeFull(
                "Need to implement splitting a leaf node.".to_string(),
            ));
        }
        assert!(idx <= self.cells.len());
        self.cells.insert(idx, Cell::new(key, *val));

        Ok(())
    }

    pub fn insert_and_split(&mut self, cell_idx: usize, key: u32, val: &RowBytes) -> Self {
        assert_eq!(self.cells.len(), LEAF_MAX_CELLS);
        assert!(cell_idx <= self.cells.len());
        self.cells.insert(cell_idx, Cell::new(key, *val));

        let cells: Vec<_> = self.cells.drain(LEAF_SPLIT_IDX..).collect();
        Self {
            is_root: false,
            parent: 0,
            cells,
        }
    }

    /// This function will return one of the three kinds of positions:
    /// - the position of the key,
    /// - the position of another key that we will need to move if we want to insert new cell
    /// - the position that past the last key,
    pub fn find_place_for_new_cell(&self, cell_key: usize) -> usize {
        // Binary search
        let mut lower = 0;
        let mut upper = self.cells.len();
        while lower < upper {
            let mid = (lower + upper) / 2;
            let key_mid = self.get_cell_key(mid).unwrap() as usize;

            use std::cmp::Ordering::*;
            match cell_key.cmp(&key_mid) {
                Equal => return mid,
                Greater => lower = mid + 1,
                Less => upper = mid,
            }
        }
        lower // cell_idx
    }
}

impl Display for LeafNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "leaf (size {})", self.cells.len())?;
        let cells_str: Vec<_> = self
            .cells
            .iter()
            .enumerate()
            .map(|(idx, Cell { key, .. })| format!("  - {} : {}", idx, key))
            .collect();
        write!(f, "{}", cells_str.join("\n"))
    }
}

impl Display for InternalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "internal (size {})", self.children.len())
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InternalNode(nd) => write!(f, "{}", nd),
            Self::LeafNode(nd) => write!(f, "{}", nd),
        }
    }
}
