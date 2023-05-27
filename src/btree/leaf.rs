use crate::error::ExecErr;
use crate::pager::{Page, PAGE_SIZE};
use crate::row::{RowBytes, ROW_SIZE};
use std::io::{self, BufRead, Read, Write};
use std::{fmt, mem};

const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
const PARENT_SIZE: usize = mem::size_of::<u32>();
const NEXT_LEAF_SIZE: usize = mem::size_of::<u32>();
pub const LEAF_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_SIZE + NEXT_LEAF_SIZE;
const CELL_KEY_SIZE: usize = mem::size_of::<u32>();
const CELL_SIZE: usize = CELL_KEY_SIZE + ROW_SIZE;
pub const LEAF_MAX_CELLS: usize = (PAGE_SIZE - LEAF_HEADER_SIZE) / CELL_SIZE;
const SPLIT_IDX: usize = LEAF_MAX_CELLS / 2 + 1;

#[derive(Debug)]
pub struct Cell {
    pub key: u32,
    pub row: RowBytes,
}

pub struct Leaf {
    pub is_root: bool,
    pub parent: u32,
    pub next_leaf: Option<u32>,
    // pub num_cells: u32, // to be remove
    pub cells: Vec<Cell>,
}

impl Cell {
    fn new(key: u32, row: RowBytes) -> Self {
        Self { key, row }
    }
}

impl Leaf {
    pub fn new(is_root: bool) -> Self {
        Self {
            is_root,
            parent: 0,
            next_leaf: None,
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

        let mut parent = [0; 4];
        reader.read_exact(&mut parent).unwrap();
        self.parent = u32::from_be_bytes(parent);

        let mut next = [0; 4];
        reader.read_exact(&mut next).unwrap();
        let next = u32::from_be_bytes(next);
        self.next_leaf = if next > 0 { Some(next) } else { None };

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

    pub fn serialize(&self) -> Page {
        let mut cache = [0u8; PAGE_SIZE];
        let mut writer = io::Cursor::new(&mut cache[..]);
        // write node-type: is_leaf
        writer.write_all(&[u8::from(true)]).unwrap();
        writer.write_all(&[u8::from(self.is_root)]).unwrap();
        writer.write_all(&self.parent.to_be_bytes()).unwrap();
        let next = self.next_leaf.unwrap_or_default();
        writer.write_all(&next.to_be_bytes()).unwrap();
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

        let cells: Vec<_> = self.cells.drain(SPLIT_IDX..).collect();
        Self {
            is_root: false,
            parent: 0,
            next_leaf: None,
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

impl fmt::Display for Leaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "leaf (size {})", self.cells.len())?;
        let cells_str: Vec<_> = self
            .cells
            .iter()
            .map(|Cell { key, .. }| format!("  - {}", key))
            .collect();
        write!(f, "{}", cells_str.join("\n"))
    }
}
