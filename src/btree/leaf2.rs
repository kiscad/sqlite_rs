use crate::btree::leaf::MAX_CELLS;
use crate::btree::utils;
use crate::error::ExecErr;
use std::fmt;
use std::io::{self, BufRead, Read, Write};

use crate::pager::{Page, PAGE_SIZE};
use crate::row::{RowBytes, ROW_SIZE};

#[derive(Debug)]
pub struct Leaf {
  pub is_root: bool,
  pub pg_idx: usize,
  pub parent: Option<usize>, // parent's pg_idx
  pub next: Option<usize>,   // next-leaf's pg_idx
  cells: Vec<Cell>,
}

#[derive(Debug)]
struct Cell {
  key: u32,
  row: RowBytes,
}

impl Leaf {
  pub fn new(is_root: bool, pg_idx: usize, parent: Option<usize>, next: Option<usize>) -> Self {
    Self {
      is_root,
      pg_idx,
      parent,
      next,
      cells: vec![],
    }
  }

  pub fn new_from_page(pg_idx: usize, page: &Page) -> Self {
    let mut reader = io::Cursor::new(page);
    reader.consume(1); // the first byte is for node-type

    let is_root = utils::read_bool_from(&mut reader);
    let parent = utils::read_u32_from(&mut reader).map(|x| x as usize);
    let next = utils::read_u32_from(&mut reader).map(|x| x as usize);

    let num_cells = utils::read_u32_from(&mut reader).unwrap_or(0);
    let cells: Vec<_> = (0..num_cells)
      .map(|_| {
        let key = utils::read_u32_from(&mut reader).unwrap();
        let row = {
          let mut buf = [0; ROW_SIZE];
          reader.read_exact(&mut buf).unwrap();
          buf
        };
        Cell { key, row }
      })
      .collect();

    Self {
      is_root,
      pg_idx,
      parent,
      next,
      cells,
    }
  }

  pub fn serialize(&self) -> Page {
    let mut cache = [0u8; PAGE_SIZE];
    let mut writer = io::Cursor::new(&mut cache[..]);

    // write node-type, is_leaf as true
    utils::write_bool_to(&mut writer, true);
    utils::write_bool_to(&mut writer, self.is_root);
    utils::write_opt_u32_to(&mut writer, self.parent.map(|x| x as u32));
    utils::write_opt_u32_to(&mut writer, self.next.map(|x| x as u32));
    utils::write_opt_u32_to(&mut writer, Some(self.cells.len() as u32));

    for Cell { key, row } in &self.cells {
      writer.write_all(&key.to_be_bytes()).unwrap();
      writer.write_all(row).unwrap();
    }
    cache
  }

  pub fn insert_row(&mut self, key: u32, row: &RowBytes) -> Result<(), ExecErr> {
    if self.cells.len() >= MAX_CELLS {
      return Err(ExecErr::LeafNodeFull("Leaf full".to_string()));
    }
    let idx = self.search_cell_idx_by_key(key);
    if self.cells.get(idx).is_some_and(|c| c.key == key) {
      return Err(ExecErr::DuplicateKey("Duplicated key".to_string()));
    }
    self.cells.insert(idx, Cell { key, row: *row });
    Ok(())
  }

  /// Find the nearest cell which key is greater or equal to the input key.
  pub fn find_row(&self, key: u32) -> Result<&Cell, ExecErr> {
    let idx = self.search_cell_idx_by_key(key);
    if idx >= self.cells.len() {
      return Err(ExecErr::CellNotFound("Cell not found".to_string()));
    }
    Ok(&self.cells[idx])
  }

  pub fn key_max(&self) -> usize {
    self.cells[self.cells.len() - 1].key as usize
  }

  fn search_cell_idx_by_key(&self, key: u32) -> usize {
    // Binary search
    let mut lower = 0;
    let mut upper = self.cells.len();
    while lower < upper {
      let mid = (lower + upper) / 2;
      let key_mid = self.cells[mid].key;

      use std::cmp::Ordering::*;
      match key.cmp(&key_mid) {
        Equal => return mid,
        Greater => lower = mid + 1,
        Less => upper = mid,
      }
    }
    lower // lower equals upper
  }
}

impl fmt::Display for Leaf {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f, "leaf (size {}, page {})", self.cells.len(), self.pg_idx)?;
    let cell_str: Vec<_> = self
      .cells
      .iter()
      .map(|Cell { key, .. }| format!("  - {key}"))
      .collect();
    write!(f, "{}", cell_str.join("\n"))
  }
}
