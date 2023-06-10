use super::utils;
use crate::error::ExecErr;
use crate::pager::{Page, PAGE_SIZE};
use std::fmt;
use std::io;
use std::io::BufRead;

use super::node::{IS_ROOT_SIZE, NODE_TYPE_SIZE, PARENT_SIZE};
const CHILD_NUM: usize = PARENT_SIZE;
const HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_SIZE + CHILD_NUM;
const CHILD_SIZE: usize = PARENT_SIZE * 2;
const CHILD_MAX: usize = (PAGE_SIZE - HEADER_SIZE) / CHILD_SIZE;
const SPLIT_IDX: usize = CHILD_MAX / 2 + 1;

#[derive(Debug)]
pub struct Intern {
  pub is_root: bool,
  pub parent: Option<usize>,

  pub children: Vec<Child>,
}

#[derive(Debug, Clone)]
pub struct Child {
  pub pg_idx: usize,
  pub key_max: u32,
}

impl Child {
  pub fn new(pg_idx: usize, key_max: u32) -> Self {
    Self { pg_idx, key_max }
  }
}

impl Intern {
  pub fn new(is_root: bool, parent: Option<usize>, children: Vec<Child>) -> Self {
    assert!(children.len() >= 2);
    Self {
      is_root,
      parent,
      children,
    }
  }

  pub fn new_from_page(page: &Page) -> Self {
    let mut reader = io::Cursor::new(page);
    reader.consume(1); // the first byte is for node-type

    let is_root = utils::read_bool_from(&mut reader);
    let parent = utils::read_u32_from(&mut reader).map(|x| x as usize);
    let num_child = utils::read_u32_from(&mut reader).unwrap();

    let children: Vec<_> = (0..num_child)
      .map(|_| {
        let pg_idx = utils::read_u32_from(&mut reader).unwrap() as usize;
        let key_max = utils::read_u32_from(&mut reader).unwrap();
        Child { pg_idx, key_max }
      })
      .collect();

    Self {
      is_root,
      parent,
      children,
    }
  }

  pub fn serialize(&self) -> Page {
    let mut buf = [0u8; PAGE_SIZE];
    let mut writer = io::Cursor::new(&mut buf[..]);

    // write node-type: is_leaf as false
    utils::write_bool_to(&mut writer, false);
    utils::write_bool_to(&mut writer, self.is_root);
    utils::write_opt_u32_to(&mut writer, self.parent.map(|x| x as u32));
    utils::write_opt_u32_to(&mut writer, Some(self.children.len() as u32));

    for Child { pg_idx, key_max } in &self.children {
      utils::write_opt_u32_to(&mut writer, Some(*pg_idx as u32));
      utils::write_opt_u32_to(&mut writer, Some(*key_max));
    }
    buf
  }

  pub fn insert_child(&mut self, pg_idx: usize, key_max: u32) -> Result<(), ExecErr> {
    // TODO: remove 1000
    if self.children.len() >= CHILD_MAX {
      return Err(ExecErr::InternNodeFull("Intern node full".to_string()));
    }
    let idx = self.search_insert_idx_by_key(key_max);
    self.children.insert(idx, Child::new(pg_idx, key_max));
    Ok(())
  }

  pub fn insert_child_and_split(&mut self, pg_idx: usize, key_max: u32) -> Result<Self, ExecErr> {
    let idx = self.search_insert_idx_by_key(key_max);
    self.children.insert(idx, Child::new(pg_idx, key_max));
    let children: Vec<_> = self.children.drain(SPLIT_IDX..).collect(); // TODO:

    Ok(Self {
      is_root: false,
      parent: self.parent,
      children,
    })
  }

  pub fn find_child_and<F, T>(&self, key_max: u32, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&Child) -> T,
  {
    let idx = self.search_child_by_key(key_max);
    Ok(f(&self.children[idx]))
  }

  #[allow(unused)]
  pub fn find_mut_child_and<F, T>(&mut self, key_max: u32, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&mut Child) -> T,
  {
    let idx = self.search_child_by_key(key_max);
    Ok(f(&mut self.children[idx]))
  }

  fn search_insert_idx_by_key(&self, key: u32) -> usize {
    // binary search
    let mut lower = 0;
    let mut upper = self.children.len();
    while lower < upper {
      let mid = (lower + upper) / 2;
      let key_mid = self.children[mid].key_max;
      if key <= key_mid {
        upper = mid;
      } else {
        lower = mid + 1;
      }
    }
    lower
  }

  fn search_child_by_key(&self, key: u32) -> usize {
    self
      .search_insert_idx_by_key(key)
      .min(self.children.len() - 1)
  }
}

impl fmt::Display for Intern {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "intern (size {})", self.children.len(),)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn setup(keys: &[u32]) -> Intern {
    Intern {
      is_root: false,
      parent: None,
      children: keys.iter().map(|&i| Child::new(i as usize, i)).collect(),
    }
  }

  #[test]
  fn insert_rightmost_child() {
    let mut parent = setup(&[1, 3]);
    parent.insert_child(4, 4).unwrap();
    let keys: Vec<_> = parent.children.iter().map(|ch| ch.key_max).collect();
    assert_eq!(keys, vec![1, 3, 4]);
  }

  #[test]
  fn insert_mid_child() {
    let mut parent = setup(&[1, 3]);
    parent.insert_child(2, 2).unwrap();
    let keys: Vec<_> = parent.children.iter().map(|ch| ch.key_max).collect();
    assert_eq!(keys, vec![1, 2, 3]);
  }

  #[test]
  fn insert_leaftmost_child() {
    let mut parent = setup(&[1, 3]);
    parent.insert_child(0, 0).unwrap();
    let keys: Vec<_> = parent.children.iter().map(|ch| ch.key_max).collect();
    assert_eq!(keys, vec![0, 1, 3]);
  }

  #[test]
  fn insert_rightmost_in_3_child_parent() {
    let mut parent = setup(&[1, 2, 3]);
    parent.insert_child(4, 4).unwrap();
    let keys: Vec<_> = parent.children.iter().map(|ch| ch.key_max).collect();
    assert_eq!(keys, vec![1, 2, 3, 4]);
  }
}
