use super::utils;
use crate::error::ExecErr;
use crate::pager2::{Page, PAGE_SIZE};
use std::fmt;
use std::io;
use std::io::BufRead;

#[derive(Debug)]
pub struct Intern {
  pub is_root: bool,
  pub parent: Option<usize>,

  pub children: Vec<Child>,
}

#[derive(Debug, Clone)]
pub struct Child {
  pub pg_idx: usize,
  pub key_max: Option<u32>, // the rightmost child doesn't have key_max
}

impl Child {
  pub fn new(pg_idx: usize, key_max: Option<u32>) -> Self {
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
    let num_child = utils::read_u32_from(&mut reader).unwrap_or(0);
    let right_most_child = utils::read_u32_from(&mut reader)
      .map(|x| x as usize)
      .unwrap();

    let mut children: Vec<_> = (0..num_child)
      .map(|_| {
        let pg_idx = utils::read_u32_from(&mut reader)
          .map(|x| x as usize)
          .unwrap();
        let key_max = utils::read_u32_from(&mut reader);
        Child { pg_idx, key_max }
      })
      .collect();

    children.push(Child {
      pg_idx: right_most_child,
      key_max: None,
    });

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
    let num_keys = self.children.len() - 1;
    utils::write_opt_u32_to(&mut writer, Some(num_keys as u32));
    let right_child_pg_idx = self.children[num_keys].pg_idx as u32;
    utils::write_opt_u32_to(&mut writer, Some(right_child_pg_idx));

    for Child { pg_idx, key_max } in &self.children[..num_keys] {
      utils::write_opt_u32_to(&mut writer, Some(*pg_idx as u32));
      utils::write_opt_u32_to(&mut writer, key_max.map(|x| x as u32));
    }
    buf
  }

  pub fn insert_child(&mut self, pg_idx: usize, key_max: u32) -> Result<(), ExecErr> {
    // TODO: remove 1000
    if self.children.len() > 100 {
      return Err(ExecErr::InternNodeFull("Intern node full".to_string()));
    }
    let idx = self.search_child_idx_by_key(key_max);
    self.children.insert(idx, Child::new(pg_idx, Some(key_max)));
    Ok(())
  }

  pub fn insert_child_and_split(&mut self, pg_idx: usize, key_max: u32) -> Result<Self, ExecErr> {
    let idx = self.search_child_idx_by_key(key_max);
    self.children.insert(idx, Child::new(pg_idx, Some(key_max)));
    let children: Vec<_> = self.children.drain(50..).collect(); // TODO:
    let ch_idx = self.children.len() - 1;
    self.children[ch_idx].key_max = None;
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
    let idx = self.search_child_idx_by_key(key_max);
    Ok(f(&self.children[idx]))
  }

  #[allow(unused)]
  pub fn find_mut_child_and<F, T>(&mut self, key_max: u32, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&mut Child) -> T,
  {
    let idx = self.search_child_idx_by_key(key_max);
    Ok(f(&mut self.children[idx]))
  }

  fn search_child_idx_by_key(&self, key: u32) -> usize {
    // binary search
    let mut lower = 0;
    let mut upper = self.children.len();
    while lower < upper {
      let mid = (lower + upper) / 2;
      let key_mid = self.children[mid].key_max;
      match key_mid {
        None => return mid,
        Some(k_max) => {
          if key <= k_max {
            upper = mid;
          } else {
            lower = mid + 1;
          }
        }
      }
    }
    lower
  }
}

impl fmt::Display for Intern {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "intern (size {})", self.children.len(),)
  }
}
