use super::utils;
use crate::error::ExecErr;
use crate::pager::{Page, PAGE_SIZE};
use std::fmt;
use std::io;
use std::io::BufRead;

#[derive(Debug)]
pub struct Intern {
  pub is_root: bool,
  pub pg_idx: usize,
  pub parent: Option<usize>,
  children: Vec<Child>,
}

#[derive(Debug)]
pub struct Child {
  pg_idx: usize,
  key_max: Option<usize>, // the rightmost child doesn't have key_max
}

impl Child {
  pub fn new(pg_idx: usize, key_max: Option<usize>) -> Self {
    Self { pg_idx, key_max }
  }
}

impl Intern {
  pub fn new(is_root: bool, pg_idx: usize, parent: Option<usize>, children: Vec<Child>) -> Self {
    assert!(children.len() >= 2);
    Self {
      is_root,
      pg_idx,
      parent,
      children,
    }
  }

  pub fn new_from_page(pg_idx: usize, page: &Page) -> Self {
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
        let key_max = utils::read_u32_from(&mut reader).map(|x| x as usize);
        Child { pg_idx, key_max }
      })
      .collect();

    children.push(Child {
      pg_idx: right_most_child,
      key_max: None,
    });

    Self {
      is_root,
      pg_idx,
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

  pub fn insert_child(&mut self, pg_idx: usize, key_max: usize) -> Result<(), ExecErr> {
    // TODO: remove 1000
    if self.children.len() > 1000 {
      return Err(ExecErr::InternNodeFull("Intern node full".to_string()));
    }
    let idx = self.search_child_idx_by_key(key_max);
    self.children.insert(idx, Child::new(pg_idx, Some(key_max)));
    Ok(())
  }

  pub fn find_child_set_key(
    &mut self,
    key_old: usize,
    key_new: Option<usize>,
  ) -> Result<(), ExecErr> {
    let idx = self.search_child_idx_by_key(key_old);
    self.children[idx].key_max = key_new;
    Ok(())
  }

  fn search_child_idx_by_key(&self, key: usize) -> usize {
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
    writeln!(
      f,
      "intern (size {}, page {})",
      self.children.len(),
      self.pg_idx
    )
  }
}
