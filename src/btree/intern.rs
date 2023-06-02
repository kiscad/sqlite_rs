use super::node::Parent;
use crate::btree::NodeRc;
use crate::error::ExecErr;
use crate::pager::{Page, PAGE_SIZE};
use std::fmt;
use std::io::{self, BufRead, Read, Write};

#[derive(Clone, Debug)]
pub struct Child {
  pub page: u32,
  pub key: u32,
  pub node: Option<NodeRc>,
}

impl Child {
  pub fn new(page: u32, key: u32) -> Self {
    Self { page,
           key,
           node: None }
  }
}

#[derive(Default, Debug)]
pub struct Intern {
  pub is_root: bool,
  pub page_idx: usize,
  pub parent: Option<Parent>,
  pub children: Vec<Child>,
}

impl Intern {
  pub fn new_root() -> Self {
    Self { is_root: true,
           page_idx: 0,
           parent: None,
           children: vec![] }
  }

  pub fn get_key_nums(&self) -> usize {
    self.children.len() - 1
  }

  pub fn set_child_by_key_with<F, T>(&mut self, key: usize, mut f: F) -> T
    where F: FnMut(&mut Child) -> T
  {
    let idx = self.search_child_by_key(key);
    f(&mut self.children[idx])
  }

  pub fn search_child_by_key(&self, key: usize) -> usize {
    // binary search
    let mut lower = 0;
    let mut upper = self.get_key_nums();
    while lower < upper {
      let mid = (lower + upper) / 2;
      let key_max = self.children[mid].key as usize;
      if key <= key_max {
        upper = mid;
      } else {
        lower = mid + 1;
      }
    }
    lower
  }

  pub fn insert_child(&mut self, child_idx: usize, child: &Child) -> Result<(), ExecErr> {
    if self.children.len() >= 1000 {
      return Err(ExecErr::InternNodeFull("Error: Intern node full.".to_string()));
    }
    assert!(child_idx <= self.children.len());
    self.children.insert(child_idx, child.clone());
    Ok(())
  }

  pub fn new_from_page(page: &Page) -> Self {
    let mut node = Self::default();
    node.read_page(page);
    node
  }

  pub fn serialize(&self) -> Page {
    let mut buf = [0u8; PAGE_SIZE];
    let mut writer = io::Cursor::new(&mut buf[..]);
    // write node-type: is-leaf
    writer.write_all(&[u8::from(false)]).unwrap();
    writer.write_all(&[u8::from(self.is_root)]).unwrap();
    writer.write_all(&self.parent
                          .as_ref()
                          .map_or(0u32.to_be_bytes(), |x| x.page.to_be_bytes()))
          .unwrap();
    let num_keys = self.children.len() - 1;
    writer.write_all(&(num_keys as u32).to_be_bytes()).unwrap();
    // write rightmost-child-page-idx
    writer.write_all(&self.children[num_keys].page.to_be_bytes())
          .unwrap();
    for Child { page, key, .. } in &self.children[..num_keys] {
      writer.write_all(&page.to_be_bytes()).unwrap();
      writer.write_all(&key.to_be_bytes()).unwrap();
    }
    buf
  }

  fn read_page(&mut self, page: &Page) {
    let mut reader = io::Cursor::new(page);
    reader.consume(1);
    let mut is_root = [0; 1];
    reader.read_exact(&mut is_root).unwrap();
    self.is_root = is_root[0] != 0;
    let mut parent = [0; 4];
    reader.read_exact(&mut parent).unwrap();
    self.parent = match u32::from_be_bytes(parent) {
      0 => None,
      x => Some(Parent::new(x)),
    };
    let mut num_keys = [0; 4];
    reader.read_exact(&mut num_keys).unwrap();
    let num_keys = u32::from_be_bytes(num_keys);
    let mut right = [0; 4];
    reader.read_exact(&mut right).unwrap();
    let right_child = Child { page: u32::from_be_bytes(right),
                              key: 0, // dummy value for the right-most child
                              node: None };

    self.children.clear();
    for _ in 0..num_keys {
      let mut page_num = [0u8; 4];
      reader.read_exact(&mut page_num).unwrap();
      let mut cell_key = [0u8; 4];
      reader.read_exact(&mut cell_key).unwrap();
      self.children
          .push(Child::new(u32::from_be_bytes(page_num), u32::from_be_bytes(cell_key)))
    }
    self.children.push(right_child);
  }
}

impl fmt::Display for Intern {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    writeln!(f,
             "internal (size {}, page {})",
             self.children.len(),
             self.page_idx)
  }
}
