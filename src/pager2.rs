use crate::btree::node2::Node;
use crate::error::ExecErr;
use crate::pager::{Page, PAGE_SIZE};
use crate::table::TABLE_MAX_PAGES;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct Pager {
  cacher: RefCell<Cacher>,
  pg_num: usize,
}

struct Cacher {
  file: File,
  pages: [Option<Node>; TABLE_MAX_PAGES],
}

impl Pager {
  pub fn new(fname: impl AsRef<Path>) -> Result<Self, ExecErr> {
    let file = OpenOptions::new()
      .write(true)
      .read(true)
      .create(true)
      .open(fname)
      .map_err(|_| ExecErr::IoError("Unable to open file.".to_string()))?;

    let file_len = file.metadata().unwrap().len() as usize;
    let num_pages = file_len / PAGE_SIZE;
    if file_len % PAGE_SIZE != 0 {
      return Err(ExecErr::IoError("Corrupted file.".to_string()));
    }
    const INIT: Option<Node> = None;
    let pages = [INIT; TABLE_MAX_PAGES];

    Ok(Self {
      cacher: RefCell::new(Cacher { file, pages }),
      pg_num: num_pages,
    })
  }

  pub fn size(&self) -> usize {
    self.pg_num
  }

  pub fn push_node(&mut self, node: Node) -> Result<(), ExecErr> {
    if self.size() == TABLE_MAX_PAGES {
      return Err(ExecErr::PagerFull("pager full".to_string()));
    }
    let slot = &mut self.cacher.borrow_mut().pages[self.size()];
    assert!(slot.is_none());
    let _ = slot.insert(node);
    self.pg_num += 1;
    Ok(())
  }

  pub fn get_node_do<F, T>(&self, pid: usize, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&Node) -> T,
  {
    if self.cacher.borrow().pages[pid].is_none() {
      self.load_node(pid)?;
    }
    Ok(f(self.cacher.borrow().pages[pid].as_ref().unwrap()))
  }

  pub fn set_node_by<F, T>(&self, pid: usize, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&mut Node) -> T,
  {
    if self.cacher.borrow().pages[pid].is_none() {
      self.load_node(pid)?;
    }
    Ok(f(self.cacher.borrow_mut().pages[pid].as_mut().unwrap()))
  }

  pub fn close(self) -> Result<(), ExecErr> {
    for pid in 0..TABLE_MAX_PAGES {
      self.write_node(pid)?;
    }
    Ok(())
  }

  fn load_node(&self, pg_id: usize) -> Result<(), ExecErr> {
    let page = self.load_page(pg_id)?;
    let node = Node::new_from_page(pg_id, &page);
    let _ = self.cacher.borrow_mut().pages[pg_id].insert(node);
    Ok(())
  }

  fn load_page(&self, pid: usize) -> Result<Page, ExecErr> {
    let mut buf = [0; PAGE_SIZE];

    let file = &mut self.cacher.borrow_mut().file;
    file
      .seek(SeekFrom::Start((pid * PAGE_SIZE) as u64))
      .map_err(|_| ExecErr::IoError("Fail seeking.".to_string()))?;
    file
      .read_exact(&mut buf)
      .map_err(|_| ExecErr::IoError("Fail reading.".to_string()))?;

    Ok(buf)
  }

  fn write_node(&self, pid: usize) -> Result<(), ExecErr> {
    if let Some(nd) = self.cacher.borrow().pages[pid].as_ref() {
      self.write_page(pid, &nd.serialize())?;
    }
    Ok(())
  }

  fn write_page(&self, pid: usize, page: &Page) -> Result<(), ExecErr> {
    let file = &mut self.cacher.borrow_mut().file;
    file
      .seek(SeekFrom::Start((pid * PAGE_SIZE) as u64))
      .map_err(|_| ExecErr::IoError("Fail seeking".to_string()))?;
    file
      .write_all(page)
      .map_err(|_| ExecErr::IoError("Fail writing".to_string()))
  }
}
