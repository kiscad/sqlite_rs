#![allow(unused)]
use crate::btree::node::Node;
use crate::error::ExecErr;
use crate::table2::MAX_PAGES;
use std::alloc::Allocator;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE];

pub struct Pager<'r, T: Allocator> {
  file: File,
  pages: Vec<Option<Node>, &'r T>,
  // cache: [Option<Box<Node, &'r T>>; MAX_PAGES], // TODO:
  pub size: usize,
}

impl<'r, A: Allocator> Pager<'r, A> {
  pub fn new(fname: impl AsRef<Path>, arena: &'r A) -> Self {
    let file = OpenOptions::new()
      .write(true)
      .read(true)
      .create(true)
      .open(fname)
      .map_err(|_| ExecErr::IoError("Unable to open file.".to_string()))
      .unwrap();

    let file_len = file.metadata().unwrap().len() as usize;
    let size = file_len / PAGE_SIZE;
    assert_ne!(file_len % PAGE_SIZE, 0);

    let mut pages = Vec::with_capacity_in(MAX_PAGES, arena);
    for _ in 0..MAX_PAGES {
      pages.push(None);
    }
    Self { file, pages, size }
  }

  pub fn push_node(&mut self, node: Node) -> Result<(), ExecErr> {
    if self.size == MAX_PAGES {
      return Err(ExecErr::PagerFull2);
    }
    let _ = self.pages[self.size].insert(node);
    self.size += 1;
    Ok(())
  }

  fn replace_node(&mut self, idx: usize, node: Node) -> Option<Node> {
    self.pages[idx].replace(node)
  }

  pub fn get_node_do<F, T>(&self, idx: usize, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&Node) -> T,
  {
    match &self.pages[idx] {
      Some(nd) => Ok(f(nd)),
      None => Err(ExecErr::PageUnload),
    }
  }

  pub fn set_node_by<F, T>(&mut self, pg_idx: usize, mut f: F) -> Result<T, ExecErr>
  where
    F: FnMut(&mut Node) -> T,
  {
    match &mut self.pages[pg_idx] {
      Some(nd) => Ok(f(nd)),
      None => Err(ExecErr::PageUnload),
    }
  }

  pub fn flush(&mut self) -> Result<(), ExecErr> {
    for pg_idx in 0..MAX_PAGES {
      self.write_node(pg_idx)?;
    }
    Ok(())
  }

  pub fn load_node(&mut self, pg_idx: usize) -> Result<(), ExecErr> {
    let page = self.load_page(pg_idx)?;
    let node = Node::new_from_page(&page);
    let _ = self.pages[pg_idx].insert(node);
    Ok(())
  }

  fn load_page(&mut self, pg_idx: usize) -> Result<Page, ExecErr> {
    let mut buf = [0; PAGE_SIZE];

    self
      .file
      .seek(SeekFrom::Start((pg_idx * PAGE_SIZE) as u64))
      .map_err(|_| ExecErr::IoError("Fail seeking.".to_string()))?;
    self
      .file
      .read_exact(&mut buf)
      .map_err(|_| ExecErr::IoError("Fail reading.".to_string()))?;

    Ok(buf)
  }

  pub fn write_node(&mut self, pg_idx: usize) -> Result<(), ExecErr> {
    let pg_opt = self.pages[pg_idx].as_ref().map(|nd| nd.serialize());
    if let Some(pg) = pg_opt {
      self.write_page(pg_idx, &pg)?;
    }
    Ok(())
  }

  fn write_page(&mut self, pg_idx: usize, page: &Page) -> Result<(), ExecErr> {
    self
      .file
      .seek(SeekFrom::Start((pg_idx * PAGE_SIZE) as u64))
      .map_err(|_| ExecErr::IoError("Fail seeking".to_string()))?;
    self
      .file
      .write_all(page)
      .map_err(|_| ExecErr::IoError("Fail writing".to_string()))
  }
}
