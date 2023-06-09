use crate::btree::intern::{Child, Intern};
use crate::btree::leaf::Leaf;
use crate::btree::node::Node;
use crate::cursor::Cursor;
use crate::error::ExecErr;
use crate::pager::Pager;
use crate::row::Row;
use std::path::Path;

const ROOT: usize = 0;
pub const MAX_PAGES: usize = 100;

pub struct Table {
  pager: Pager,
}

impl Table {
  pub fn open_db(fname: impl AsRef<Path>) -> Result<Self, ExecErr> {
    let mut pager = Pager::new(fname)?;

    if pager.size() == 0 {
      let root = Node::Leaf(Leaf::new(true, None, None));
      pager.push_node(root)?;
    }
    Ok(Self { pager })
  }

  pub fn close_db(&self) -> Result<(), ExecErr> {
    self.pager.flush()
  }

  pub fn insert_row(&mut self, key: u32, row: &Row) -> Result<(), ExecErr> {
    let row = row.serialize();
    let leaf_idx = self.find_leaf_recur(ROOT, key)?;
    let res = self
      .pager
      .set_node_by(leaf_idx, |nd| nd.as_leaf_mut()?.insert_row(key, &row))?;

    match res {
      Err(ExecErr::LeafNodeFull(_)) => {
        let pg_idx_new = self.pager.size();
        let leaf = self.pager.set_node_by(leaf_idx, |nd| {
          nd.as_leaf_mut()?
            .insert_row_and_split(key, &row, pg_idx_new)
        })??;

        // update the key_max of the splitted node in the parent's child item
        let key_max = self
          .pager
          .get_node_do(leaf_idx, |nd| nd.as_leaf().unwrap().key_max())?;
        if let Some(parent) = leaf.parent {
          self.pager.set_node_by(parent, |nd| {
            nd.as_intern_mut()
              .unwrap()
              .find_mut_child_and(key_max, |ch| ch.key_max = key_max)
          })??;
        }

        let key_max = leaf.key_max();
        let parent = leaf.parent;
        self.pager.push_node(Node::Leaf(leaf))?;
        let child = Child::new(pg_idx_new, key_max);
        self.insert_child(child, parent)
      }
      others => others,
    }
  }

  pub fn new_cursor_by_key(&self, key: u32) -> Cursor {
    let leaf_idx = self.find_leaf_recur(ROOT, key).unwrap();
    let (cell_idx, at_end) = self
      .pager
      .get_node_do(leaf_idx, |nd| {
        let leaf = nd.as_leaf().unwrap();
        let cell_idx = leaf.search_cell_idx_by_key(key);
        let at_end = leaf.next.is_none() && cell_idx == leaf.size();
        (cell_idx, at_end)
      })
      .unwrap();
    Cursor::new(leaf_idx, cell_idx, at_end)
  }

  pub fn advance_cursor(&self, cursor: &mut Cursor) {
    cursor.cell_idx += 1;
    self
      .pager
      .get_node_do(cursor.leaf_idx, |nd| {
        let leaf = nd.as_leaf().unwrap();
        if leaf.size() == cursor.cell_idx {
          match leaf.next {
            None => cursor.at_end = true,
            Some(pid) => {
              cursor.leaf_idx = pid;
              cursor.cell_idx = 0;
            }
          }
        }
      })
      .unwrap();
  }

  pub fn select_row(&self, cursor: &Cursor) -> Row {
    let rowbytes = self
      .pager
      .get_node_do(cursor.leaf_idx, |nd| {
        nd.as_leaf().unwrap().cells[cursor.cell_idx].row
      })
      .unwrap();
    Row::deserialize_from(rowbytes)
  }

  pub fn btree_to_str(&self) -> String {
    self.btree_to_str_recur(ROOT)
  }

  fn find_leaf_recur(&self, pg_idx: usize, key: u32) -> Result<usize, ExecErr> {
    let (is_leaf, pid) = self.pager.get_node_do(pg_idx, |node| match node {
      Node::Intern(nd) => {
        let pg_idx = nd.find_child_and(key, |ch| ch.pg_idx)?;
        Ok((false, pg_idx))
      }
      Node::Leaf(_) => Ok((true, pg_idx)),
    })??;

    if is_leaf {
      Ok(pid)
    } else {
      self.find_leaf_recur(pid, key)
    }
  }

  fn insert_child(&mut self, child: Child, parent: Option<usize>) -> Result<(), ExecErr> {
    match parent {
      // base case
      None => self.new_root_and_insert_child(child),
      // recursive case
      Some(pg) => {
        let res = self.pager.set_node_by(pg, |nd| {
          nd.as_intern_mut()
            .unwrap()
            .insert_child(child.pg_idx, child.key_max)
        })?;
        match res {
          Err(ExecErr::InternNodeFull(_)) => {
            let intern = self.pager.set_node_by(pg, |nd| {
              nd.as_intern_mut()
                .unwrap()
                .insert_child_and_split(child.pg_idx, child.key_max)
            })??;
            let pg_idx_new = self.pager.size();
            let parent = intern.parent;
            self.pager.push_node(Node::Intern(intern))?;
            let key_max = self.key_max(pg_idx_new);
            let child = Child::new(pg_idx_new, key_max);
            self.insert_child(child, parent)
          }
          other => other,
        }
      }
    }
  }

  fn new_root_and_insert_child(&mut self, child_rht: Child) -> Result<(), ExecErr> {
    self.pager.set_node_by(child_rht.pg_idx, |nd| {
      nd.set_parent(Some(ROOT));
    })?;
    let pg_idx_new = self.pager.size();
    let key_max = self.key_max(ROOT);
    let root_new = self.pager.set_node_by(ROOT, |nd| {
      nd.set_is_root(false);
      nd.set_parent(Some(ROOT));
      let child_lft = Child::new(pg_idx_new, key_max);
      let children = vec![child_lft, child_rht.clone()];
      Node::Intern(Intern::new(true, None, children))
    })?;

    let mut root_old = self.pager.replace_node(ROOT, root_new).unwrap();
    root_old.set_parent(Some(ROOT));
    self.pager.push_node(root_old)?;
    Ok(())
  }

  fn key_max(&self, pg_idx: usize) -> u32 {
    self
      .pager
      .get_node_do(pg_idx, |node| match node {
        Node::Leaf(nd) => nd.key_max(),
        Node::Intern(nd) => nd
          .find_child_and(u32::MAX, |ch| self.key_max(ch.pg_idx))
          .unwrap(),
      })
      .unwrap()
  }

  fn btree_to_str_recur(&self, pg_idx: usize) -> String {
    let mut res = String::new();
    let node_str = self
      .pager
      .get_node_do(pg_idx, |nd| format!("{}\n", nd))
      .unwrap();
    res.push_str(&node_str);

    if self.pager.get_node_do(pg_idx, |nd| nd.is_leaf()).unwrap() {
      return res;
    }

    let s: String = self
      .pager
      .get_node_do(pg_idx, |nd| {
        nd.as_intern()
          .unwrap()
          .children
          .iter()
          .map(|x| x.pg_idx)
          .collect::<Vec<_>>()
      })
      .unwrap()
      .into_iter()
      .map(|pgid| {
        self
          .btree_to_str_recur(pgid)
          .lines()
          .map(|s| format!("  {}\n", s))
          .collect::<String>()
      })
      .collect();

    res.push_str(&s);
    res
  }
}
