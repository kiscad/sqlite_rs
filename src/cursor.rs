pub struct Cursor {
  pub leaf_idx: usize,
  pub cell_idx: usize,
  pub at_end: bool,
}

impl Cursor {
  pub fn new(leaf_idx: usize, cell_idx: usize, at_end: bool) -> Self {
    Self {
      leaf_idx,
      cell_idx,
      at_end,
    }
  }
}
