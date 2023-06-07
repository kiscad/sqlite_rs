use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum MetaCmdErr {
  Unrecognized(String),
}

impl Display for MetaCmdErr {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Unrecognized(s) => write!(f, "{s}"),
    }
  }
}

impl std::error::Error for MetaCmdErr {}

#[derive(Debug)]
pub enum PrepareErr {
  Unrecognized(String),
  SyntaxErr(String),
  StringTooLong(String),
  NegativeId(String),
}

impl Display for PrepareErr {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::SyntaxErr(s) | Self::Unrecognized(s) | Self::NegativeId(s) | Self::StringTooLong(s) => {
        write!(f, "{s}")
      }
    }
  }
}

impl std::error::Error for PrepareErr {}

#[derive(Debug)]
pub enum ExecErr {
  TableFull(String),
  DuplicateKey(String),
  LeafNodeFull(String),
  InternNodeFull(String),
  PagerFull(String),
  PageNumOutBound(String),
  IoError(String),
  NodeError(String),
  CellNotFound(String),
}

impl Display for ExecErr {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::TableFull(s)
      | Self::DuplicateKey(s)
      | Self::PagerFull(s)
      | Self::LeafNodeFull(s)
      | Self::InternNodeFull(s)
      | Self::PageNumOutBound(s)
      | Self::IoError(s)
      | Self::NodeError(s)
      | Self::CellNotFound(s) => write!(f, "{s}"),
    }
  }
}

impl std::error::Error for ExecErr {}

#[derive(Debug)]
pub enum DbError {
  MetaCmdErr(MetaCmdErr),
  PrepareErr(PrepareErr),
  ExecErr(ExecErr),
}
