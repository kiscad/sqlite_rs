#![feature(iter_array_chunks)]
#![feature(split_array)]

mod btree;
mod cursor;
mod pager;
mod row;
mod table;

use std::fmt::{Display, Formatter};
use std::num::IntErrorKind;

use lazy_static::lazy_static;
use regex::Regex;

use cursor::Cursor;
use row::{deserialize_row, serialize_row, Row};
use table::TABLE_MAX_ROWS;

use crate::btree::{Node, LEAF_NODE_MAX_CELLS};
pub use table::Table;

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
            Self::SyntaxErr(s)
            | Self::Unrecognized(s)
            | Self::NegativeId(s)
            | Self::StringTooLong(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for PrepareErr {}

#[derive(Debug)]
pub enum ExecErr {
    TableFull(String),
}

impl Display for ExecErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableFull(s) => write!(f, "{s}"),
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

pub fn run_cmd(cmd_str: &str, table: &mut Table) -> Result<(), DbError> {
    if cmd_str.starts_with('.') {
        return do_meta_command(cmd_str, table).map_err(|e| DbError::MetaCmdErr(e));
    }

    let statement = prepare_statement(cmd_str).map_err(|e| DbError::PrepareErr(e))?;

    execute_statement(&statement, table).map_err(|e| DbError::ExecErr(e))
}

fn do_meta_command(cmd_str: &str, table: &mut Table) -> Result<(), MetaCmdErr> {
    match cmd_str.as_ref() {
        ".exit" => {
            table.close_db();
            std::process::exit(0);
        }
        _ => Err(MetaCmdErr::Unrecognized(format!(
            "Unrecognized command {cmd_str:?}."
        ))),
    }
}

enum Statement {
    Insert(Row),
    Select,
}

fn prepare_statement(cmd_str: &str) -> Result<Statement, PrepareErr> {
    lazy_static! {
        static ref RE_INSERT: Regex = Regex::new(
            r"(?x)
            insert
            \s+
            (-?\d+)      # id
            \s+
            ([^\s]+)    # username
            \s+
            ([^\s]+)    # email
        "
        )
        .unwrap();
    }
    let syntax_err = "Syntax error. Could not parse statement.".to_string();
    match cmd_str {
        s if s.starts_with("insert") => match RE_INSERT.captures(cmd_str) {
            Some(cap) => {
                let id = match cap[1].parse::<u32>() {
                    Ok(v) => v,
                    Err(e) if e.kind() == &IntErrorKind::InvalidDigit => {
                        return Err(PrepareErr::NegativeId("ID must be positive.".to_string()))
                    }
                    Err(_) => return Err(PrepareErr::SyntaxErr(syntax_err)),
                };
                Ok(Statement::Insert(Row::build(id, &cap[2], &cap[3])?))
            }
            None => Err(PrepareErr::SyntaxErr(syntax_err)),
        },
        s if s.starts_with("select") => Ok(Statement::Select),
        _ => Err(PrepareErr::Unrecognized(format!(
            "Unrecognized keyword at start of {cmd_str:?}."
        ))),
    }
}

fn execute_statement(stmt: &Statement, table: &mut Table) -> Result<(), ExecErr> {
    use Statement::*;
    match stmt {
        Insert(row) => execute_insert(row, table),
        Select => execute_select(table),
    }
}

fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecErr> {
    let node = table.pager.get_leaf_node(table.root_page_num).unwrap();

    if node.get_num_cells() as usize >= LEAF_NODE_MAX_CELLS {
        return Err(ExecErr::TableFull("Error: Table full.".to_string()));
    }

    let mut cursor = Cursor::new_at_table_end(table);
    let cell_num = cursor.cell_num as u32;
    let page_num = cursor.page_num;
    let mut node = table.pager.get_leaf_node(page_num).unwrap();
    node.write_cell_value(cell_num, |w| row.serialize_to(w));
    // serialize_row(row, cursor.get_row_bytes());
    // leaf_node_insert(cursor, row.id, row);

    Ok(())
}

fn execute_select(table: &mut Table) -> Result<(), ExecErr> {
    let mut cursor = Cursor::new_at_table_start(table);
    while !cursor.end_of_table {
        // let row = deserialize_row(cursor.get_row_bytes());
        // println!("{row}");
        cursor.advance();
    }
    Ok(())
}
