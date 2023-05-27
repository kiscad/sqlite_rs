mod btree;
mod cursor;
pub mod error;
mod pager;
mod row;
mod table;

use std::num::IntErrorKind;

use lazy_static::lazy_static;
use regex::Regex;

use cursor::Cursor;
use error::{DbError, ExecErr, MetaCmdErr, PrepareErr};
use row::Row;

pub use table::Table;

pub fn run_cmd(cmd_str: &str, table: &Table) -> Result<(), DbError> {
    if cmd_str.starts_with('.') {
        return do_meta_command(cmd_str, table).map_err(DbError::MetaCmdErr);
    }

    let statement = prepare_statement(cmd_str).map_err(DbError::PrepareErr)?;

    execute_statement(&statement, table).map_err(DbError::ExecErr)
}

fn do_meta_command(cmd_str: &str, table: &Table) -> Result<(), MetaCmdErr> {
    match cmd_str {
        ".exit" => {
            table.close_db();
            std::process::exit(0);
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
        }
        ".btree" => {
            println!("Tree:");
            println!("{}", table.btree_to_string(table.root_idx));
        }
        _ => {
            return Err(MetaCmdErr::Unrecognized(format!(
                "Unrecognized command {cmd_str:?}."
            )));
        }
    }
    Ok(())
}

enum Statement {
    Insert(Box<Row>),
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
                Ok(Statement::Insert(Box::new(Row::build(
                    id, &cap[2], &cap[3],
                )?)))
            }
            None => Err(PrepareErr::SyntaxErr(syntax_err)),
        },
        s if s.starts_with("select") => Ok(Statement::Select),
        _ => Err(PrepareErr::Unrecognized(format!(
            "Unrecognized keyword at start of {cmd_str:?}."
        ))),
    }
}

fn execute_statement(stmt: &Statement, table: &Table) -> Result<(), ExecErr> {
    use Statement::*;
    match stmt {
        Insert(row) => execute_insert(row, table),
        Select => execute_select(table),
    }
}

fn execute_insert(row: &Row, table: &Table) -> Result<(), ExecErr> {
    let key = row.id;
    let mut cursor = Cursor::new_by_key(table, key);
    row.insert_to(&mut cursor)?;
    Ok(())
}

fn execute_select(table: &Table) -> Result<(), ExecErr> {
    let mut cursor = Cursor::new_at_table_start(table);
    while !cursor.end_of_table {
        let mut row = Row::default();
        row.read_from(&mut cursor)?;
        println!("{row}");
        cursor.advance()?;
    }
    Ok(())
}

fn print_constants() {
    println!("ROW_SIZE:                  {}", row::ROW_SIZE);
    println!("LEAF_NODE_HEADER_SIZE:     {}", btree::LEAF_HEADER_SIZE);
    println!(
        "LEAF_NODE_SPACE_FOR_CELLS: {}",
        pager::PAGE_SIZE - btree::LEAF_HEADER_SIZE
    );
    println!("LEAF_NODE_MAX_CELLS:       {}", btree::LEAF_MAX_CELLS);
}
