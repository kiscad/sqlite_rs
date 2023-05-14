mod pager;
mod row;
mod table;

use lazy_static::lazy_static;
use regex::Regex;
use row::{deserialize_row, serialize_row, Row};
use std::io::Write;
use std::num::IntErrorKind;
use table::TABLE_MAX_ROWS;

pub use table::Table;

pub fn repl(table: &mut Table) {
    let mut input_buffer = InputBuffer::new();
    print_prompt();
    read_input(&mut input_buffer);

    if input_buffer.starts_with('.') {
        match do_meta_command(&input_buffer) {
            Err(MetaCmdErr::Unrecognized) => println!("Unrecognized command {input_buffer:?}."),
            _ => (),
        }
        return;
    }

    let statement = match prepare_statement(&input_buffer) {
        Ok(stmt) => stmt,
        Err(PrepareErr::SyntaxErr) => {
            println!("Syntax error. Could not parse statement.");
            return;
        }
        Err(PrepareErr::StringTooLong) => {
            println!("String is too long.");
            return;
        }
        Err(PrepareErr::NegativeId) => {
            println!("ID must be positive.");
            return;
        }
        Err(PrepareErr::Unrecognized) => {
            println!("Unrecognized keyword at start of {input_buffer:?}.");
            return;
        }
    };

    match execute_statement(&statement, table) {
        Ok(_) => println!("Executed."),
        Err(ExecErr::TableFull) => {
            println!("Error: Table full.");
            return;
        }
    }
}

type InputBuffer = String;

fn print_prompt() {
    print!("db > ");
    std::io::stdout().flush().unwrap();
}

fn read_input(input_buffer: &mut InputBuffer) {
    match std::io::stdin().read_line(input_buffer) {
        Ok(_) => {
            if input_buffer.ends_with('\n') {
                input_buffer.pop();
            }
        }
        Err(_) => {
            println!("Error reading input!");
            std::process::exit(1);
        }
    }
}

pub enum MetaCmdErr {
    Unrecognized,
}

fn do_meta_command(input: &InputBuffer) -> Result<(), MetaCmdErr> {
    match input.as_ref() {
        ".exit" => std::process::exit(0),
        _ => return Err(MetaCmdErr::Unrecognized),
    }
    // Ok(())
}

enum Statement {
    Insert(Row),
    Select,
}

pub enum PrepareErr {
    Unrecognized,
    SyntaxErr,
    StringTooLong,
    NegativeId,
}

fn prepare_statement(input: &InputBuffer) -> Result<Statement, PrepareErr> {
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
    match input {
        s if s.starts_with("insert") => match RE_INSERT.captures(input) {
            Some(cap) => {
                let id = match cap[1].parse::<u32>() {
                    Ok(v) => v,
                    Err(e) if e.kind() == &IntErrorKind::InvalidDigit => {
                        return Err(PrepareErr::NegativeId)
                    }
                    Err(_) => return Err(PrepareErr::SyntaxErr),
                };
                Ok(Statement::Insert(Row::build(id, &cap[2], &cap[3])?))
            }
            None => Err(PrepareErr::SyntaxErr),
        },
        s if s.starts_with("select") => Ok(Statement::Select),
        _ => Err(PrepareErr::Unrecognized),
    }
}

fn execute_statement(stmt: &Statement, table: &mut Table) -> Result<(), ExecErr> {
    use Statement::*;
    match stmt {
        Insert(row) => execute_insert(row, table),
        Select => execute_select(table),
    }
}

enum ExecErr {
    TableFull,
}

fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecErr> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(ExecErr::TableFull);
    }
    let rowb = table.row_slot(row.id as usize);
    serialize_row(row, rowb);
    table.num_rows += 1;
    Ok(())
}

fn execute_select(table: &mut Table) -> Result<(), ExecErr> {
    for i in 0..table.num_rows {
        let rowb = table.row_slot(i);
        let row = deserialize_row(rowb);
        println!("{row}");
    }
    Ok(())
}
