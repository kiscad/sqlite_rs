use lazy_static::lazy_static;
use regex::Regex;
use std::ffi::CStr;
use std::fmt::Formatter;
use std::io::{Read, Write};

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

enum MetaCmdErr {
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

enum PrepareErr {
    Unrecognized,
    SyntaxErr,
}

fn prepare_statement(input: &InputBuffer) -> Result<Statement, PrepareErr> {
    lazy_static! {
        static ref RE_INSERT: Regex = Regex::new(
            r"(?x)
            insert
            \s+
            (\d+)      # id
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
                let id = cap[1].parse::<u32>().unwrap();
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
        let rowb = table.row_slot(i + 1);
        let row = deserialize_row(rowb);
        println!("{row}");
    }
    Ok(())
}

/* Row Data Structure */

const COL_USERNAME_SIZE: usize = 32;
const COL_EMAIL_SIZE: usize = 255;

#[derive(Debug)]
struct Row {
    id: u32,
    username: [u8; COL_USERNAME_SIZE],
    email: [u8; COL_EMAIL_SIZE],
}

impl Row {
    fn build(id: u32, name: &str, mail: &str) -> Result<Self, PrepareErr> {
        if name.len() > COL_USERNAME_SIZE - 1 || mail.len() > COL_EMAIL_SIZE - 1 {
            return Err(PrepareErr::SyntaxErr);
        }
        let mut username = [0u8; COL_USERNAME_SIZE];
        username[..name.len()].copy_from_slice(name.as_bytes());
        let mut email = [0u8; COL_EMAIL_SIZE];
        email[..mail.len()].copy_from_slice(mail.as_bytes());
        Ok(Self {
            id,
            username,
            email,
        })
    }
    fn new(id: u32, username: [u8; COL_USERNAME_SIZE], email: [u8; COL_EMAIL_SIZE]) -> Self {
        Self {
            id,
            username,
            email,
        }
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let username = CStr::from_bytes_until_nul(&self.username)
            .unwrap()
            .to_string_lossy();
        let email = CStr::from_bytes_until_nul(&self.email)
            .unwrap()
            .to_string_lossy();
        write!(f, "({}, {:?}, {:?})", self.id, username, email)
    }
}

/* Page Data Structure */

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = COL_USERNAME_SIZE;
const EMAIL_SIZE: usize = COL_EMAIL_SIZE;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

struct RowB<'a>(&'a mut [u8]);

fn serialize_row(row: &Row, rowb: RowB) {
    let mut w = std::io::Cursor::new(rowb.0);
    let id_bytes: [u8; 4] = row.id.to_be_bytes();
    w.write(&id_bytes).unwrap();
    w.write(&row.username).unwrap();
    w.write(&row.email).unwrap();
}

fn deserialize_row(rowb: RowB) -> Row {
    let mut w = std::io::Cursor::new(rowb.0);
    let mut buf = [0u8; 4];
    w.read(&mut buf).unwrap();
    let id = u32::from_be_bytes(buf);
    let mut name_buf = [0u8; 32];
    w.read(&mut name_buf).unwrap();
    let mut email_buf = [0u8; 255];
    w.read(&mut email_buf).unwrap();
    Row::new(id, name_buf, email_buf)
}

/* Table Structure */

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Page([u8; PAGE_SIZE]);

impl Page {
    fn new() -> Self {
        Self([0; PAGE_SIZE])
    }
}

pub struct Table {
    num_rows: usize,
    pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
}

impl Table {
    fn row_slot(&mut self, row_num: usize) -> RowB {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = &mut self.pages[page_num];
        let page = page.get_or_insert_with(|| Box::new(Page::new()));
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        RowB(&mut page.0[byte_offset..byte_offset + ROW_SIZE])
    }

    pub fn new() -> Self {
        const INIT: Option<Box<Page>> = None;
        Self {
            num_rows: 0,
            pages: [INIT; TABLE_MAX_PAGES],
        }
    }
}
