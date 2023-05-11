use std::io::Write;

fn main() {
    loop {
        let mut input_buffer = InputBuffer::new();
        print_prompt();
        read_input(&mut input_buffer);

        if input_buffer.starts_with('.') {
            match do_meta_command(&input_buffer) {
                Ok(_) => continue,
                Err(MetaCmdErr::Unrecognized) => {
                    println!("Unrecognized command {input_buffer:?}.");
                    continue;
                }
            }
        }

        let statement = match prepare_statement(&input_buffer) {
            Ok(stmt) => stmt,
            Err(_) => {
                println!("Unrecognized keyword at start of {input_buffer:?}.");
                continue;
            }
        };

        execute_statement(&statement);

        println!("Executed.");
    }
}

type InputBuffer = String;

fn print_prompt() {
    print!("db > ");
    std::io::stdout().flush().unwrap();
}

fn read_input(input_buffer: &mut InputBuffer) {
    match std::io::stdin().read_line(input_buffer) {
        Ok(_) => if input_buffer.ends_with('\n') {
            input_buffer.pop();
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
    Ok(())
}

enum Statement {
    Insert,
    Select,
}

enum PrepareErr {
    Unrecognized,
}

fn prepare_statement(input: &InputBuffer) -> Result<Statement, PrepareErr> {
    match input {
        s if s.starts_with("insert") => Ok(Statement::Insert),
        s if s.starts_with("select") => Ok(Statement::Select),
        _ => Err(PrepareErr::Unrecognized)
    }
}

fn execute_statement(stmt: &Statement) {
    use Statement::*;
    match stmt {
        Insert => println!("This is where we would do an insert."),
        Select => println!("This is where we would do a select."),
    }
}
