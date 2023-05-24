use sqlite_rs::Table;
use std::io::Write;
use std::process;

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let filename = parse_args(&args).unwrap_or_else(|e| {
        eprintln!("{e}");
        process::exit(1);
    });

    let table = Table::open_db(filename).unwrap_or_else(|e| {
        eprintln!("{e}");
        process::exit(1);
    });

    loop {
        let mut cmd_line = String::new();

        print_prompt();
        read_command(&mut cmd_line);

        use sqlite_rs::error::DbError;
        match sqlite_rs::run_cmd(&cmd_line, &table) {
            Ok(_) => println!("Executed."),
            Err(DbError::MetaCmdErr(e)) => eprintln!("{e}"),
            Err(DbError::PrepareErr(e)) => eprintln!("{e}"),
            Err(DbError::ExecErr(e)) => eprintln!("{e}"),
        }
    }
}

fn parse_args(args: &[String]) -> Result<&str, String> {
    if args.is_empty() {
        Err("Must supply a database filename.".to_string())
    } else {
        Ok(&args[0])
    }
}

fn print_prompt() {
    print!("db > ");
    std::io::stdout().flush().unwrap();
}

fn read_command(input_buffer: &mut String) {
    match std::io::stdin().read_line(input_buffer) {
        Ok(_) => {
            if input_buffer.ends_with('\n') {
                input_buffer.pop();
            }
        }
        Err(_) => {
            println!("Error reading input!");
            process::exit(1);
        }
    }
}
