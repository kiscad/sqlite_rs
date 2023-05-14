use sqlite_rs::Table;

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty() {
        println!("Must supply a database filename.");
        std::process::exit(1);
    }
    let filename = &args[0];
    let mut table = match Table::open_db(filename.as_ref()) {
        Ok(t) => t,
        Err(e) => {
            println!("{e}");
            std::process::exit(1);
        }
    };
    loop {
        sqlite_rs::repl(&mut table);
    }
}
