use sqlite_rs::Table;

fn main() {
    let mut table = Table::new();
    loop {
        sqlite_rs::repl(&mut table);
    }
}
