use assert_cmd::Command;
use predicates::prelude::predicate;

#[test]
fn insert_and_retrieve_a_row() {
    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let assert = cmd
        .write_stdin(["insert 0 foo foo@bar.com", "select", ".exit"].join("\n"))
        .assert();

    assert.success().stdout(
        [
            "db > Executed.",
            "db > (0, \"foo\", \"foo@bar.com\")",
            "Executed.",
            "db > ",
        ]
        .join("\n"),
    );
}

#[test]
fn table_is_full() {
    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let input: String = (0..1401)
        .map(|i| format!("insert {i} user{i} person{i}@example.com\n"))
        .collect();
    let assert = cmd.write_stdin(input + ".exit").assert();

    assert
        .success()
        .stdout(predicate::str::contains("Error: Table full."));
}
