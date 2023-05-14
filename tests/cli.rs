use assert_cmd::Command;
use predicates::prelude::predicate;
use std::fs::OpenOptions;

#[test]
fn insert_and_retrieve_a_row() {
    let username = "foo";
    let email = "foo@bar.com";
    let filename = "insert_and_retrieve_a_row.db";

    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let assert = cmd
        .arg(filename)
        .write_stdin([&format!("insert 0 {username} {email}"), "select", ".exit"].join("\n"))
        .assert();

    let _ = std::fs::remove_file(filename);

    assert.success().stdout(
        [
            "db > Executed.",
            &format!("db > (0, {username:?}, {email:?})"),
            "Executed.",
            "db > ",
        ]
        .join("\n"),
    );
}

#[test]
fn table_is_full() {
    let filename = "table_is_full.db";

    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let script: String = (0..1401)
        .map(|i| format!("insert {i} user{i} person{i}@example.com\n"))
        .collect();
    let assert = cmd.arg(filename).write_stdin(script + ".exit").assert();

    let _ = std::fs::remove_file(filename);

    assert
        .success()
        .stdout(predicate::str::contains("Error: Table full."));
}

#[test]
fn allows_insert_strings_of_maximum_length() {
    let long_username = ["a"; 32].join("");
    let long_email = ["a"; 255].join("");
    let filename = "allows_insert_strings_of_max_length.db";

    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let assert = cmd
        .arg(filename)
        .write_stdin(
            [
                &format!("insert 0 {long_username} {long_email}"),
                "select",
                ".exit",
            ]
            .join("\n"),
        )
        .assert();

    let _ = std::fs::remove_file(filename);

    assert.success().stdout(
        [
            "db > Executed.",
            &format!("db > (0, {long_username:?}, {long_email:?})"),
            "Executed.",
            "db > ",
        ]
        .join("\n"),
    );
}

#[test]
fn print_error_msg_if_string_too_long() {
    let long_username = ["a"; 33].join("");
    let long_email = ["a"; 256].join("");
    let filename = "print_error_msg_if_string_too_long.db";

    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let assert = cmd
        .arg(filename)
        .write_stdin(
            [
                &format!("insert 0 {long_username} {long_email}"),
                "select",
                ".exit",
            ]
            .join("\n"),
        )
        .assert();

    let _ = std::fs::remove_file(filename);

    assert
        .success()
        .stdout(["db > String is too long.", "db > Executed.", "db > "].join("\n"));
}

#[test]
fn print_error_msg_if_id_is_negative() {
    let filename = "print_error_msg_if_id_is_negative.db";

    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let assert = cmd
        .arg(filename)
        .write_stdin(["insert -1 cstack foo@bar.com", "select", ".exit"].join("\n"))
        .assert();

    let _ = std::fs::remove_file(filename);

    assert
        .success()
        .stdout(["db > ID must be positive.", "db > Executed.", "db > "].join("\n"));
}

#[test]
fn keeps_data_after_closing_connection() {
    let user = "user1";
    let email = "person1@example.com";
    let filename = "keeps_data_after_closing_connection.db";

    Command::cargo_bin("sqlite_rs")
        .unwrap()
        .arg(filename)
        .write_stdin([&format!("insert 0 {user} {email}"), ".exit"].join("\n"))
        .assert()
        .success()
        .stdout(["db > Executed.", "db > "].join("\n"));

    let assert = Command::cargo_bin("sqlite_rs")
        .unwrap()
        .arg(filename)
        .write_stdin(["select", ".exit"].join("\n"))
        .assert();

    let _ = std::fs::remove_file(filename);

    assert.success().stdout(
        [
            &format!("db > (0, {user:?}, {email:?})"),
            "Executed.",
            "db > ",
        ]
        .join("\n"),
    );
}
