use assert_cmd::Command;

#[test]
fn insert_2_records_and_select() {
    let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
    let assert = cmd
        .write_stdin(
            [
                "insert 1 foo foo@bar.com",
                "insert 2 bob bob@example.com",
                "select",
                ".exit",
            ]
            .join("\n"),
        )
        .assert();

    assert.success().stdout(
        [
            "db > Executed.",
            "db > Executed.",
            "db > (1, \"foo\", \"foo@bar.com\")",
            "(2, \"bob\", \"bob@example.com\")",
            "Executed.",
            "db > ",
        ]
        .join("\n"),
    );
}
