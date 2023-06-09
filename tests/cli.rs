use assert_cmd::Command;

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
#[ignore]
fn table_is_full() {
  let filename = "table_is_full.db";

  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let script: String = (0..14)
    .map(|i| format!("insert {i} user{i} person{i}@example.com\n"))
    .collect();
  let assert = cmd.arg(filename).write_stdin(script + ".exit").assert();

  let _ = std::fs::remove_file(filename);

  assert.success().stderr("Error: Table full.\n");
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
    .write_stdin([&format!("insert 0 {long_username} {long_email}"), ".exit"].join("\n"))
    .assert();

  let _ = std::fs::remove_file(filename);

  assert
    .success()
    .stderr("String too long\n")
    .stdout("db > db > ");
}

#[test]
fn print_error_msg_if_id_is_negative() {
  let filename = "print_error_msg_if_id_is_negative.db";

  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let assert = cmd
    .arg(filename)
    .write_stdin(["insert -1 foo foo@bar.com", ".exit"].join("\n"))
    .assert();

  let _ = std::fs::remove_file(filename);

  assert
    .success()
    .stdout("db > db > ")
    .stderr("ID must be positive.\n");
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

#[test]
fn print_structure_of_leaf_node() {
  let filename = "print_structure_of_leaf_node.db";
  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let mut script: String = [3, 1, 2]
    .iter()
    .map(|i| format!("insert {i} user{i} person{i}@example.com\n"))
    .collect();
  script.push_str(".btree\n.exit");
  let assert = cmd.arg(filename).write_stdin(script).assert();

  let _ = std::fs::remove_file(filename);

  assert.success().stdout(
    [
      "db > Executed.",
      "db > Executed.",
      "db > Executed.",
      "db > Tree:",
      "leaf (size 3)",
      "  - 1",
      "  - 2",
      "  - 3",
      "\nExecuted.",
      "db > ",
    ]
    .join("\n"),
  );
}

#[test]
fn print_error_when_insert_duplicate_key() {
  let filename = "print_error_when_insert_duplicate_key.db";
  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let assert = cmd
    .arg(filename)
    .write_stdin(
      [
        "insert 1 user1 person1@example.com",
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
      ]
      .join("\n"),
    )
    .assert();

  let _ = std::fs::remove_file(filename);

  assert
    .success()
    .stdout(
      [
        "db > Executed.",
        "db > db > (1, \"user1\", \"person1@example.com\")",
        "Executed.",
        "db > ",
      ]
      .join("\n"),
    )
    .stderr("Duplicated key\n");
}

#[test]
fn allows_printing_out_the_structure_of_2_leaf_node_btree() {
  let filename = "allows_printing_out_the_structure_of_2_leaf_node_btree.db";
  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let mut script: String = (0..14)
    .map(|i| format!("insert {i} user{i} person{i}@example.com\n"))
    .collect();
  script.push_str(".btree\n.exit");
  let assert = cmd.arg(filename).write_stdin(script).assert();

  let _ = std::fs::remove_file(filename);
  let mut expect: String = (0..14).map(|_| "db > Executed.\n").collect();
  expect.push_str("db > Tree:\nintern (size 2)\n  leaf (size 7)\n");
  expect.push_str(&(0..7).map(|i| format!("    - {i}\n")).collect::<String>());
  expect.push_str("  leaf (size 7)\n");
  expect.push_str(&(7..14).map(|i| format!("    - {i}\n")).collect::<String>());
  expect.push_str("\nExecuted.\ndb > ");
  assert.success().stdout(expect);
}

#[test]
fn allows_printing_out_the_structure_of_3_leaf_node_btree() {
  let filename = "allows_printing_out_the_structure_of_3_leaf_node_btree.db";
  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let mut script: String = (0..21)
    .map(|i| format!("insert {i} user{i} person{i}@example.com\n"))
    .collect();
  script.push_str(".btree\n.exit");
  let assert = cmd.arg(filename).write_stdin(script).assert();

  let _ = std::fs::remove_file(filename);

  let mut expect: String = (0..21).map(|_| "db > Executed.\n").collect();
  expect.push_str("db > Tree:\nintern (size 3)\n  leaf (size 7)\n");
  expect.push_str(&(0..7).map(|i| format!("    - {i}\n")).collect::<String>());
  expect.push_str("  leaf (size 7)\n");
  expect.push_str(&(7..14).map(|i| format!("    - {i}\n")).collect::<String>());
  expect.push_str("  leaf (size 7)\n");
  expect.push_str(&(14..21).map(|i| format!("    - {i}\n")).collect::<String>());
  expect.push_str("\nExecuted.\ndb > ");
  assert.success().stdout(expect);
}

#[test]
#[should_panic]
fn prints_all_rows_in_a_multi_level_tree() {
  let filename = "prints_all_rows_in_a_multi_level_tree.db";
  let mut cmd = Command::cargo_bin("sqlite_rs").unwrap();
  let mut script: String = (0..15)
    .map(|i| format!("insert {i} user{i} person{i}@com\n"))
    .collect();
  script.push_str("select\n.exit");
  let assert = cmd.arg(filename).write_stdin(script).assert();

  let _ = std::fs::remove_file(filename);
  let mut expect: String = (0..15).map(|_| "db > Executed.\n").collect();
  expect.push_str("db > ");
  let select_output: String = (0..15)
    .map(|i| {
      format!(
        "(i, {:?}, {:?})\n",
        format!("user{i}"),
        format!("person{i}@com")
      )
    })
    .collect();
  expect.push_str(&select_output);
  expect.push_str("Executed.\ndb > ");
  assert.success().stdout(expect);
}
