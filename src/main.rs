use std::io::Write;

fn main() {
    let mut input_buffer = InputBuffer::new();
    loop {
        print_prompt();
        read_input(&mut input_buffer);

        match input_buffer.as_ref() {
            ".exit" => break,
            s => println!("Unrecognized command {s:?}.")
        }
        input_buffer.clear();
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