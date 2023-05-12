use lazy_static::lazy_static;
use regex::Regex;

fn main() {
    lazy_static!{
        static ref RE: Regex = Regex::new(r"(?x)
            insert
            \s+
            (\d+) # year
            \s+
            ([^\s]+) # month
            \s+
            ([^\s]+) # day
        ").unwrap();
    }
    let cap = RE.captures("insert 2023  chenchen chenchen@huawe.com").unwrap();
    println!("{:?}", &cap[1]);
    println!("{:?}", &cap[2]);
    println!("{:?}", &cap[3]);
}