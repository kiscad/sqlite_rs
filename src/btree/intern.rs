use crate::pager::{Page, PAGE_SIZE};
use std::fmt;
use std::io::{self, BufRead, Read, Write};

pub struct PageKey {
    pub page: u32,
    pub key: u32,
}

impl PageKey {
    pub fn new(page: u32, key: u32) -> Self {
        Self { page, key }
    }
}

#[derive(Default)]
pub struct Intern {
    pub is_root: bool,
    pub parent: u32,
    pub children: Vec<PageKey>,
    pub right_child_page: u32,
}

impl Intern {
    pub fn new_root() -> Self {
        Self {
            is_root: true,
            parent: 0,
            children: vec![],
            right_child_page: 0,
        }
    }

    pub fn new(left_page: u32, left_key: u32, right_page: u32) -> Self {
        let page_key = PageKey::new(left_page, left_key);
        Self {
            is_root: false,
            parent: 0,
            children: vec![page_key],
            right_child_page: right_page,
        }
    }

    pub fn get_child_by(&self, cell_key: u32) -> u32 {
        // binary search
        let mut lower = 0;
        let mut upper = self.children.len();

        while lower < upper {
            let mid = (lower + upper) / 2;
            let key = self.children[mid].key;
            if cell_key <= key {
                upper = mid;
            } else {
                lower = mid + 1;
            }
        }

        if lower >= self.children.len() {
            self.right_child_page
        } else {
            self.children[lower].page
        }
    }

    pub fn get_start_child(&self) -> u32 {
        let PageKey { page, .. } = &self.children[0];
        *page
    }

    pub fn new_from_page(page: &Page) -> Self {
        let mut node = Self::default();
        node.read_page(page);
        node
    }

    pub fn serialize(&self) -> Page {
        let mut cache = [0u8; PAGE_SIZE];
        let mut writer = io::Cursor::new(&mut cache[..]);
        // write node-type: is-leaf
        writer.write_all(&[u8::from(false)]).unwrap();
        writer.write_all(&[u8::from(self.is_root)]).unwrap();
        writer.write_all(&self.parent.to_be_bytes()).unwrap();
        let num_keys = self.children.len() as u32;
        writer.write_all(&num_keys.to_be_bytes()).unwrap();
        writer
            .write_all(&self.right_child_page.to_be_bytes())
            .unwrap();
        for PageKey { page, key } in &self.children {
            writer.write_all(&page.to_be_bytes()).unwrap();
            writer.write_all(&key.to_be_bytes()).unwrap();
        }
        cache
    }

    fn read_page(&mut self, page: &Page) {
        let mut reader = io::Cursor::new(page);
        reader.consume(1);
        let mut is_root = [0; 1];
        reader.read_exact(&mut is_root).unwrap();
        self.is_root = is_root[0] != 0;
        let mut parent = [0; 4];
        reader.read_exact(&mut parent).unwrap();
        self.parent = u32::from_be_bytes(parent);
        let mut num_keys = [0; 4];
        reader.read_exact(&mut num_keys).unwrap();
        let num_keys = u32::from_be_bytes(num_keys);
        let mut right = [0; 4];
        reader.read_exact(&mut right).unwrap();
        self.right_child_page = u32::from_be_bytes(right);

        self.children.clear();
        for _ in 0..num_keys {
            let mut page_num = [0u8; 4];
            reader.read_exact(&mut page_num).unwrap();
            let mut cell_key = [0u8; 4];
            reader.read_exact(&mut cell_key).unwrap();
            self.children.push(PageKey::new(
                u32::from_be_bytes(page_num),
                u32::from_be_bytes(cell_key),
            ))
        }
    }
}

impl fmt::Display for Intern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "internal (size {})", self.children.len())
    }
}
