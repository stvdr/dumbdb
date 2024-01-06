use std::path::Path;
use std::sync::{Arc, Mutex};
use crate::file_manager::{FileManager, Page, PAGE_SIZE};

pub struct LogManager {
    file_manager: FileManager,
    current_page: Mutex<Page>,
    current_position: usize,
}

impl LogManager {
    pub fn new(root_directory: &Path) -> Self {
        let file_manager = FileManager::new(root_directory);
        let mut page = Page::new();
        page.write_at_offset(0u32, 0);

        let block_id = file_manager.append_block("log", &page).unwrap();

        LogManager {
            file_manager,
            current_page: Mutex::new(page),
            current_position: PAGE_SIZE,
        }
    }

    pub fn append(record: &[u8]) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_append_record() {}
}