use crate::file_manager::{BlockId, FileManager, Page, PAGE_SIZE};
use std::mem::size_of;
use std::path::Path;

const LOG_NAME: &str = "log";

type LogPage = Page;
type frontier = u32;
const FRONTIER_POS: usize = PAGE_SIZE;
const FRONTIER_START: usize = PAGE_SIZE - size_of::<frontier>();

pub struct LogManager {
    file_manager: FileManager,
    page: LogPage,
    block_num: u32,
}

trait ImplLogPage {
    fn get_frontier(&self) -> u32;
    fn set_frontier(&mut self, f: u32);
}

impl ImplLogPage for LogPage {
    fn get_frontier(&self) -> u32 {
        {
            self.read_backwards::<u32>(FRONTIER_POS)
        }
    }

    fn set_frontier(&mut self, f: u32) {
        self.write_backwards(f, FRONTIER_POS);
    }
}

impl LogManager {
    pub fn new(root_directory: &Path) -> Self {
        let file_manager = FileManager::new(root_directory);

        let num_blocks = file_manager.num_blocks(&LOG_NAME).unwrap();

        let mut page = Page::new();

        let block_num = if num_blocks == 0 {
            // If there are currently no blocks in the file, a new file needs to be created.
            // Create the file and set the initial frontier.
            page.write_backwards((PAGE_SIZE - 4) as u32, PAGE_SIZE);
            file_manager.append_block(&LOG_NAME, &page).unwrap().num()
        } else {
            // Get the last block from
            let block_num = num_blocks - 1;
            let _ = file_manager
                .get_block(&BlockId::new(LOG_NAME, block_num), &mut page)
                .expect("failed to read block");
            block_num
        };

        LogManager {
            file_manager,
            page,
            block_num: block_num as u32,
        }
    }

    fn append_block(&mut self) {
        self.page = Page::new();
        self.page
            .write_backwards(FRONTIER_START as u32, FRONTIER_POS);
        self.block_num = self
            .file_manager
            .append_block(&LOG_NAME, &self.page)
            .unwrap()
            .num() as u32;
    }

    pub fn append(&mut self, record: &[u8]) {
        // If the record will fit in the existing page, place it there and update the frontier
        // Otherwise, create a new block

        let len = record.len();

        // TODO: hardcoding the value "4" here
        // TODO: verify the math here
        if self.page.get_frontier() < len as u32 + 4 {
            // record won't fit in the existing page, append a new block
            self.append_block()
        }

        let frontier = self.page.get_frontier();
        let frontier = self.page.write_backwards(record, frontier as usize) as u32;
        self.page.set_frontier(frontier);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_append_records() {
        let td = tempdir().unwrap();
        let root_dir = td.path().join("data");
        fs::create_dir_all(&root_dir).expect("Failed to create root directory");

        let lm = LogManager::new(&root_dir);

        assert_eq!(lm.block_num, 0);
    }
}
