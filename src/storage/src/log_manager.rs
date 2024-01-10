use crate::file_manager::{BlockId, FileManager, Page, PAGE_SIZE};
use std::mem::size_of;
use std::path::Path;

const LOG_NAME: &str = "log";

type LogPage = Page;
type Frontier = u32;
const FRONTIER_POS: usize = 0;
const FRONTIER_START: usize = size_of::<Frontier>();

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
        self.read::<u32>(FRONTIER_POS)
    }

    fn set_frontier(&mut self, f: u32) {
        self.write(f, FRONTIER_POS);
    }
}

// TODO: error handling
impl LogManager {
    pub fn new(root_directory: &Path) -> Self {
        let file_manager = FileManager::new(root_directory);

        let num_blocks = file_manager.num_blocks(&LOG_NAME).unwrap();

        let mut page = Page::new();

        let block_num = if num_blocks == 0 {
            // If there are currently no blocks in the file, a new file needs to be created.
            // Create the file and set the initial frontier.
            page.write(FRONTIER_START as u32, FRONTIER_POS);
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
        self.page.write(FRONTIER_START as u32, FRONTIER_POS);
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
        let mut frontier = self.page.get_frontier() as usize;

        println!("{}", frontier);
        if frontier + len + size_of::<u32>() >= PAGE_SIZE {
            // the record won't fit in the existing page, append a new block
            self.flush();
            self.append_block();

            // refresh the frontier, as it will now point to the start of the newly created block
            frontier = self.page.get_frontier() as usize;
        }

        frontier += self.page.write_bytes(record, frontier);
        frontier += self.page.write(len as u32, frontier);
        self.page.set_frontier(frontier as u32);
    }

    pub fn flush(&self) {
        self.file_manager
            .write_block(
                &BlockId::new(&LOG_NAME, self.block_num as usize),
                &self.page,
            )
            .unwrap();
    }

    pub fn snapshot(&self) -> LogManagerSnapshot {
        self.flush();

        // TODO: block_num should prob not be a usize?
        let block = BlockId::new(LOG_NAME, self.block_num as usize);
        let mut page = Page::new();
        self.file_manager.get_block(&block, &mut page).unwrap();

        LogManagerSnapshot {
            file_manager: &self.file_manager,
            block,
            page,
            current_pos: FRONTIER_START as u32,
        }
    }
}

#[derive(Debug)]
pub struct LogManagerSnapshot<'a> {
    file_manager: &'a FileManager,
    block: BlockId,
    page: LogPage,
    current_pos: u32,
}

// TODO: error handling
impl Iterator for LogManagerSnapshot<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(self.current_pos >= FRONTIER_START as u32);

        if self.current_pos == FRONTIER_START as u32 {
            self.block = self.block.previous()?;
            self.page = LogPage::new();
            self.file_manager
                .get_block(&self.block, &mut self.page)
                .unwrap();

            self.current_pos = self.page.get_frontier();
        }

        self.current_pos -= size_of::<u32>() as u32;
        let len = self.page.read::<u32>(self.current_pos as usize) as usize;
        self.current_pos -= len as u32;

        // Read the next record
        let r = self.page.read_bytes(self.current_pos as usize, len);

        Some(r.to_vec())
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

        let mut lm = LogManager::new(&root_dir);

        assert_eq!(lm.block_num, 0);

        for i in 0..1000 {
            println!("{}", i);
            let record = [(i % 255) as u8; 16];
            lm.append(&record);
        }

        println!("running!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        let snapshot = lm.snapshot();

        let mut i = 999;
        for r in snapshot {
            println!("{} ", i);
            assert_eq!(r, [(i % 255) as u8; 16].to_vec());
            i -= 1;
        }

        assert_eq!(i, -1);
    }
}
