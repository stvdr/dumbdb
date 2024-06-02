use crate::file_manager::{BlockId, FileManager, Page};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

const LOG_NAME: &str = "log";

type LogPage<const P: usize> = Page<P>;
type Frontier = u32;
type RecordLength = u32;

// The position in the page where the current frontier is recorded
const FRONTIER_POS: usize = 0;

// The initial value of the frontier
const FRONTIER_START: usize = size_of::<Frontier>();

pub struct LogManager<const P: usize> {
    file_manager: Arc<FileManager<P>>,
    page: LogPage<P>,
    block_num: u64,
    latest_lsn: i64,
    last_saved_lsn: i64,
}

trait ImplLogPage<const P: usize> {
    fn get_frontier(&self) -> u32;
    fn set_frontier(&mut self, f: u32);
}

impl<const P: usize> ImplLogPage<P> for LogPage<P> {
    fn get_frontier(&self) -> u32 {
        self.read::<u32>(FRONTIER_POS)
    }

    fn set_frontier(&mut self, f: u32) {
        self.write(f, FRONTIER_POS);
    }
}

// TODO: error handling
impl<const P: usize> LogManager<P> {
    pub fn new(root_directory: &Path) -> Self {
        let file_manager = Arc::new(FileManager::new(root_directory));

        let num_blocks = file_manager.length(&LOG_NAME).unwrap();

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
            block_num,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    fn append_block(&mut self) {
        self.page = Page::new();
        self.page.write(FRONTIER_START as u32, FRONTIER_POS);
        self.block_num = self
            .file_manager
            .append_block(&LOG_NAME, &self.page)
            .unwrap()
            .num();
    }

    /// Append a record to the log and return the latest lsn.
    ///
    /// # Arguments
    ///
    /// * `record` - Bytes that will be written to the log
    pub fn append(&mut self, record: &[u8]) -> i64 {
        // If the record will fit in the existing page, place it there and update the frontier
        // Otherwise, create a new block
        let len = record.len() as u64;
        assert!(len < P as u64, "record does not fit in a single page!");

        let mut frontier = self.page.get_frontier();

        if frontier as u64 + len + size_of::<RecordLength>() as u64 >= P as u64 {
            // the record won't fit in the existing page, append a new block
            self.flush_all();
            self.append_block();

            // refresh the frontier, as it will now point to the start of the newly created block
            frontier = self.page.get_frontier();
        }

        frontier += self.page.write_bytes(record, frontier as usize) as u32;
        frontier += self.page.write(len as RecordLength, frontier as usize) as u32;
        self.page.set_frontier(frontier as RecordLength);
        self.latest_lsn += 1;
        self.latest_lsn
    }

    /// Flushes all log records to durable storage.
    pub fn flush(&mut self, lsn: i64) {
        if self.latest_lsn >= lsn {
            return;
        }

        self.flush_all();
    }

    fn flush_all(&mut self) {
        self.file_manager
            .write_block(&BlockId::new(&LOG_NAME, self.block_num), &self.page)
            .unwrap();

        self.last_saved_lsn = self.latest_lsn;
    }

    /// Gets a snapshot of the log that can be iterated over.
    ///
    /// Creating a snapshot will cause the log to be flushed.
    pub fn snapshot(&mut self) -> LogManagerSnapshot<P> {
        self.flush_all();

        // TODO: block_num should prob not be a usize?
        let block = BlockId::new(LOG_NAME, self.block_num);
        let mut page = Page::new();
        self.file_manager.get_block(&block, &mut page).unwrap();

        LogManagerSnapshot {
            file_manager: Arc::clone(&self.file_manager),
            block,
            page,
            current_pos: self.page.get_frontier(),
        }
    }
}

#[derive(Debug)]
pub struct LogManagerSnapshot<const P: usize> {
    file_manager: Arc<FileManager<P>>,
    block: BlockId,
    page: LogPage<P>,
    current_pos: u32,
}

// TODO: error handling
impl<const P: usize> Iterator for LogManagerSnapshot<P> {
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

        self.current_pos -= size_of::<RecordLength>() as u32;
        let len = self.page.read::<RecordLength>(self.current_pos as usize) as usize;
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
        {
            let mut lm = LogManager::<4096>::new(&root_dir);

            assert_eq!(lm.block_num, 0);

            for i in 0..1000 {
                let record = [(i % 256) as u8; 16];
                lm.append(&record);
            }

            let snapshot = lm.snapshot();

            let mut i = 999;
            for r in snapshot {
                assert_eq!(r, [(i % 256) as u8; 16].to_vec());
                i -= 1;
            }

            assert_eq!(i, -1);

            lm.flush(1000);
        }

        let mut lm = LogManager::<4096>::new(&root_dir);
        let snapshot = lm.snapshot();
        let mut i = 999;
        for r in snapshot {
            assert_eq!(r, [(i % 256) as u8; 16].to_vec());
            i -= 1;
        }

        assert_eq!(i, -1);
    }

    #[test]
    fn test_multi_snapshot() {
        let td = tempdir().unwrap();
        let root_dir = td.path().join("data");
        fs::create_dir_all(&root_dir).expect("Failed to create root directory");
        let mut lm = LogManager::<4096>::new(&root_dir);

        assert_eq!(lm.block_num, 0);

        for i in 0..1000 {
            let record = [(i % 256) as u8; 16];
            lm.append(&record);
        }

        let snapshot1 = lm.snapshot();
        let snapshot2 = lm.snapshot();

        let mut i = 999;
        for r in snapshot1 {
            assert_eq!(r, [(i % 256) as u8; 16].to_vec());
            i -= 1;
        }
        assert_eq!(i, -1);

        for i in 1000..1500 {
            let record = [(i % 256) as u8; 16];
            lm.append(&record);
        }

        // Take a new snapshot after adding an additional 500 records
        let snapshot3 = lm.snapshot();
        let mut i = 1499;
        for r in snapshot3 {
            assert_eq!(r, [(i % 256) as u8; 16].to_vec());
            i -= 1;
        }
        assert_eq!(i, -1);

        // Iterate through one of the snapshots created before adding more records
        i = 999;
        for r in snapshot2 {
            assert_eq!(r, [(i % 256) as u8; 16].to_vec());
            i -= 1;
        }
        assert_eq!(i, -1);
    }
}
