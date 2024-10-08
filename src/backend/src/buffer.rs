use std::sync::{Arc, Mutex};

use tracing::trace;

use crate::{block_id::BlockId, file_manager::FileManager, log_manager::LogManager, page::Page};

pub struct Buffer {
    file_manager: Arc<FileManager>,
    log_manager: Arc<Mutex<LogManager>>,
    pub page: Page,
    pub blk: Option<BlockId>,
    pub pin_count: u32,
    pub tx_num: i64,
    pub lsn: i64,
}

impl Buffer {
    pub fn new(file_manager: Arc<FileManager>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        Self {
            file_manager,
            log_manager,
            page: Page::new(),
            blk: None,
            pin_count: 0,
            // TODO: this will be changed back to -1 in the future
            tx_num: 0,
            lsn: -1,
        }
    }

    pub fn set_modified(&mut self, tx_num: i64, lsn: i64) {
        self.tx_num = tx_num;
        self.lsn = lsn;
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        self.pin_count -= 1;
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    // TODO: error handling
    /// Assign this Buffer to a new Block
    ///
    /// The buffer will be flushed and the pin count will reset to 0.
    ///
    /// # Arguments
    ///
    /// * `blk` - The BlockId describing the block to load into the buffer.
    pub fn assign_to_block(&mut self, blk: BlockId) {
        trace!("Assign to block called");
        self.flush();
        self.file_manager.get_block(&blk, &mut self.page).unwrap();
        self.blk = Some(blk);
        self.pin_count = 0;
    }

    // TODO: error handling
    pub fn flush(&mut self) {
        trace!("flush called");
        match &self.blk {
            Some(blk) => {
                trace!("Checking to see if block needs to be written to storage");
                if self.tx_num >= 0 {
                    trace!("Writing to storage");
                    {
                        let mut lm = self.log_manager.lock().unwrap();
                        lm.flush(self.lsn);
                    }

                    self.file_manager.write_block(&blk, &self.page).unwrap();
                    // TODO: this should be set in the future
                    //self.tx_num = -1;
                }
            }
            None => {
                trace!("No block to flush");
                return;
            }
        }
    }
}
