use std::sync::{Arc, Mutex};

use crate::{
    file_manager::{BlockId, FileManager, Page},
    log_manager::LogManager,
};

pub struct Buffer<const P: usize> {
    file_manager: Arc<FileManager<P>>,
    log_manager: Arc<Mutex<LogManager<P>>>,
    pub page: Page<P>,
    pub blk: Option<BlockId>,
    pub pin_count: u32,
    pub tx_num: i64,
    pub lsn: i64,
}

impl<const P: usize> Buffer<P> {
    pub fn new(file_manager: Arc<FileManager<P>>, log_manager: Arc<Mutex<LogManager<P>>>) -> Self {
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
        log::trace!("Assign to block called");
        self.flush();
        self.file_manager.get_block(&blk, &mut self.page).unwrap();
        self.blk = Some(blk);
        self.pin_count = 0;
    }

    // TODO: error handling
    pub fn flush(&mut self) {
        log::trace!("flush called");
        match &self.blk {
            Some(blk) => {
                log::trace!("Checking to see if block needs to be written to storage");
                if self.tx_num >= 0 {
                    log::trace!("Writing to storage");
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
                log::trace!("No block to flush");
                return;
            }
        }
    }
}
