use crate::{
    eviction_policy::{EvictionPolicy, SimpleEvictionPolicy},
    file_manager::{BlockId, FileManager, Page},
    log_manager::LogManager,
};

use std::collections::HashMap;

pub struct Buffer<'a> {
    file_manager: &'a FileManager,
    log_manager: &'a LogManager,
    page: Page,
    blk: Option<BlockId>,
    pin_count: u32,
    tx_num: i64,
    lsn: i64,
}

impl<'a> Buffer<'a> {
    fn new(file_manager: &'a FileManager, log_manager: &'a LogManager) -> Self {
        Self {
            file_manager,
            log_manager,
            page: Page::new(),
            blk: None,
            pin_count: 0,
            tx_num: -1,
            lsn: -1,
        }
    }

    fn set_modified(&mut self, tx_num: i64, lsn: i64) {
        self.tx_num = tx_num;
        self.lsn = lsn;
    }

    fn pin(&mut self) {
        self.pin_count += 1;
    }

    fn unpin(&mut self) {
        self.pin_count -= 1;
        assert!(self.pin_count >= 0);
    }

    fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    // TODO: error handling
    fn assign_to_block(&mut self, blk: BlockId) {
        self.flush();
        self.file_manager.get_block(&blk, &mut self.page).unwrap();
        self.blk = Some(blk);
        self.pin_count = 0;
    }

    // TODO: error handling
    fn flush(&mut self) {
        match &self.blk {
            Some(blk) => {
                self.log_manager.flush();
                self.file_manager.write_block(&blk, &self.page).unwrap();
                self.tx_num -= 1;
            }
            None => return,
        }
    }
}

struct BufferManager<'a, E: EvictionPolicy> {
    unused: Vec<usize>,
    blk_to_buf: HashMap<BlockId, usize>,
    buffers: Vec<Buffer<'a>>,
    eviction_policy: E,
}

impl<'a, E: EvictionPolicy> BufferManager<'a, E> {
    fn new(
        size: usize,
        file_manager: &'a FileManager,
        log_manager: &'a LogManager,
        eviction_policy: E,
    ) -> Self {
        Self {
            unused: (0..size).collect(),
            buffers: (0..size)
                .map(|_| Buffer::new(file_manager, log_manager))
                .collect(),
            eviction_policy,
        }
    }

    /// Find a buffer that is unused (i.e. a block has never been loaded into it)
    fn get_unused_buffer(&mut self) -> Option<usize> {
        self.unused.pop()
    }

    /// Find an already existing buffer that stores the specified block
    fn get_existing_buffer(&mut self, blk: &BlockId) -> Option<usize> {
        self.blk_to_buf.get(&blk);
    }

    /// Evict a block from a buffer to get a free buffer
    fn get_evicted_buffer(&mut self) -> Option<usize> {
        self.eviction_policy.evict()
    }

    pub fn pin(&mut self, blk: &BlockId) -> &Buffer {
        let (buf, mut buffer) = match self.get_existing_buffer(&blk) {
            Some(buf) => (buf, self.buffers[buf]),
            None => {
                let buf = self
                    .get_unused_buffer()
                    .or_else(|| self.get_evicted_buffer())
                    .expect("No available buffers");

                let mut buffer = self.buffers[buf];

                // If the buffer was already holding a block, make sure we are no longer mapping
                // the block to a buffer
                if let Some(block_id) = buffer.blk {
                    self.blk_to_buf.remove(&block_id);
                }

                buffer.assign_to_block(blk.clone());
                (buf, buffer)
            }
        };

        buffer.pin();

        // The page is no longer evictable, it has been pinned
        self.eviction_policy.remove(buf);

        &buffer
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {}
}
