use crate::{
    eviction_policy::{EvictionPolicy, SimpleEvictionPolicy},
    file_manager::{BlockId, FileManager, Page},
    log_manager::LogManager,
};
use std::sync::{Arc, LockResult, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

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

    fn pin_count(&self) -> u32 {
        self.pin_count
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

// struct BufferGuard<'a> {
//     buffer: Arc<RwLock<Buffer<'a>>>,
// }
//
// enum BufferGuard<'a> {
//     Read(RwLockReadGuard<'a, Buffer<'a>>),
//     Write(RwLockWriteGuard<'a, Buffer<'a>>),
// }
//
// impl<'a> Drop for BufferGuard<'a> {
//     fn drop(&mut self) {
//         match ()
//     }
// }

// impl<'a> BufferGuard<'a> {
//     // fn new(buffer: Arc<RwLock<Buffer<'a>>>) -> Self {
//     //     Self { buffer }
//     // }
//     //
//     // pub fn read(&self) -> LockResult<RwLockReadGuard<Buffer<'a>>> {
//     //     self.buffer.read()
//     // }
//     //
//     // pub fn write(&self) -> LockResult<RwLockWriteGuard<Buffer<'a>>> {
//     //     self.buffer.write()
//     // }
//     //
//     fn read(buffer: Arc<RwLock<Buffer<'a>>>) -> Self {
//         Self {
//             buffer: RwLockWriteGuard,
//         }
//     }
// }

// impl<'a> Drop for BufferGuard<'a> {
//     fn drop(&mut self) {
//         self.buffer.
//     }
// }

struct BufferManager<'a, E: EvictionPolicy> {
    unused: Vec<usize>,
    blk_to_buf: Arc<Mutex<HashMap<BlockId, usize>>>,
    buffers: Vec<Arc<RwLock<Buffer<'a>>>>,
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
                .map(|_| Arc::new(RwLock::new(Buffer::new(file_manager, log_manager))))
                .collect(),
            blk_to_buf: Arc::new(Mutex::new(HashMap::new())),
            eviction_policy,
        }
    }

    /// Evict a block from a buffer to get a free buffer
    fn get_evicted_buffer(&mut self) -> Option<usize> {
        self.eviction_policy.evict()
    }

    // TODO: Error Checking
    pub fn pin(&'a mut self, blk: &BlockId) -> Arc<RwLock<Buffer<'a>>> {
        let arc = Arc::clone(&self.blk_to_buf);
        let mut btb = arc.lock().unwrap();

        let buf_index = match btb.get(&blk) {
            Some(buf_index) => {
                let arc = Arc::clone(&self.buffers[*buf_index]);
                let mut buf = arc.write().unwrap();
                buf.pin();
                *buf_index
            }
            None => {
                let buf_index = self
                    .unused
                    .pop()
                    .or_else(|| self.get_evicted_buffer())
                    // TODO: condition variable to notify waiting threads on buffer availability
                    .expect("No available buffers");

                let arc = Arc::clone(&self.buffers[buf_index]);
                let mut buffer = arc.write().expect("Unable to write lock unused buffer");

                // If the buffer was already holding a block, make sure we are no longer mapping
                // the block to a buffer
                if let Some(block_id) = &buffer.blk {
                    btb.remove(&block_id);
                }

                buffer.assign_to_block(blk.clone());
                buffer.pin();
                buf_index
            }
        };

        // The page is no longer evictable, it has been pinned
        self.eviction_policy.remove(buf_index);

        Arc::clone(&self.buffers[buf_index])
    }

    // TODO: error checking
    pub fn unpin(&mut self, buffer: &mut Buffer) {
        let arc = Arc::clone(&self.blk_to_buf);
        let mut btb = arc.lock().unwrap();

        buffer.unpin();

        if !buffer.is_pinned() {
            let b = buffer.blk.as_ref().unwrap();

            if let Some((_blk, buf_index)) = btb.remove_entry(b) {
                self.eviction_policy.add(buf_index);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_pin() {}
}
