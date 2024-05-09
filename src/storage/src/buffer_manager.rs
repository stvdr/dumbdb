use crate::{
    buffer::Buffer,
    eviction_policy::EvictionPolicy,
    file_manager::{BlockId, FileManager, Page},
    log_manager::LogManager,
};
use std::sync::{Arc, LockResult, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::collections::HashMap;

struct BufferManager<E: EvictionPolicy> {
    unused: Vec<usize>,
    blk_to_buf: HashMap<BlockId, usize>,
    buffers: Vec<Arc<RwLock<Buffer>>>,
    num_available: usize,
    eviction_policy: E,
}

unsafe impl<E: EvictionPolicy> Sync for BufferManager<E> {}
unsafe impl<E: EvictionPolicy> Send for BufferManager<E> {}

impl<E: EvictionPolicy> BufferManager<E> {
    /// Creates a new BufferManager.
    ///
    /// # Arguments
    ///
    /// * `size` - The number of buffers that will be stored in the manager's pool.
    /// * `file_manager` - A FileManager that can be used to manage the buffer's underlying data
    /// pages.
    /// * `log_manager` - A LogManager that will be used for logging changes to data.
    /// * `eviction_policy` - The policy to be used for identifying and evicting unpinned buffers
    /// from the pool.
    pub fn new(
        size: usize,
        file_manager: Arc<FileManager>,
        log_manager: Arc<Mutex<LogManager>>,
        eviction_policy: E,
    ) -> Self {
        Self {
            unused: (0..size).collect(),
            buffers: (0..size)
                .map(|_| {
                    Arc::new(RwLock::new(Buffer::new(
                        file_manager.clone(),
                        log_manager.clone(),
                    )))
                })
                .collect(),
            blk_to_buf: HashMap::new(),
            num_available: size,
            eviction_policy,
        }
    }

    pub fn num_available(&self) -> usize {
        // TODO: error checking?
        self.num_available
    }

    /// Evict a block from a buffer to get a free buffer
    fn get_evicted_buffer(&mut self) -> Option<usize> {
        log::trace!("Evicting block from buffer");
        self.eviction_policy.evict()
    }

    // TODO: Error Checking
    pub fn pin(&mut self, blk: &BlockId) -> Arc<RwLock<Buffer>> {
        let buf_index = match self.blk_to_buf.get(&blk) {
            Some(buf_index) => {
                log::trace!(
                    "Pinning buffer already holding block {} at index {}",
                    blk,
                    *buf_index
                );

                // The block is loaded into an existing buffer
                let arc = Arc::clone(&self.buffers[*buf_index]);
                let mut buf = arc.write().unwrap();
                if !buf.is_pinned() {
                    // If the buffer was not pinned, the number of available buffers has been
                    // decremented
                    self.num_available -= 1;
                    log::trace!(
                        "Buffer {} is not already pinned - decremented available buffers to {}",
                        *buf_index,
                        self.num_available
                    );
                }
                buf.pin();
                log::trace!(
                    "Increased buffer {} pin count to {}",
                    *buf_index,
                    buf.pin_count()
                );
                *buf_index
            }
            None => {
                log::trace!("Block needs to be pulled into buffer, looking for free buffer");

                // The block needs to be pulled into a buffer, look for a free buffer
                let buf_index = self
                    .unused
                    .pop()
                    .or_else(|| self.get_evicted_buffer())
                    // TODO: condition variable to notify waiting threads on buffer availability
                    .expect("No available buffers");

                log::trace!("Found available buffer at index {}", buf_index);

                // Take a write lock on the unused buffer so the block can be loaded
                let arc = Arc::clone(&self.buffers[buf_index]);
                let mut unused_buf = arc.write().expect("Unable to write lock unused buffer");

                // If the buffer was already holding a block, make sure we are no longer mapping
                // the block to a buffer
                if let Some(block_id) = &unused_buf.blk {
                    log::trace!(
                        "Removing block {} from buffer at index {}",
                        block_id,
                        buf_index
                    );
                    self.blk_to_buf.remove(&block_id);
                }

                self.num_available -= 1;

                self.blk_to_buf.insert(blk.clone(), buf_index);
                unused_buf.assign_to_block(blk.clone());
                assert_eq!(unused_buf.pin_count(), 0);
                unused_buf.pin();
                assert_eq!(unused_buf.pin_count(), 1);

                log::trace!("Added block {} to buffer at index {}", blk, buf_index);
                buf_index
            }
        };

        // The page is no longer evictable, it has been pinned
        self.eviction_policy.remove(buf_index);

        Arc::clone(&self.buffers[buf_index])
    }

    /// Unpin a buffer. The provided buffer will have a write lock taken for the duration of this
    /// method call.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to unpin.
    pub fn unpin(&mut self, buffer: Arc<RwLock<Buffer>>) {
        let mut buffer = buffer.write().unwrap();
        self.unpin_locked(&mut buffer);
    }

    // TODO: error checking
    /// Unpin a buffer. This method can be used when a write lock has already been taken on a buffer.
    ///
    /// # Arguments
    ///
    /// * `buffer` - A mutable reference to a Buffer.
    pub fn unpin_locked(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        if !buffer.is_pinned() {
            let b = buffer.blk.as_ref().unwrap();
            if let Some(buf_index) = self.blk_to_buf.get(b) {
                log::trace!("Marking buffer {} as available for eviction", buf_index);
                self.eviction_policy.add(*buf_index);
            }
            self.num_available += 1;
            log::trace!("Incremented available buffers to {}", self.num_available());
        }
    }

    // TODO: error checking
    pub fn flush_all(&mut self, tx_num: i64) {
        for buf in self.buffers.iter() {
            let arc = buf.clone();
            let mut b = arc.write().unwrap();
            if b.tx_num == tx_num {
                b.flush();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
        thread,
    };

    use tempfile::tempdir;

    use crate::{
        eviction_policy::SimpleEvictionPolicy,
        file_manager::{BlockId, FileManager, Page, PAGE_SIZE},
        log_manager::LogManager,
    };

    use super::BufferManager;

    #[test]
    fn test() {
        let td = tempdir().unwrap();
        let data_dir = td.path().join("data");
        fs::create_dir_all(&data_dir).expect("Failed to create root directory");
        let log_dir = td.path().join("log");
        fs::create_dir_all(&log_dir).expect("Failed to create root directory");

        let lm = LogManager::new(&log_dir);
        let fm = FileManager::new(&data_dir);
        let mut bm = BufferManager::new(
            3,
            Arc::new(fm),
            Arc::new(Mutex::new(lm)),
            SimpleEvictionPolicy::new(),
        );

        assert_eq!(bm.num_available(), 3);

        let buf1 = bm.pin(&BlockId::new("test", 0));
        assert_eq!(bm.num_available(), 2);

        let buf2 = bm.pin(&BlockId::new("test", 1));
        assert_eq!(bm.num_available(), 1);

        let buf3 = bm.pin(&BlockId::new("test", 2));
        assert_eq!(bm.num_available(), 0);

        let buf3_2 = bm.pin(&BlockId::new("test", 2));
        assert_eq!(bm.num_available(), 0);

        bm.unpin(buf1);
        assert_eq!(bm.num_available(), 1);

        bm.unpin(buf2);
        assert_eq!(bm.num_available(), 2);

        bm.unpin(buf3);
        assert_eq!(bm.num_available(), 2);

        bm.unpin(buf3_2);
        assert_eq!(bm.num_available(), 3);
    }

    #[test]
    fn test_parallel_pins() {
        let td = tempdir().unwrap();
        let data_dir = td.path().join("data");
        fs::create_dir_all(&data_dir).expect("Failed to create root directory");
        let log_dir = td.path().join("log");
        fs::create_dir_all(&log_dir).expect("Failed to create root directory");

        let lm = LogManager::new(&log_dir);
        let fm = Arc::new(FileManager::new(&data_dir));
        let bm = Arc::new(Mutex::new(BufferManager::new(
            1,
            fm.clone(),
            Arc::new(Mutex::new(lm)),
            SimpleEvictionPolicy::new(),
        )));

        let num_threads = 3u64;
        let num_pages_per_thread = 10u64;

        // TODO (2024-05-08): not really clear to me the interaction between appending blocks and reading
        // blocks at this point
        // Append all blocks that will be read below
        for _ in 0..num_threads * num_pages_per_thread {
            let _ = fm.append_block("test", &Page::new());
        }

        let mut handles = vec![];
        for t in 0..num_threads {
            let bm = bm.clone();
            handles.push(thread::spawn(move || {
                for i in 0..num_pages_per_thread {
                    let mut lock = bm.lock().unwrap();
                    let buf = lock.pin(&BlockId::new("test", (t * num_pages_per_thread) + i));
                    {
                        let mut wb = buf.write().unwrap();
                        wb.page.write((t * num_pages_per_thread) + i, 0);
                        lock.unpin_locked(&mut wb);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut bm_lock = bm.lock().unwrap();
        for p in 0..num_threads * num_pages_per_thread {
            let buf = bm_lock.pin(&BlockId::new("test", p));
            {
                let mut wb = buf.write().unwrap();
                let val: u64 = wb.page.read(0);

                assert_eq!(val, p);
                bm_lock.unpin_locked(&mut wb);
            }
        }
    }
}
