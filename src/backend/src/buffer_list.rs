use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    block_id::BlockId, buffer::Buffer, buffer_manager::BufferManager,
    eviction_policy::SimpleEvictionPolicy,
};

/// A mapping of Blocks to the buffers that are loaded into them.
pub struct BufferList {
    buffers: HashMap<BlockId, Arc<RwLock<Buffer>>>,
    pins: Vec<BlockId>,
    buf_mgr: Arc<Mutex<BufferManager<SimpleEvictionPolicy>>>,
}

impl BufferList {
    pub fn new(buf_mgr: Arc<Mutex<BufferManager<SimpleEvictionPolicy>>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buf_mgr,
        }
    }

    /// Get the buffer associated with the specified BlockId.
    pub fn get_buffer(&self, blk: &BlockId) -> Arc<RwLock<Buffer>> {
        // TODO: error handling, can ultimately use `cloned` here?
        let buf = self
            .buffers
            .get(blk)
            .expect(&format!("Attempt to operate on unpinned block: {}", blk));
        buf.clone()
    }

    /// Pin the specified block.
    pub fn pin(&mut self, blk: &BlockId) {
        let buf = {
            // TODO: error handling
            let mut buf_mgr = self.buf_mgr.lock().unwrap();
            buf_mgr.pin(blk)
        };
        self.buffers.insert(blk.clone(), buf);
        self.pins.push(blk.clone());
    }

    /// Unpin the specified block.
    pub fn unpin(&mut self, blk: &BlockId) {
        // TODO: error handling
        let buf = self.buffers.get(blk).unwrap();
        {
            let mut buf_mgr = self.buf_mgr.lock().unwrap();
            buf_mgr.unpin(&buf);
        }

        // TODO: do this in constant time
        if let Some(pos) = self.pins.iter().position(|b| b == blk) {
            self.pins.swap_remove(pos);
        }

        if None == self.pins.iter().find(|b| *b == blk) {
            self.buffers.remove(blk);
        }
    }

    /// Unpin all blocks in this BufferList.
    pub fn unpin_all(&mut self) {
        for blk in self.pins.iter() {
            // TODO: error handling
            let buf = self.buffers.get(blk).unwrap();
            {
                let mut buf_mgr = self.buf_mgr.lock().unwrap();
                buf_mgr.unpin(&buf);
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
