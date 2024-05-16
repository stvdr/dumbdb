use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    buffer::Buffer, buffer_manager::BufferManager, eviction_policy::EvictionPolicy,
    file_manager::BlockId,
};

struct BufferList {
    buffers: HashMap<BlockId, Arc<RwLock<Buffer>>>,
    pins: Vec<BlockId>,
    buf_mgr: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    pub fn new(buf_mgr: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buf_mgr,
        }
    }

    pub fn get_buffer(&self, blk: &BlockId) -> Arc<RwLock<Buffer>> {
        // TODO: error handling, can ultimately use `cloned` here?
        let buf = self.buffers.get(blk).unwrap();
        buf.clone()
    }

    pub fn pin(&mut self, blk: &BlockId) {
        //let buf = self.buf_mgr.
        let buf: Arc<RwLock<Buffer>>;
        {
            // TODO: error handling
            let mut buf_mgr = self.buf_mgr.lock().unwrap();
            buf = buf_mgr.pin(blk);
        }
        self.buffers.insert(blk.clone(), buf);
        self.pins.push(blk.clone());
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        // TODO: error handling
        let buf = self.buffers.get(blk).unwrap();
        {
            let mut buf_mgr = self.buf_mgr.lock().unwrap();
            buf_mgr.unpin(&buf);
        }

        if let Some(pos) = self.pins.iter().position(|b| b == blk) {
            self.pins.swap_remove(pos);
        }

        if None == self.pins.iter().find(|b| *b == blk) {
            self.buffers.remove(blk);
        }
    }

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
