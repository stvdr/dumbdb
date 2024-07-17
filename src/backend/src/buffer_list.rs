use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use tracing::trace;

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
        trace!("pinning block {}", blk);
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
        trace!("unpinning buffer holding block {}", blk);
        // TODO: error handling
        let buf = self
            .buffers
            .get(blk)
            .expect(&format!("block does not exist in the BufferList: {}", blk));
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
        trace!("unpinning all buffers");
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

    #[cfg(test)]
    pub fn pin_count(&self, blk: &BlockId) -> usize {
        self.pins.iter().filter(|b| *b == blk).count()
    }
}

#[cfg(test)]
mod tests {
    use crate::block_id::BlockId;
    use crate::buffer_list::BufferList;
    use crate::tests::test_utils::{create_default_tables, test_db};
    use tempfile::tempdir;

    #[test]
    fn test_buffer_list_pin_unpin() {
        let td = tempdir().unwrap();
        let mut db = test_db(&td);
        create_default_tables(&mut db);
        let mut bl = BufferList::new(db.buffer_manager());

        let blk0 = BlockId::new("student", 0);
        let blk1 = BlockId::new("student", 1);
        let blk2 = BlockId::new("student", 2);

        bl.pin(&blk0);
        bl.pin(&blk1);
        bl.pin(&blk0);
        bl.pin(&blk2);
        bl.pin(&blk0);

        assert_eq!(3, bl.pin_count(&blk0));

        bl.unpin(&BlockId::new("student", 0));
        assert_eq!(2, bl.pin_count(&blk0));

        bl.unpin(&BlockId::new("student", 0));
        assert_eq!(1, bl.pin_count(&blk0));

        bl.unpin(&BlockId::new("student", 0));
        assert_eq!(0, bl.pin_count(&blk0));

        // assert other blocks 1 & 2 remain pinned
        assert_eq!(1, bl.pin_count(&blk1));
        assert_eq!(1, bl.pin_count(&blk2));

        // assert block 0 is pinned normally after unpinning
        bl.pin(&blk0);
        assert_eq!(1, bl.pin_count(&blk0));
    }

    #[test]
    fn test_buffer_list_unpin_all() {
        let td = tempdir().unwrap();
        let mut db = test_db(&td);
        create_default_tables(&mut db);
        let mut bl = BufferList::new(db.buffer_manager());

        let blk1 = BlockId::new("student", 0);
        let blk2 = BlockId::new("student", 1);
        bl.pin(&blk1);
        bl.pin(&blk1);
        bl.pin(&blk2);
        bl.pin(&blk2);
        assert_eq!(2, bl.pin_count(&blk1));
        assert_eq!(2, bl.pin_count(&blk2));
        bl.unpin_all();
        assert_eq!(0, bl.pin_count(&blk1));
        assert_eq!(0, bl.pin_count(&blk2));

        // assert blocks are pinned normally after unpinning
        bl.pin(&blk1);
        bl.pin(&blk2);
        assert_eq!(1, bl.pin_count(&blk1));
        assert_eq!(1, bl.pin_count(&blk2));
    }
}
