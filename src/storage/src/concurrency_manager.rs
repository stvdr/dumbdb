use std::{collections::HashMap, sync::Arc};

use crate::file_manager::BlockId;
use crate::lock_table::LockTable;

/// Manages locks for a single Transaction.
pub struct ConcurrencyManager {
    lock_tbl: Arc<LockTable>,
    locks: HashMap<BlockId, char>,
}

impl ConcurrencyManager {
    /// Create a new Concurrency Manager.
    ///
    /// # Arguments
    ///
    /// * `lock_tbl` - A LockTable that is shared by all ConcurrencyManagers.
    pub fn new(lock_tbl: Arc<LockTable>) -> Self {
        Self {
            lock_tbl,
            locks: HashMap::new(),
        }
    }

    /// Take a shared lock on a block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The block to lock.
    pub fn slock(&mut self, blk: &BlockId) {
        if self.locks.get(blk).is_none() {
            self.lock_tbl.slock(blk);
            self.locks.insert(blk.clone(), 'S');
        }
    }

    /// Take an exclusive lock on a block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The block to lock.
    pub fn xlock(&mut self, blk: &BlockId) {
        if !self.has_xlock(blk) {
            // TODO: I don't really understand why an slock needs to be taken before the xlock
            self.slock(blk);
            self.lock_tbl.xlock(blk);
            self.locks.insert(blk.clone(), 'X');
        }
    }

    /// Release all locks.
    pub fn release(&mut self) {
        for (blk, _) in self.locks.iter() {
            self.lock_tbl.unlock(blk);
        }
        self.locks.clear();
    }

    fn has_xlock(&self, blk: &BlockId) -> bool {
        self.locks.get(blk).is_some_and(|l| *l == 'X')
    }
}
