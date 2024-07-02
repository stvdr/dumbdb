use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

use crate::{block_id::BlockId, transaction::Tx};

struct SlottedRecordPage {
    tx: Arc<Mutex<Tx>>,
    blk: BlockId,
}

impl SlottedRecordPage {
    pub fn new(tx: Arc<Mutex<Tx>>, blk: BlockId) -> Self {
        tx.lock().unwrap().pin(&blk);

        Self {
            tx: tx.clone(),
            blk,
        }
    }
}
