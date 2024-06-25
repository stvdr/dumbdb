use serde::{Deserialize, Serialize};

use crate::{block_id::BlockId, transaction::Transaction};

#[derive(Debug, Serialize, Deserialize)]
pub enum LogRecord {
    Checkpoint,
    Start {
        tx_num: i64,
    },
    Commit {
        tx_num: i64,
    },
    Rollback {
        tx_num: i64,
    },
    SetInt {
        tx_num: i64,
        block: BlockId,
        offset: u16,
        val: i32,
    },
    SetString {
        tx_num: i64,
        block: BlockId,
        offset: u16,
        val: String,
    },
}

impl LogRecord {
    pub fn undo(&self, tx: &mut Transaction) {
        match self {
            LogRecord::SetInt {
                tx_num,
                block,
                offset,
                val,
            } => {
                tx.pin(block);
                tx.set_int(block, *offset as usize, *val, false);
                tx.unpin(block);
            }
            LogRecord::SetString {
                tx_num,
                block,
                offset,
                val,
            } => {
                tx.pin(block);
                tx.set_string(block, *offset as usize, val, false);
                tx.unpin(block);
            }
            _ => return,
        }
    }
}
