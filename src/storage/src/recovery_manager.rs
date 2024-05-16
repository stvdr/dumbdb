use std::sync::{Arc, RwLock};

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

use crate::{
    buffer::Buffer, buffer_manager::BufferManager, file_manager::BlockId, log_manager::LogManager,
    transaction::Transaction,
};

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
    fn undo(&self, tx: &mut Transaction) {
        match self {
            LogRecord::SetInt {
                tx_num,
                block,
                offset,
                val,
            } => {}
            LogRecord::SetString {
                tx_num,
                block,
                offset,
                val,
            } => {}
            _ => return,
        }
    }
}

struct RecoveryManager {
    log_mgr: LogManager,
    buf_mgr: BufferManager,
    tx: Transaction,
    tx_num: i64,
}

impl RecoveryManager {
    pub fn new(tx: Transaction, tx_num: i64, log_mgr: LogManager, buf_mgr: BufferManager) -> Self {
        Self {
            tx,
            tx_num,
            log_mgr,
            buf_mgr,
        }
    }

    pub fn commit(&mut self) {
        self.buf_mgr.flush_all(self.tx_num);
        let log_record = LogRecord::Commit {
            tx_num: self.tx_num,
        };
        // TODO: error handling
        let encoded = bincode::serialize(&log_record).unwrap();
        self.log_mgr.append(&encoded);
    }

    /// Rollback the transaction associated with this RecoveryManager
    pub fn rollback(&mut self) {
        let snapshot = self.log_mgr.snapshot();
        for record in snapshot {
            // TODO: error handling
            let decoded = bincode::deserialize::<LogRecord>(&record)
                .expect("failed to deserialize log record");
            match decoded {
                LogRecord::Start { tx_num } => {
                    // We've reached the start of the transaction, nothing left to do
                    break;
                }
                _ => decoded.undo(&mut self.tx), //LogRecord::SetInt {
                                                 //    tx_num,
                                                 //    block,
                                                 //    offset,
                                                 //    val,
                                                 //} if tx_num == self.tx_num => {
                                                 //    // TODO: error handling
                                                 //    let buf = self.buf_mgr.pin(&block);
                                                 //    let mut buf = buf.write().unwrap();
                                                 //    buf.page.write(val, offset as usize);
                                                 //}
                                                 //LogRecord::SetString {
                                                 //    tx_num,
                                                 //    block,
                                                 //    offset,
                                                 //    val,
                                                 //} if tx_num == self.tx_num => {
                                                 //    // TODO: error handling
                                                 //    let buf = self.buf_mgr.pin(&block);
                                                 //    let mut buf = buf.write().unwrap();
                                                 //    buf.page.write(val.as_str(), offset as usize);
                                                 //}
                                                 //_ => break,
            }
        }

        self.buf_mgr.flush_all(self.tx_num);
        let log_record = LogRecord::Rollback {
            tx_num: self.tx_num,
        };
        // TODO: error handling
        let encoded = bincode::serialize(&log_record).unwrap();
        let lsn = self.log_mgr.append(&encoded);
        self.log_mgr.flush(lsn);
    }

    pub fn recover(&mut self) {
        // TODO: recovery

        self.buf_mgr.flush_all(self.tx_num);
        // TODO: error handling
        let encoded = bincode::serialize(&LogRecord::Checkpoint).unwrap();
        let lsn = self.log_mgr.append(&encoded);
        self.log_mgr.flush(lsn);
    }

    pub fn set_int(&mut self, buf: Arc<RwLock<Buffer>>, offset: usize, new_val: i32) -> i64 {
        let mut write_lock = buf.write().unwrap();
        let old_val: i32 = write_lock.page.read(offset);

        // TODO: error handling
        let log_record = LogRecord::SetInt {
            tx_num: self.tx_num,
            block: write_lock
                .blk
                .as_ref()
                .expect("buffer does not have a block loaded")
                .clone(),
            offset: offset as u16,
            val: old_val,
        };
        let encoded = bincode::serialize(&log_record).unwrap();
        let lsn = self.log_mgr.append(&encoded);

        write_lock.page.write(new_val, offset);
        lsn
    }

    pub fn set_string(&mut self, buf: Arc<RwLock<Buffer>>, offset: usize, new_val: &str) -> i64 {
        let mut write_lock = buf.write().unwrap();
        let old_val: i32 = write_lock.page.read(offset);

        // TODO: error handling
        let log_record = LogRecord::SetString {
            tx_num: self.tx_num,
            block: write_lock
                .blk
                .as_ref()
                .expect("buffer does not have a block loaded")
                .clone(),
            offset: offset as u16,
            // TODO: best way to handle the string stuff?
            val: old_val.to_string(),
        };
        let encoded = bincode::serialize(&log_record).unwrap();
        let lsn = self.log_mgr.append(&encoded);

        write_lock.page.write(new_val, offset);
        lsn
    }
}

#[cfg(test)]
mod tests {
    use crate::file_manager::BlockId;

    use super::LogRecord;

    #[test]
    fn test_serde() {
        let log_record = LogRecord::SetInt {
            tx_num: 42,
            block: BlockId::new("test", 1),
            offset: 10,
            val: 4242,
        };

        let encoded: Vec<u8> = bincode::serialize(&log_record).unwrap();
        let decoded: LogRecord = bincode::deserialize(&encoded).unwrap();

        //let blk = BlockId::new("test", 1);
        //
        if let LogRecord::SetInt {
            tx_num,
            block,
            offset,
            val,
        } = &decoded
        {
            assert_eq!(*tx_num, 42);
            assert_eq!(block, &BlockId::new("test", 1));
            assert_eq!(*offset, 10);
            assert_eq!(*val, 4242);
        } else {
            panic!("Pattern does not match LogRecord::SetInt");
        }
    }
}
