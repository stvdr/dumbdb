use std::sync::{
    atomic::{AtomicI64, AtomicU64},
    Arc, Mutex,
};

use crate::{
    block_id::BlockId, buffer::Buffer, buffer_list::BufferList, buffer_manager::BufferManager,
    concurrency_manager::ConcurrencyManager, eviction_policy::SimpleEvictionPolicy,
    file_manager::FileManager, lock_table::LockTable, log_manager::LogManager,
    log_record::LogRecord, page::Page,
};

static NEXT_TRANSACTION_NUM: AtomicI64 = AtomicI64::new(0);
static END_OF_FILE: u64 = std::u64::MAX;

// TODO: commit/rollback on drop

pub struct Tx {
    concurrency_mgr: ConcurrencyManager,
    buffer_mgr: Arc<Mutex<BufferManager<SimpleEvictionPolicy>>>,
    log_mgr: Arc<Mutex<LogManager>>,
    file_mgr: Arc<FileManager>,
    tx_num: i64,
    buffer_list: Arc<Mutex<BufferList>>,
}

impl Tx {
    pub fn new(
        file_mgr: Arc<FileManager>,
        log_mgr: Arc<Mutex<LogManager>>,
        buffer_mgr: Arc<Mutex<BufferManager<SimpleEvictionPolicy>>>,
        lock_tbl: Arc<LockTable>,
    ) -> Self {
        // TODO: verify the atomic ordering
        let tx_num = NEXT_TRANSACTION_NUM.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Self {
            file_mgr,
            log_mgr,
            tx_num,
            buffer_mgr: buffer_mgr.clone(),
            buffer_list: Arc::new(Mutex::new(BufferList::new(buffer_mgr))),
            concurrency_mgr: ConcurrencyManager::new(lock_tbl),
        }
    }

    pub fn tx_num(&self) -> i64 {
        self.tx_num
    }

    pub fn commit(&mut self) {
        self.buffer_mgr.lock().unwrap().flush_all(self.tx_num);
        let log_record = LogRecord::Commit {
            tx_num: self.tx_num,
        };

        // TODO: error handling
        let encoded = bincode::serialize(&log_record).unwrap();
        self.log_mgr.lock().unwrap().append(&encoded);

        self.concurrency_mgr.release();
        self.buffer_list.lock().unwrap().unpin_all();
        log::trace!("Transaction {} committed", self.tx_num);
    }

    /// Rollback the transaction associated with this RecoveryManager
    pub fn rollback(&mut self) {
        let snapshot = self.log_mgr.lock().unwrap().snapshot();
        for record in snapshot {
            // TODO: error handling
            let decoded = bincode::deserialize::<LogRecord>(&record)
                .expect("failed to deserialize log record");

            match decoded {
                LogRecord::Start { tx_num } if tx_num == self.tx_num => {
                    // Reached the start of the transaction, nothing left to do
                    break;
                }
                LogRecord::SetInt { tx_num, .. } | LogRecord::SetString { tx_num, .. }
                    if tx_num == self.tx_num =>
                {
                    decoded.undo(self)
                }
                _ => continue,
            }
        }

        self.buffer_mgr.lock().unwrap().flush_all(self.tx_num);
        let log_record = LogRecord::Rollback {
            tx_num: self.tx_num,
        };
        self.append_to_log_and_flush(&log_record);

        self.concurrency_mgr.release();
        self.buffer_list.lock().unwrap().unpin_all();
        log::trace!("Rolled back transaction with id {}", self.tx_num);
    }

    /// Pin the specified block
    pub fn pin(&mut self, blk: &BlockId) {
        // TODO: error handling
        self.buffer_list.lock().unwrap().pin(blk);
    }

    /// Unpin the specified block
    pub fn unpin(&mut self, blk: &BlockId) {
        // TODO: error handling
        self.buffer_list.lock().unwrap().unpin(blk);
    }

    pub fn block_size(&self) -> usize {
        self.file_mgr.page_size()
    }

    fn recover(&mut self) {
        self.buffer_mgr.lock().unwrap().flush_all(self.tx_num);

        let mut completed_txs: Vec<i64> = vec![];
        let log_snapshot = self.log_mgr.lock().unwrap().snapshot();
        for record in log_snapshot.map(|b| {
            bincode::deserialize::<LogRecord>(&b).expect("Failed to deserialize log record")
        }) {
            match record {
                LogRecord::Checkpoint => break,
                LogRecord::Commit { tx_num } | LogRecord::Rollback { tx_num } => {
                    completed_txs.push(tx_num)
                }
                LogRecord::SetInt { tx_num, .. } | LogRecord::SetString { tx_num, .. }
                    if !completed_txs.contains(&tx_num) =>
                {
                    record.undo(self)
                }
                _ => continue,
            }
        }
        // TODO: recovery

        self.buffer_mgr.lock().unwrap().flush_all(self.tx_num);
        self.append_to_log_and_flush(&LogRecord::Checkpoint);
    }

    fn append_to_log_and_flush(&mut self, record: &LogRecord) {
        // TODO: error handling
        let encoded = bincode::serialize(record).unwrap();
        let mut log_mgr_locked = self.log_mgr.lock().unwrap();
        let lsn = log_mgr_locked.append(&encoded);
        log_mgr_locked.flush(lsn);
    }

    /// Sets an integer in a block. The block will be locked exclusively for the remaining duration
    /// of the Transaction.
    ///
    /// # Arguments
    ///
    /// * `blk` - The block where the integer will be written.
    /// * `offset` - The offset in the block's page to write the integer.
    /// * `val` - The integer value to write.
    /// * `ok_to_log` - A boolean indicating whether the change should be logged.
    pub fn set_int(&mut self, blk: &BlockId, offset: usize, val: i32, ok_to_log: bool) {
        self.concurrency_mgr.xlock(blk);
        let buf = self.buffer_list.lock().unwrap().get_buffer(blk);
        let mut buf = buf.write().unwrap();

        let lsn = if ok_to_log {
            self.log_set_int(&mut buf, offset, val)
        } else {
            -1
        };

        buf.page.write(val, offset);
        buf.set_modified(self.tx_num, lsn);
    }

    /// Sets a string in a block. The block will be locked exclusively for the remaining duration
    /// of the Transaction.
    ///
    /// # Arguments
    ///
    /// * `blk` - The block where the integer will be written.
    /// * `offset` - The offset in the block's page to write the integer.
    /// * `val` - The &str value to write.
    /// * `ok_to_log` - A boolean indicating whether the change should be logged.
    pub fn set_string(&mut self, blk: &BlockId, offset: usize, val: &str, ok_to_log: bool) {
        self.concurrency_mgr.xlock(blk);

        let buf = self.buffer_list.lock().unwrap().get_buffer(blk);
        let mut buf = buf.write().unwrap();

        let lsn = if ok_to_log {
            self.log_set_string(&mut buf, offset, val)
        } else {
            -1
        };

        buf.page.write(val, offset);
        buf.set_modified(self.tx_num, lsn);
    }

    /// Get the number of blocks in a file. A shared lock will be acquired on the file.
    pub fn size(&mut self, file_id: &str) -> u64 {
        // Take a shared lock on the dummy block
        self.concurrency_mgr
            .slock(&BlockId::new(file_id, END_OF_FILE));

        // TODO: error handling
        self.file_mgr.length(file_id).unwrap()
    }

    /// Append a new block to a file.
    ///
    /// # Arguments
    ///
    /// * `file_id` - The id of the file that will have a block appended.
    pub fn append(&mut self, file_id: &str) -> BlockId {
        log::trace!("xlocking the dummy block for file '{}'", file_id);

        // Take an exclusive lock on the dummy block
        self.concurrency_mgr
            .xlock(&BlockId::new(file_id, END_OF_FILE));

        // TODO: error handling
        self.file_mgr.append_block(file_id, &Page::new()).unwrap()
    }

    /// Get an integer from the specified block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The `BlockId` where the integer will be read from.
    /// * `offset` - The offset in the block that the integer will be read from.
    pub fn get_int(&mut self, blk: &BlockId, offset: usize) -> i32 {
        self.concurrency_mgr.slock(blk);
        let buff = self.buffer_list.lock().unwrap().get_buffer(blk);
        let val = buff.read().unwrap().page.read(offset);
        val
    }

    /// Get a string from the specified block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The `BlockId` where the string will be read from.
    /// * `offset` - The offset in the block that the string will be read from.
    pub fn get_string(&mut self, blk: &BlockId, offset: usize) -> String {
        self.concurrency_mgr.slock(blk);
        let buff = self.buffer_list.lock().unwrap().get_buffer(blk);
        let val = buff.read().unwrap().page.read(offset);
        val
    }

    /// Logs (for recovery) the setting of an integer value in a buffer.
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer where the integer is being written.
    /// * `offset` - The offset in the buffer's page.
    /// * `new_val` - The new value to be written.
    // TODO: why is `new_val` provided to this method?
    fn log_set_int(&mut self, buf: &mut Buffer, offset: usize, new_val: i32) -> i64 {
        let old_val: i32 = buf.page.read(offset);

        // TODO: error handling
        let log_record = LogRecord::SetInt {
            tx_num: self.tx_num,
            block: buf
                .blk
                .as_ref()
                .expect("buffer does not have a block loaded")
                .clone(),
            offset: offset as u16,
            val: old_val,
        };
        let encoded = bincode::serialize(&log_record).unwrap();
        self.log_mgr.lock().unwrap().append(&encoded)
    }

    /// Logs (for recovery) the setting of a string in a buffer.
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer where the string is being written.
    /// * `offset` - The offset in the buffer's page.
    /// * `new_val` - The new value being written.
    // TODO: why is `new_val` provided to this method?
    fn log_set_string(&mut self, buf: &mut Buffer, offset: usize, new_val: &str) -> i64 {
        let old_val: String = buf.page.read(offset);

        // TODO: error handling
        let log_record = LogRecord::SetString {
            tx_num: self.tx_num,
            block: buf
                .blk
                .as_ref()
                .expect("buffer does not have a block loaded")
                .clone(),
            offset: offset as u16,
            // TODO: best way to handle the string stuff?
            val: old_val,
        };
        let encoded = bincode::serialize(&log_record).unwrap();
        self.log_mgr.lock().unwrap().append(&encoded)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::mpsc, thread};

    use tempfile::tempdir;

    use crate::{buffer_manager, eviction_policy::SimpleEvictionPolicy, page::Page};

    use super::*;

    #[test]
    fn test_serial_transactions() {
        let td = tempdir().unwrap();
        let data_dir = td.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let log_dir = td.path().join("log");
        fs::create_dir_all(&log_dir).unwrap();

        let lm = Arc::new(Mutex::new(LogManager::new(&log_dir)));
        let fm = Arc::new(FileManager::new(&data_dir));
        let bm = Arc::new(Mutex::new(BufferManager::new(
            10,
            fm.clone(),
            lm.clone(),
            SimpleEvictionPolicy::new(),
        )));
        let locks = Arc::new(LockTable::new());

        let blk = fm.append_block("test", &Page::new()).unwrap();

        // Verify that committed sets are read from a separate transaction
        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk);
        tx.set_int(&blk, 0, 10, true);
        tx.set_string(&blk, 100, "test string", true);
        assert_eq!(tx.get_int(&blk, 0), 10);
        assert_eq!(tx.get_string(&blk, 100), "test string");
        tx.commit();
        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk);
        let int_val: i32 = tx.get_int(&blk, 0);
        let str_val: String = tx.get_string(&blk, 100);
        assert_eq!(int_val, 10);
        assert_eq!(str_val, "test string");
        tx.commit();

        // Verify that sets are read in the same transaction
        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk);
        tx.set_int(&blk, 0, 20, true);
        tx.set_string(&blk, 100, "another test string", true);
        let int_val: i32 = tx.get_int(&blk, 0);
        let str_val: String = tx.get_string(&blk, 100);
        assert_eq!(int_val, 20);
        assert_eq!(str_val, "another test string");
        tx.rollback();

        // Verify that the above data is not read after rollback
        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk);
        let int_val: i32 = tx.get_int(&blk, 0);
        let str_val: String = tx.get_string(&blk, 100);
        assert_eq!(int_val, 10);
        assert_eq!(str_val, "test string");
        tx.commit();

        // Verify that multiple integers and string get rolled back
        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk);
        tx.set_int(&blk, 20, 3, true);
        tx.set_int(&blk, 40, 6, true);
        tx.set_int(&blk, 60, 9, true);
        tx.set_string(&blk, 200, "test1", true);
        tx.set_string(&blk, 300, "test2", true);
        tx.set_string(&blk, 400, "test3", true);
        assert_eq!(tx.get_int(&blk, 20), 3);
        assert_eq!(tx.get_int(&blk, 40), 6);
        assert_eq!(tx.get_int(&blk, 60), 9);
        assert_eq!(tx.get_string(&blk, 200), "test1");
        assert_eq!(tx.get_string(&blk, 300), "test2");
        assert_eq!(tx.get_string(&blk, 400), "test3");
        tx.rollback();

        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk);
        assert_eq!(tx.get_int(&blk, 20), 0);
        assert_eq!(tx.get_int(&blk, 40), 0);
        assert_eq!(tx.get_int(&blk, 60), 0);
        assert_eq!(tx.get_string(&blk, 200), "");
        assert_eq!(tx.get_string(&blk, 300), "");
        assert_eq!(tx.get_string(&blk, 400), "");
        tx.rollback();
    }

    #[test]
    fn test_parallel_transactions() {
        let td = tempdir().unwrap();
        let data_dir = td.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let log_dir = td.path().join("log");
        fs::create_dir_all(&log_dir).unwrap();

        let lm = Arc::new(Mutex::new(LogManager::new(&log_dir)));
        let fm = Arc::new(FileManager::new(&data_dir));
        let bm = Arc::new(Mutex::new(BufferManager::new(
            10,
            fm.clone(),
            lm.clone(),
            SimpleEvictionPolicy::new(),
        )));
        let locks = Arc::new(LockTable::new());

        let blk1 = fm.append_block("test", &Page::new()).unwrap();
        let blk2 = fm.append_block("test", &Page::new()).unwrap();

        let mut tx_a = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        let mut tx_b = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        let mut tx_c = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());

        let (send_a, recv_a) = mpsc::channel::<bool>();
        let (send_c, recv_c) = mpsc::channel::<bool>();

        let handle_1 = thread::spawn({
            let blk1 = blk1.clone();
            let blk2 = blk2.clone();

            move || {
                tx_a.pin(&blk1);
                tx_a.pin(&blk2);

                log::trace!("[A] wait slock on blk1");
                let val = tx_a.get_int(&blk1, 0);
                log::trace!("[A] receive slock on blk1");
                assert_eq!(val, 0);

                log::trace!("[A] signal to C that slock on blk1 was acquired");
                send_c.send(true).unwrap();

                // Wait for B to signal that it has passed the point of taking an xlock on blk2
                log::trace!("[A] wait for B to retrieve xlock on blk2");
                recv_a.recv().unwrap();

                // The slock required here should not be granted until B commits and releases its
                // xlock on blk2
                log::trace!("[A] wait slock on blk2");
                let val = tx_a.get_int(&blk2, 0);
                log::trace!("[A] receive slock on blk2");
                assert_eq!(val, 2);

                tx_a.commit();
            }
        });

        let handle_2 = thread::spawn({
            let blk1 = blk1.clone();
            let blk2 = blk2.clone();

            move || {
                tx_b.pin(&blk1);
                tx_b.pin(&blk2);

                log::trace!("[B] wait xlock on blk2");
                tx_b.set_int(&blk2, 0, 2, false);
                log::trace!("[B] receive xlock on blk2");

                log::trace!("[B] signal A that xlock as been acquired on blk2");
                send_a.send(true).unwrap();

                log::trace!("[B] wait slock on blk1");
                let val = tx_b.get_int(&blk1, 0);
                log::trace!("[B] receive slock on blk1");
                assert_eq!(val, 0);

                // The commit will release a shared lock on blk1 and the xlock on blk2
                tx_b.commit();
            }
        });

        let handle_3 = thread::spawn({
            let blk1 = blk1.clone();
            let blk2 = blk2.clone();

            move || {
                tx_c.pin(&blk1);
                tx_c.pin(&blk2);

                // Wait for A to signal that it has already taken a shared lock on blk1
                log::trace!("[C] wait for A to acquire shared lock on blk1");
                recv_c.recv().unwrap();

                // This should block until A commits and releases its slock on blk1
                log::trace!("[C] wait xlock on blk1");
                tx_c.set_int(&blk1, 0, 3, false);
                log::trace!("[C] receive xlock on blk1");

                log::trace!("[C] wait slock on blk2");
                let val = tx_c.get_int(&blk2, 0);
                log::trace!("[C] receive slock on blk2");
                assert_eq!(val, 2);
                tx_c.commit();
            }
        });

        handle_1.join().unwrap();
        handle_2.join().unwrap();
        handle_3.join().unwrap();

        let mut tx = Tx::new(fm.clone(), lm.clone(), bm.clone(), locks.clone());
        tx.pin(&blk1);
        tx.pin(&blk2);

        assert_eq!(tx.get_int(&blk1, 0), 3);
        assert_eq!(tx.get_int(&blk2, 0), 2);
    }
}
