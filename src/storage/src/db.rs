use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    buffer_manager::BufferManager,
    eviction_policy::SimpleEvictionPolicy,
    file_manager::{self, FileManager},
    lock_table::LockTable,
    log_manager::LogManager,
    transaction::Transaction,
};

const DEFAULT_BLOCK_SIZE: usize = 4096;
const DEFAULT_BUFFER_SIZE: usize = 1024;

pub struct SimpleDB<const PAGE_SIZE: usize> {
    buffer_manager: Arc<Mutex<BufferManager<PAGE_SIZE, SimpleEvictionPolicy>>>,
    file_manager: Arc<FileManager<PAGE_SIZE>>,
    lock_table: Arc<LockTable>,
    log_manager: Arc<Mutex<LogManager<PAGE_SIZE>>>,
}

impl<const PAGE_SIZE: usize> SimpleDB<PAGE_SIZE> {
    pub fn new(data_dir: &Path, log_dir: &Path, num_bufs: usize) -> Self {
        let file_manager = Arc::new(FileManager::new(data_dir));
        let log_manager = Arc::new(Mutex::new(LogManager::new(log_dir)));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            num_bufs,
            file_manager.clone(),
            log_manager.clone(),
            SimpleEvictionPolicy::new(),
        )));

        Self {
            buffer_manager,
            file_manager,
            log_manager,
            lock_table: Arc::new(LockTable::new()),
        }
    }

    pub fn create_transaction(&self) -> Transaction<PAGE_SIZE> {
        Transaction::new(
            self.file_manager(),
            self.log_manager(),
            self.buffer_manager(),
            self.lock_table(),
        )
    }

    pub fn buffer_manager(&self) -> Arc<Mutex<BufferManager<PAGE_SIZE>>> {
        self.buffer_manager.clone()
    }

    pub fn file_manager(&self) -> Arc<FileManager<PAGE_SIZE>> {
        self.file_manager.clone()
    }

    pub fn lock_table(&self) -> Arc<LockTable> {
        self.lock_table.clone()
    }

    pub fn log_manager(&self) -> Arc<Mutex<LogManager<PAGE_SIZE>>> {
        self.log_manager.clone()
    }
}
