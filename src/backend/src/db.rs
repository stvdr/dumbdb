use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    buffer_manager::BufferManager,
    eviction_policy::SimpleEvictionPolicy,
    file_manager::{self, FileManager},
    lock_table::LockTable,
    log_manager::LogManager,
    metadata::metadata_manager::MetadataManager,
    transaction::Tx,
};

const DEFAULT_BUFFER_SIZE: usize = 1024;

pub struct SimpleDB {
    buffer_manager: Arc<Mutex<BufferManager<SimpleEvictionPolicy>>>,
    file_manager: Arc<FileManager>,
    lock_table: Arc<LockTable>,
    log_manager: Arc<Mutex<LogManager>>,
    metadata_manager: Arc<RwLock<MetadataManager>>,
}

impl SimpleDB {
    pub fn new(data_dir: &Path, log_dir: &Path, num_bufs: usize) -> Self {
        let file_manager = Arc::new(FileManager::new(data_dir));
        let log_manager = Arc::new(Mutex::new(LogManager::new(log_dir)));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            num_bufs,
            file_manager.clone(),
            log_manager.clone(),
            SimpleEvictionPolicy::new(),
        )));
        let lock_table = Arc::new(LockTable::new());

        let tx = Arc::new(Mutex::new(Tx::new(
            file_manager.clone(),
            log_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
        )));

        let metadata_manager = Arc::new(RwLock::new(MetadataManager::new(&tx)));

        tx.lock().unwrap().commit();

        Self {
            buffer_manager,
            file_manager,
            log_manager,
            lock_table,
            metadata_manager,
        }
    }

    pub fn new_tx(&self) -> Tx {
        Tx::new(
            self.file_manager(),
            self.log_manager(),
            self.buffer_manager(),
            self.lock_table(),
        )
    }

    pub fn buffer_manager(&self) -> Arc<Mutex<BufferManager>> {
        self.buffer_manager.clone()
    }

    pub fn file_manager(&self) -> Arc<FileManager> {
        self.file_manager.clone()
    }

    pub fn lock_table(&self) -> Arc<LockTable> {
        self.lock_table.clone()
    }

    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        self.log_manager.clone()
    }

    pub fn metadata_manager(&self) -> Arc<RwLock<MetadataManager>> {
        self.metadata_manager.clone()
    }
}
