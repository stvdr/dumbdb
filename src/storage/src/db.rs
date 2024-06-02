use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    buffer_manager::BufferManager,
    eviction_policy::SimpleEvictionPolicy,
    file_manager::{self, FileManager},
    lock_table::LockTable,
    log_manager::LogManager,
};

pub struct SimpleDB<const P: usize> {
    buffer_manager: Arc<Mutex<BufferManager<P, SimpleEvictionPolicy>>>,
    file_manager: Arc<FileManager<P>>,
    lock_table: Arc<LockTable>,
    log_manager: Arc<Mutex<LogManager<P>>>,
}

impl<const P: usize> SimpleDB<P> {
    pub fn new(root_dir: &Path, num_bufs: usize) -> Self {
        let file_manager = Arc::new(FileManager::new(&root_dir.join("data")));
        let log_manager = Arc::new(Mutex::new(LogManager::new(&root_dir.join("log"))));
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
}
