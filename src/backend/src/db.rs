use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use crate::planning::basic_query_planner::BasicQueryPlanner;
use crate::planning::basic_update_planner::BasicUpdatePlanner;
use crate::planning::planner::Planner;
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
    metadata_manager: Arc<MetadataManager>,
    planner: Arc<Mutex<Planner>>,
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

        let metadata_manager = Arc::new(MetadataManager::new(&tx));

        tx.lock().unwrap().commit();

        let planner = Arc::new(Planner::new(
            Box::new(BasicQueryPlanner::new(metadata_manager.clone())),
            Box::new(BasicUpdatePlanner::new(metadata_manager.clone())),
        ));

        Self {
            buffer_manager,
            file_manager,
            log_manager,
            lock_table,
            metadata_manager,
            planner,
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

    pub fn metadata_manager(&self) -> Arc<MetadataManager> {
        self.metadata_manager.clone()
    }

    pub fn planner(&mut self) -> Arc<Mutex<Planner>> {
        self.planner.clone()
    }
}
