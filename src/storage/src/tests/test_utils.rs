use std::fs;

use log::warn;
use tempfile::{tempdir, TempDir};

use crate::db::SimpleDB;

const DEFAULT_BLOCK_SIZE: usize = 4096;

/// Get a `SimpleDB` with log and data storage written into temporary directories.
pub fn test_db<const PAGE_SIZE: usize>(td: &TempDir) -> SimpleDB<PAGE_SIZE> {
    let data_dir = td.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let log_dir = td.path().join("log");
    fs::create_dir_all(&log_dir).unwrap();

    SimpleDB::new(&data_dir, &log_dir, 1024)
}

pub fn default_test_db(td: &TempDir) -> SimpleDB<DEFAULT_BLOCK_SIZE> {
    test_db(td)
}
