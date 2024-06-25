use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{layout::Layout, scan::scan::Scan, table_scan::TableScan, transaction::Tx};

use super::table_manager::TableManager;

#[derive(Clone)]
pub struct StatisticsInfo {
    num_blocks: u64,
    num_records: u64,
}

impl StatisticsInfo {
    pub fn new(num_blocks: u64, num_records: u64) -> Self {
        Self {
            num_blocks,
            num_records,
        }
    }

    pub fn blocks_accessed(&self) -> u64 {
        self.num_blocks
    }

    pub fn records_output(&self) -> u64 {
        self.num_records
    }

    // TODO: lol
    pub fn distinct_values(&self, field_name: &str) -> u64 {
        1 + (self.records_output() / 3) as u64
    }
}

pub struct StatisticsManager {
    tbl_mgr: TableManager,
    tbl_stats: Arc<Mutex<HashMap<String, StatisticsInfo>>>,
    num_calls: usize,
}

impl StatisticsManager {
    pub fn new(tx: &Arc<Mutex<Tx>>) -> Self {
        let mut s = Self {
            tbl_mgr: TableManager::new(tx),
            tbl_stats: Arc::new(Mutex::new(HashMap::new())),
            num_calls: 0,
        };

        s.refresh_stats(&tx);

        s
    }

    pub fn get_stats(
        &mut self,
        tbl_name: &str,
        layout: &Layout,
        tx: &Arc<Mutex<Tx>>,
    ) -> Option<StatisticsInfo> {
        self.num_calls += 1;

        if self.num_calls > 100 {
            self.refresh_stats(tx);
        }

        let mut l = self.tbl_stats.lock().unwrap();
        let stats = l
            .entry(tbl_name.to_string())
            .or_insert_with(|| self.calculate_stats(tbl_name, layout, &tx));
        Some(stats.clone())
    }

    fn refresh_stats(&mut self, tx: &Arc<Mutex<Tx>>) {
        let mut new_stats = HashMap::new();
        self.num_calls = 0;
        let tcat_layout = self
            .tbl_mgr
            .get_table_layout("tblcat", tx)
            .expect("tblcat metadata table does not exist");
        let mut scan = TableScan::new(tx.clone(), tcat_layout, "tblcat");
        while scan.next() {
            let tblname = scan
                .get_string("tblname")
                .expect("table metadata not available");
            let layout = self
                .tbl_mgr
                .get_table_layout(&tblname, tx)
                .expect("could not get layout for table");
            let stats = self.calculate_stats(&tblname, &layout, tx);
            new_stats.insert(tblname, stats);
        }

        self.tbl_stats = Arc::new(Mutex::new(new_stats));
    }

    fn calculate_stats(
        &self,
        tbl_name: &str,
        layout: &Layout,
        tx: &Arc<Mutex<Tx>>,
    ) -> StatisticsInfo {
        let mut num_records = 0;
        let mut num_blocks = 0;

        let mut scan = TableScan::new(tx.clone(), layout.clone(), tbl_name);
        while scan.next() {
            num_records += 1;
            // TODO: error handling
            num_blocks = scan
                .get_rid()
                .expect("failed updating stats for non-updateable scan")
                .block_num()
                + 1;
        }

        StatisticsInfo::new(num_blocks, num_records)
    }
}
