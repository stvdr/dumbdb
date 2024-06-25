use std::{
    fmt::Display,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    layout::Layout,
    metadata::{metadata_manager::MetadataManager, stats_manager::StatisticsInfo},
    scan::scan::Scan,
    schema::Schema,
    table_scan::TableScan,
    transaction::Transaction,
};

use super::plan::Plan;

pub struct TablePlan {
    tx: Arc<Mutex<Transaction>>,
    tbl_name: String,
    layout: Layout,
    stat_info: StatisticsInfo,
}

impl TablePlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        tbl_name: &str,
        meta_mgr: &mut MetadataManager,
    ) -> Self {
        let layout = meta_mgr
            .get_table_layout(&tbl_name, &tx)
            .expect(&format!("cannot find table: {}", tbl_name));
        let stat_info = meta_mgr
            .get_stat_info(&tbl_name, &layout, &tx)
            .expect(&format!(
                "could not fetch statistics for table: {}",
                tbl_name
            ));
        Self {
            tx,
            tbl_name: tbl_name.to_string(),
            layout,
            stat_info,
        }
    }
}

impl Plan for TablePlan {
    fn open(&mut self) -> Box<dyn Scan> {
        Box::new(TableScan::new(
            self.tx.clone(),
            self.layout.clone(),
            &self.tbl_name,
        ))
    }

    fn blocks_accessed(&self) -> u64 {
        self.stat_info.blocks_accessed()
    }

    fn records_output(&self) -> u64 {
        self.stat_info.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> u64 {
        self.stat_info.distinct_values(&self.tbl_name)
    }

    fn schema(&self) -> &Schema {
        self.layout.schema()
    }
}

impl Display for TablePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
