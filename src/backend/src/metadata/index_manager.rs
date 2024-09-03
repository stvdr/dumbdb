use std::sync::{Arc, Mutex};

use crate::{
    index::btree::btree_index::BTreeIndex,
    layout::Layout,
    scan::scan::{Scan, UpdateScannable},
    schema::Schema,
    table_scan::TableScan,
    transaction::Tx,
};

use super::{
    stats_manager::{StatisticsInfo, StatisticsManager},
    table_manager::{TableManager, MAX_NAME},
};

pub struct IndexInfo {
    name: String,
    field_name: String,
    tx: Arc<Mutex<Tx>>,
    layout: Layout,
    stat_info: StatisticsInfo,
}

impl IndexInfo {
    fn new(
        name: String,
        field_name: String,
        tx: Arc<Mutex<Tx>>,
        layout: Layout,
        stat_info: StatisticsInfo,
    ) -> Self {
        Self {
            name,
            field_name,
            tx,
            layout,
            stat_info,
        }
    }

    // TODO: return different types of indexes
    pub fn open(&self) -> BTreeIndex {
        BTreeIndex::new(self.tx.clone(), &self.name, self.layout.clone())
    }

    pub fn blocks_accessed(&self) -> u64 {
        let recs_per_blk = self.tx.lock().unwrap().block_size() as u64 / self.layout.slot_size();
        let num_blks = self.stat_info.records_output() / recs_per_blk;
        BTreeIndex::search_cost(num_blks, recs_per_blk)
    }

    pub fn records_outputs(&self) -> u64 {
        self.stat_info.records_output() / self.stat_info.distinct_values(&self.field_name)
    }

    pub fn distinct_values(&self, field_name: &str) -> u64 {
        if self.field_name == field_name {
            1
        } else {
            self.stat_info.distinct_values(field_name)
        }
    }

    // TODO: update after index definitions exist
    pub fn create_index_layout(&self) -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("block");
        schema.add_int_field("id");

        // TODO: field type should be an enum?
        // TODO: additional types
        if self
            .layout
            .schema()
            .get_field_type(&self.field_name)
            .expect("field does not exist")
            == 0
        {
            schema.add_int_field("dataval");
        } else {
            let len = self
                .layout
                .schema()
                .get_field_length(&self.field_name)
                .expect("field does not exist");
            schema.add_string_field("dataval", len);
        }

        Layout::from_schema(schema)
    }
}

pub struct IndexManager {
    layout: Layout,
    tbl_mgr: TableManager,
    stat_mgr: Arc<Mutex<StatisticsManager>>,
}

impl IndexManager {
    pub fn new(stats_mgr: Arc<Mutex<StatisticsManager>>, tx: &Arc<Mutex<Tx>>) -> Self {
        let mut schema = Schema::new();
        schema.add_string_field("indexname", MAX_NAME);
        schema.add_string_field("tablename", MAX_NAME);
        schema.add_string_field("fieldname", MAX_NAME);

        let s = Self {
            layout: Layout::from_schema(schema),
            tbl_mgr: TableManager::new(tx),
            stat_mgr: stats_mgr,
        };

        s
    }

    pub fn create_index(
        &self,
        idx_name: &str,
        tbl_name: &str,
        field_name: &str,
        tx: Arc<Mutex<Tx>>,
    ) {
        // TODO: verify that index does not already exist
        let mut scan = TableScan::new(tx, self.layout.clone(), "idxcat");
        scan.insert();
        scan.set_string("indexname", idx_name);
        scan.set_string("tablename", tbl_name);
        scan.set_string("fieldname", field_name);
    }

    // TODO: figure this out once indexes actually exist. Not clear how transactions fit in here at
    // the moment.
    //pub fn get_index_info<const P: usize>(
    //    &self,
    //    idx_name: &str,
    //    tx: Arc<Mutex<Transaction<P>>>,
    //) -> Option<IndexInfo<P>> {

    //}
}
