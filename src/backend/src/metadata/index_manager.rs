use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    index::btree::btree_index::BTreeIndex,
    layout::Layout,
    scan::scan::{Scan, Scannable, UpdateScannable},
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
        name: &str,
        field_name: &str,
        tx: Arc<Mutex<Tx>>,
        tbl_layout: &Layout,
        stat_info: StatisticsInfo,
    ) -> Self {
        Self {
            name: name.to_string(),
            field_name: field_name.to_string(),
            layout: IndexInfo::create_index_layout(&tbl_layout, field_name),
            tx,
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

    fn create_index_layout(tbl_layout: &Layout, field_name: &str) -> Layout {
        let mut schema = Schema::new();

        // TODO: field type should be an enum?
        // TODO: additional types
        if tbl_layout
            .schema()
            .get_field_type(field_name)
            .expect("field does not exist")
            == 0
        {
            schema.add_int_field("dataval");
        } else {
            let len = tbl_layout
                .schema()
                .get_field_length(field_name)
                .expect("field does not exist");
            schema.add_string_field("dataval", len);
        }

        schema.add_int_field("block");
        schema.add_int_field("id");

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

    /// Create an index on the specified table/field.
    pub fn create_index(
        &self,
        idx_name: &str,
        tbl_name: &str,
        field_name: &str,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<(), String> {
        // TODO: verify that index does not already exist
        let mut scan = TableScan::new(tx, self.layout.clone(), "idxcat");
        scan.insert();
        scan.set_string("indexname", idx_name);
        scan.set_string("tablename", tbl_name);
        scan.set_string("fieldname", field_name);

        // TODO: how can this always return Ok? Failure must be possible somewhere upstream..
        Ok(())
    }

    /// Gets index info for the specified table.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table.
    /// * `tx` - The transaction used to read from the metadata table.
    pub fn get_index_info(&self, tbl_name: &str, tx: Arc<Mutex<Tx>>) -> HashMap<String, IndexInfo> {
        let mut result = HashMap::new();
        let mut scan = TableScan::new(tx.clone(), self.layout.clone(), "idxcat");
        while scan.next() {
            let table_name = scan.get_string("tablename").unwrap();
            if table_name == tbl_name {
                let index_name = scan.get_string("indexname").unwrap();
                let field_name = scan.get_string("fieldname").unwrap();
                let table_layout = self.tbl_mgr.get_table_layout(&table_name, &tx).unwrap();
                let stats_info = self
                    .stat_mgr
                    .lock()
                    .unwrap()
                    .get_stats(&table_name, &table_layout, &tx)
                    .unwrap();
                let ii = IndexInfo::new(
                    &index_name,
                    &field_name,
                    tx.clone(),
                    &table_layout,
                    stats_info,
                );
                result.insert(field_name, ii);
            }
        }

        result
    }
}
