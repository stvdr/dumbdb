use crate::{
    index::index::Index, metadata::index_manager::IndexInfo, parser::constant::Value,
    planning::plan::Plan, table_scan::TableScan,
};

use super::scan::{Scan, ScanResult, Scannable, UpdateScannable};

pub struct IndexSelectScan {
    inner_scan: Box<TableScan>,
    idx: Box<dyn Index>,
    val: Value,
}

impl<'a> IndexSelectScan {
    pub fn new(scan: Box<TableScan>, idx: Box<dyn Index>, val: Value) -> Self {
        Self {
            inner_scan: scan,
            idx,
            val,
        }
    }
}

impl Scannable for IndexSelectScan {
    fn before_first(&mut self) {
        self.idx.before_first(&self.val);
    }

    fn next(&mut self) -> bool {
        if !self.idx.next() {
            return false;
        }

        if let Some(rid) = self.idx.get_rid() {
            self.inner_scan.move_to_rid(rid);
            true
        } else {
            false
        }
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        self.inner_scan.get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        self.inner_scan.get_string(field_name)
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
        self.inner_scan.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.inner_scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.idx.close();
        self.inner_scan.close();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        mem,
        sync::{Arc, Mutex},
    };

    use tempfile::tempdir;

    use crate::{
        index::btree::btree_index::BTreeIndex,
        metadata::metadata_manager::MetadataManager,
        parser::{
            constant::Value,
            expression::Expression,
            parser::{self, Parser},
            predicate::Predicate,
            term::Term,
        },
        scan::scan::{Scan, Scannable},
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, default_test_db},
    };

    use super::IndexSelectScan;

    #[test]
    fn test_select_index_scan() {
        let td = tempdir().unwrap();
        let mut db = default_test_db(&td);
        create_default_tables(&mut db);

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let metadata_manager = MetadataManager::new(&tx);

        let table_layout = metadata_manager.get_table_layout("student", &tx).unwrap();
        let indexes = metadata_manager.get_index_info("student", tx.clone());
        let ii = indexes.get("sid").unwrap();
        let index = Box::new(ii.open());

        //unsafe {
        //    let bi: BTreeIndex = mem::transmute(index);
        //    bi.generate_dot_file("test_dot");
        //}

        let table_scan = Box::new(TableScan::new(tx.clone(), table_layout, "student"));
        let mut idx_select_scan = IndexSelectScan::new(table_scan, index, Value::Int(4));

        idx_select_scan.before_first();
        assert!(idx_select_scan.next());
        assert_eq!(idx_select_scan.get_int("sid").unwrap(), 4);
        assert!(!idx_select_scan.next());
    }
}
