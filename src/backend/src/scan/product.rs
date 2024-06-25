use crate::parser::constant::Value;

use super::scan::{Scan, ScanResult};

pub struct ProductScan {
    left: Box<dyn Scan>,
    right: Box<dyn Scan>,
}

impl ProductScan {
    pub fn new(left: Box<dyn Scan>, right: Box<dyn Scan>) -> Self {
        let mut s = Self { left, right };
        s.before_first();
        s
    }
}

impl Scan for ProductScan {
    fn before_first(&mut self) {
        self.left.before_first();
        self.left.next();
        self.right.before_first();
    }

    fn next(&mut self) -> bool {
        let records_exist_in_right = self.right.next();
        if records_exist_in_right {
            true
        } else {
            self.right.before_first();
            self.right.next() && self.left.next()
        }
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        if self.left.has_field(field_name) {
            self.left.get_int(field_name)
        } else {
            self.right.get_int(field_name)
        }
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        if self.left.has_field(field_name) {
            self.left.get_string(field_name)
        } else {
            self.right.get_string(field_name)
        }
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
        if self.left.has_field(field_name) {
            self.left.get_val(field_name)
        } else {
            self.right.get_val(field_name)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.left.has_field(field_name) || self.right.has_field(field_name)
    }

    fn close(&mut self) {
        self.left.close();
        self.right.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        metadata::metadata_manager::MetadataManager,
        scan::scan::Scan,
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, default_test_db},
    };

    use super::ProductScan;

    #[test]
    fn test_product_scan() {
        let td = tempdir().unwrap();
        let mut db = default_test_db(&td);
        create_default_tables(&mut db);

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let meta_mgr = MetadataManager::new(&tx);

        let mut left_scan = Box::new(TableScan::new(
            tx.clone(),
            meta_mgr.get_table_layout("student", &tx).unwrap(),
            "student",
        ));

        let mut right_scan = Box::new(TableScan::new(
            tx.clone(),
            meta_mgr.get_table_layout("dept", &tx).unwrap(),
            "dept",
        ));

        let mut product_scan = ProductScan::new(left_scan, right_scan);

        for s in 1..10 {
            for d in [10, 20, 30].iter() {
                assert!(product_scan.next());
                assert_eq!(product_scan.get_int("sid").unwrap(), s);
                assert_eq!(product_scan.get_int("did").unwrap(), *d);
            }
        }

        assert!(!product_scan.next());
    }

    // TODO: test join against empty table
}
