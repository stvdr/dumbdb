use crate::{index::index::Index, parser::constant::Value};

use super::scan::{Scan, ScanResult, Scannable, UpdateScannable};

pub struct IndexJoinScan {
    lhs: Box<Scan>,
    rhs: Box<Scan>,
    index: Box<dyn Index>,
    join_field: String,
}

impl IndexJoinScan {
    /// Create a new IndexJoinScan that joins a `lhs` Scan against a `rhs` scan.
    /// An index on `join_field` must exist for the `rhs` Scan. A linear scan will execute against
    /// `lhs`; each record in `lhs` will incur an index lookup that will be used to set the position 
    /// in the `rhs` scan.
    pub fn new(lhs: Box<Scan>, rhs: Box<Scan>, index: Box<dyn Index>, join_field: &str) -> Self {
        Self {
            lhs,
            rhs,
            index,
            join_field: join_field.to_string(),
        }
    }
}

impl Scannable for IndexJoinScan {
    fn before_first(&mut self) {
        self.lhs.before_first();

        if !self.lhs.next() {
            return;
        }

        if let Ok(val) = self.lhs.get_val(&self.join_field) {
            self.index.before_first(&val);
        } else {
            // TODO: change `before_first` to return a Result
            panic!("index cannot be initialized");
        }
    }

    fn next(&mut self) -> bool {
        loop {
            if self.index.next() {
                // TODO: error handling
                let rid = self.index.get_rid().unwrap();
                println!("moved rhs to rid: {}", rid);
                self.rhs.move_to_rid(rid);
                return true;
            }

            if !self.lhs.next() {
                break;
            }

            if let Ok(val) = self.lhs.get_val(&self.join_field) {
                self.index.before_first(&val);
            } else {
                // TODO: proper error handling
                panic!("index cannot be initialized");
            }
        }

        false
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        if self.rhs.has_field(field_name) {
            self.rhs.get_int(field_name)
        } else {
            self.lhs.get_int(field_name)
        }
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        if self.rhs.has_field(field_name) {
            self.rhs.get_string(field_name)
        } else {
            self.lhs.get_string(field_name)
        }
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
        if self.rhs.has_field(field_name) {
            self.rhs.get_val(field_name)
        } else {
            self.lhs.get_val(field_name)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.rhs.has_field(field_name) || self.lhs.has_field(field_name)
    }

    fn close(&mut self) {
        self.lhs.close();
        self.index.close();
        self.rhs.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        assert_table_scan_results,
        metadata::metadata_manager::MetadataManager,
        parser::{constant::Value, expression::Expression, predicate::Predicate, term::Term},
        scan::scan::{Scan, Scannable},
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, default_test_db},
    };

    use super::IndexJoinScan;

    #[test]
    fn test_index_join_scan() {
        let td = tempdir().unwrap();
        let mut db = default_test_db(&td);
        create_default_tables(&mut db);

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let metadata_manager = MetadataManager::new(&tx);

        let lhs_layout= metadata_manager.get_table_layout("enroll", &tx).unwrap();
        let rhs_layout= metadata_manager.get_table_layout("student", &tx).unwrap();
        let lhs = Box::new(Scan::Table(TableScan::new(
            tx.clone(),
            lhs_layout,
            "enroll",
        )));
        let rhs= Box::new(Scan::Table(TableScan::new(
            tx.clone(),
            rhs_layout,
            "student",
        )));
        let index_info = metadata_manager.get_index_info("student", tx.clone());
        let index = Box::new(index_info.get("sid").unwrap().open());

        let mut ijs = IndexJoinScan::new(lhs, rhs, index, "sid");

        ijs.before_first();

        assert!(ijs.next());
        assert_eq!(ijs.get_int("eid").unwrap(), 14);
        assert_eq!(ijs.get_int("sid").unwrap(), 1);
        assert_eq!(ijs.get_int("section_id").unwrap(), 13);
        assert_eq!(ijs.get_string("grade").unwrap(), "A");
        assert_eq!(ijs.get_string("sname").unwrap(), "joe");

        assert!(ijs.next());
        assert_eq!(ijs.get_int("eid").unwrap(), 24);
        assert_eq!(ijs.get_int("sid").unwrap(), 1);
        assert_eq!(ijs.get_int("section_id").unwrap(), 43);
        assert_eq!(ijs.get_string("grade").unwrap(), "C");
        assert_eq!(ijs.get_string("sname").unwrap(), "joe");

        assert!(ijs.next());
        assert_eq!(ijs.get_int("eid").unwrap(), 34);
        assert_eq!(ijs.get_int("sid").unwrap(), 2);
        assert_eq!(ijs.get_int("section_id").unwrap(), 43);
        assert_eq!(ijs.get_string("grade").unwrap(), "B+");
        assert_eq!(ijs.get_string("sname").unwrap(), "amy");

        assert!(ijs.next());
        assert_eq!(ijs.get_int("eid").unwrap(), 44);
        assert_eq!(ijs.get_int("sid").unwrap(), 4);
        assert_eq!(ijs.get_int("section_id").unwrap(), 33);
        assert_eq!(ijs.get_string("grade").unwrap(), "B");
        assert_eq!(ijs.get_string("sname").unwrap(), "sue");

        assert!(ijs.next());
        assert_eq!(ijs.get_int("eid").unwrap(), 54);
        assert_eq!(ijs.get_int("sid").unwrap(), 4);
        assert_eq!(ijs.get_int("section_id").unwrap(), 53);
        assert_eq!(ijs.get_string("grade").unwrap(), "A");
        assert_eq!(ijs.get_string("sname").unwrap(), "sue");

        assert!(ijs.next());
        assert_eq!(ijs.get_int("eid").unwrap(), 64);
        assert_eq!(ijs.get_int("sid").unwrap(), 6);
        assert_eq!(ijs.get_int("section_id").unwrap(), 53);
        assert_eq!(ijs.get_string("grade").unwrap(), "A");
        assert_eq!(ijs.get_string("sname").unwrap(), "kim");

        assert!(!ijs.next());
    }
}
