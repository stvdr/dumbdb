use crate::{
    parser::{constant::Value, predicate::Predicate},
    rid::RID,
};

use super::scan::{Scan, ScanResult, Scannable, UpdateScannable};

pub struct SelectScan {
    predicate: Predicate,
    scan: Box<Scan>,
}

impl SelectScan {
    /// Creates a new Select Scan that will iterate over an underlying `Scan`.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A predicate that will be applied to each record in the underlying scan.
    ///     Only records that satisfy the predicate will be returned by this scan.
    /// * `scan` - The `Scan` underlying this `SelectScan`.
    pub fn new(predicate: Predicate, scan: Box<Scan>) -> Self {
        Self { predicate, scan }
    }
}

impl Scannable for SelectScan {
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> bool {
        while self.scan.next() {
            //if self.predicate.is_satisfied(self.scan.as_super_mut()) {
            if self.predicate.is_satisfied(&*self.scan) {
                // This is a record that satisfies the predicate
                return true;
            }
        }

        false
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        self.scan.get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        self.scan.get_string(field_name)
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
        self.scan.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan.close()
    }
}

impl UpdateScannable for SelectScan {
    fn set_int(&mut self, field_name: &str, val: i32) {
        self.scan.set_int(field_name, val);
    }

    fn set_string(&mut self, field_name: &str, val: &str) {
        self.scan.set_string(field_name, val);
    }

    fn set_val(&mut self, field_name: &str, val: &Value) {
        self.scan.set_val(field_name, val);
    }

    fn insert(&mut self) {
        self.scan.insert();
    }

    fn delete(&mut self) {
        self.scan.delete();
    }

    fn get_rid(&self) -> RID {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: RID) {
        self.scan.move_to_rid(rid);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        metadata::metadata_manager::MetadataManager,
        parser::{constant::Value, expression::Expression, predicate::Predicate, term::Term},
        scan::scan::{Scan, Scannable},
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, default_test_db},
    };

    use super::SelectScan;

    #[test]
    fn test_select_scan_with_predicate() {
        let td = tempdir().unwrap();
        let mut db = default_test_db(&td);
        create_default_tables(&mut db);

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let metadata_manager = MetadataManager::new(&tx);

        let lhs1 = Expression::Field("grad_year".to_string());
        let value = Expression::Constant(Value::Int(2021));
        let rhs1 = value;
        let t1 = Term::new(lhs1, rhs1);

        let mut predicate = Predicate::from_term(t1);

        let table_layout = metadata_manager.get_table_layout("student", &tx).unwrap();
        let table_scan = Box::new(Scan::Table(TableScan::new(
            tx.clone(),
            table_layout,
            "student",
        )));
        let mut select_scan = SelectScan::new(predicate.clone(), table_scan);

        assert!(select_scan.next());
        assert_eq!(select_scan.get_int("sid").unwrap(), 1);
        assert!(select_scan.next());
        assert_eq!(select_scan.get_int("sid").unwrap(), 7);
        assert!(select_scan.next());
        assert_eq!(select_scan.get_int("sid").unwrap(), 9);
        assert!(!select_scan.next());
    }
}
