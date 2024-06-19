use std::collections::HashSet;

use super::{
    constant::Constant,
    scan::{Error, Scan, ScanResult, UpdateScan},
};

pub struct ProjectScan {
    field_list: Vec<String>,
    scan: Box<dyn Scan>,
}

impl ProjectScan {
    pub fn new(field_list: Vec<String>, scan: Box<dyn Scan>) -> Self {
        let mut s = Self { field_list, scan };
        s.before_first();
        s
    }
}

impl Scan for ProjectScan {
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> bool {
        self.scan.next()
    }

    // TODO: learn / figure out what the Result should look like
    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        if !self.has_field(field_name) {
            Err(Error::NonExistentField(field_name.to_string()))
        } else {
            self.scan.get_int(field_name)
        }
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        if !self.has_field(field_name) {
            Err(Error::NonExistentField(field_name.to_string()))
        } else {
            self.scan.get_string(field_name)
        }
    }

    // TODO: implement Constants
    fn get_val(&self, field_name: &str) -> ScanResult<Constant> {
        self.scan.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.field_list.contains(&field_name.to_string())
    }

    fn close(&mut self) {
        self.scan.close();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
    };

    use tempfile::tempdir;

    use crate::{
        metadata::metadata_manager::MetadataManager,
        scan::scan::Scan,
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, default_test_db},
    };

    use super::ProjectScan;

    #[test]
    fn test_project_scan() {
        let td = tempdir().unwrap();
        let mut db = default_test_db(&td);
        create_default_tables(&mut db);

        let tx = Arc::new(Mutex::new(db.create_transaction()));
        let meta_mgr = MetadataManager::new(&tx);

        let scan = Box::new(TableScan::new(
            tx.clone(),
            meta_mgr.get_table_layout("student", &tx).unwrap(),
            "student",
        ));

        let mut project_scan =
            ProjectScan::new(vec!["sid".to_string(), "grad_year".to_string()], scan);

        let mut num_students = 0;
        while project_scan.next() {
            assert!(project_scan.get_int("sid").is_ok());
            assert!(project_scan.get_int("grad_year").is_ok());
            assert!(project_scan.get_int("sname").is_err());
            assert!(project_scan.get_int("major_id").is_err());
            num_students += 1;
        }

        assert_eq!(num_students, 9);
    }
}
