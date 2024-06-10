use std::collections::HashSet;

use super::{
    constant::Constant,
    scan::{Error, Scan, ScanResult, UpdateScan},
};

pub struct ProjectScan<'a> {
    field_list: HashSet<String>,
    scan: &'a mut dyn UpdateScan,
}

impl<'a> ProjectScan<'a> {
    pub fn new(field_list: HashSet<String>, scan: &'a mut dyn UpdateScan) -> Self {
        Self { field_list, scan }
    }
}

impl Scan for ProjectScan<'_> {
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
        self.field_list.contains(field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }
}
