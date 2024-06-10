use crate::rid::RID;
use crate::scan::predicate::Predicate;
use crate::scan::scan::Scan;

use super::{
    constant::Constant,
    scan::{ScanResult, UpdateScan},
};

pub struct SelectScan<'a> {
    predicate: Predicate,
    scan: &'a mut dyn UpdateScan,
}

impl<'a> SelectScan<'a> {
    /// Creates a new Select Scan that will iterate over an underlying `UpdateScan`.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A predicate that will be applied to each record in the underlying scan.
    ///     Only records that satisfy the predicate will be returned by this scan.
    /// * `scan` - The `UpdateScan` underlying this `SelectScan`.
    pub fn new(predicate: Predicate, scan: &'a mut dyn UpdateScan) -> Self {
        Self { predicate, scan }
    }
}

impl Scan for SelectScan<'_> {
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> bool {
        while self.scan.next() {
            //if self.predicate.is_satisfied(self.scan.as_super_mut()) {
            if self.predicate.is_satisfied(self.scan) {
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

    fn get_val(&self, field_name: &str) -> ScanResult<Constant> {
        self.scan.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan.close()
    }
}

impl UpdateScan for SelectScan<'_> {
    fn set_int(&mut self, field_name: &str, val: i32) {
        self.scan.set_int(field_name, val);
    }

    fn set_string(&mut self, field_name: &str, val: &str) {
        self.scan.set_string(field_name, val);
    }

    fn set_val(&mut self, field_name: &str, val: Constant) {
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
