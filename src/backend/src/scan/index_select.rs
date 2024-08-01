//use crate::{
//    index::index::Index, metadata::index_manager::IndexInfo, parser::constant::Value,
//    planning::plan::Plan, table_scan::TableScan,
//};
//
//use super::scan::{Scan, ScanResult};
//
//pub struct IndexSelectScan {
//    scan: TableScan,
//    idx: Box<dyn Index>,
//    val: Value,
//}
//
//impl<'a> IndexSelectScan {
//    pub fn new(scan: TableScan, idx: Box<dyn Index>, val: Value) -> Self {
//        Self { scan, idx, val }
//    }
//}
//
//impl Scan for IndexSelectScan {
//    fn before_first(&mut self) {
//        self.idx.before_first(&self.val);
//    }
//
//    fn next(&mut self) -> bool {
//        if !self.idx.next() {
//            return false;
//        }
//
//        if let Some(rid) = self.idx.get_rid() {
//            self.scan.move_to_rid(rid).is_ok()
//        } else {
//            false
//        }
//    }
//
//    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
//        self.scan.get_int(field_name)
//    }
//
//    fn get_string(&self, field_name: &str) -> ScanResult<String> {
//        self.scan.get_string(field_name)
//    }
//
//    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
//        self.scan.get_val(field_name)
//    }
//
//    fn has_field(&self, field_name: &str) -> bool {
//        self.scan.has_field(field_name)
//    }
//
//    fn close(&mut self) {
//        self.idx.close();
//        self.scan.close();
//    }
//}
