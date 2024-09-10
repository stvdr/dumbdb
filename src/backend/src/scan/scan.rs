use crate::{index::index::Index, parser::constant::Value, rid::RID, table_scan::TableScan};
use crate::scan::index_join_scan::IndexJoinScan;

use super::{
    index_select_scan::IndexSelectScan, product_scan::ProductScan, project_scan::ProjectScan,
    select_scan::SelectScan,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ScanError {
    NonExistentField(String),
    UpdateNotSupported,
}

pub type ScanResult<T> = Result<T, ScanError>;

pub enum Scan {
    Table(TableScan),
    Select(SelectScan),
    Project(ProjectScan),
    Product(ProductScan),

    IndexSelect(IndexSelectScan),
    IndexJoin(IndexJoinScan),
}

impl Scannable for Scan {
    fn before_first(&mut self) {
        match self {
            Scan::Table(scan) => scan.before_first(),
            Scan::Select(scan) => scan.before_first(),
            Scan::Project(scan) => scan.before_first(),
            Scan::Product(scan) => scan.before_first(),

            Scan::IndexSelect(scan) => scan.before_first(),
            Scan::IndexJoin(scan) => scan.before_first(),
        }
    }

    fn next(&mut self) -> bool {
        match self {
            Scan::Table(scan) => scan.next(),
            Scan::Select(scan) => scan.next(),
            Scan::Project(scan) => scan.next(),
            Scan::Product(scan) => scan.next(),

            Scan::IndexSelect(scan) => scan.next(),
            Scan::IndexJoin(scan) => scan.next(),
        }
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        match self {
            Scan::Table(scan) => scan.get_int(field_name),
            Scan::Select(scan) => scan.get_int(field_name),
            Scan::Project(scan) => scan.get_int(field_name),
            Scan::Product(scan) => scan.get_int(field_name),

            Scan::IndexSelect(scan) => scan.get_int(field_name),
            Scan::IndexJoin(scan) => scan.get_int(field_name),
        }
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        match self {
            Scan::Table(scan) => scan.get_string(field_name),
            Scan::Select(scan) => scan.get_string(field_name),
            Scan::Project(scan) => scan.get_string(field_name),
            Scan::Product(scan) => scan.get_string(field_name),

            Scan::IndexSelect(scan) => scan.get_string(field_name),
            Scan::IndexJoin(scan) => scan.get_string(field_name),
        }
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
        match self {
            Scan::Table(scan) => scan.get_val(field_name),
            Scan::Select(scan) => scan.get_val(field_name),
            Scan::Project(scan) => scan.get_val(field_name),
            Scan::Product(scan) => scan.get_val(field_name),

            Scan::IndexSelect(scan) => scan.get_val(field_name),
            Scan::IndexJoin(scan) => scan.get_val(field_name),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        match self {
            Scan::Table(scan) => scan.has_field(field_name),
            Scan::Select(scan) => scan.has_field(field_name),
            Scan::Project(scan) => scan.has_field(field_name),
            Scan::Product(scan) => scan.has_field(field_name),

            Scan::IndexSelect(scan) => scan.has_field(field_name),
            Scan::IndexJoin(scan) => scan.has_field(field_name),
        }
    }

    fn close(&mut self) {
        match self {
            Scan::Table(scan) => scan.close(),
            Scan::Select(scan) => scan.close(),
            Scan::Project(scan) => scan.close(),
            Scan::Product(scan) => scan.close(),

            Scan::IndexSelect(scan) => scan.close(),
            Scan::IndexJoin(scan) => scan.close(),
        }
    }
}

impl UpdateScannable for Scan {
    fn set_int(&mut self, field_name: &str, val: i32) {
        match self {
            Scan::Table(scan) => scan.set_int(field_name, val),
            Scan::Select(scan) => scan.set_int(field_name, val),
            _ => panic!("Scan is not updateable"),
        };
    }

    fn set_string(&mut self, field_name: &str, val: &str) {
        match self {
            Scan::Table(scan) => scan.set_string(field_name, val),
            Scan::Select(scan) => scan.set_string(field_name, val),
            _ => panic!("Scan is not updateable"),
        };
    }

    fn set_val(&mut self, field_name: &str, val: &Value) {
        match self {
            Scan::Table(scan) => scan.set_val(field_name, val),
            Scan::Select(scan) => scan.set_val(field_name, val),
            _ => panic!("Scan is not updateable"),
        };
    }

    fn insert(&mut self) {
        match self {
            Scan::Table(scan) => scan.insert(),
            Scan::Select(scan) => scan.insert(),
            _ => panic!("Scan is not updateable"),
        };
    }

    fn delete(&mut self) {
        match self {
            Scan::Table(scan) => scan.delete(),
            Scan::Select(scan) => scan.delete(),
            _ => panic!("Scan is not updateable"),
        };
    }

    fn get_rid(&self) -> RID {
        match self {
            Scan::Table(scan) => scan.get_rid(),
            Scan::Select(scan) => scan.get_rid(),
            _ => panic!("Scan is not updateable"),
        }
    }

    fn move_to_rid(&mut self, rid: RID) {
        match self {
            Scan::Table(scan) => scan.move_to_rid(rid),
            Scan::Select(scan) => scan.move_to_rid(rid),
            _ => panic!("Scan is not updateable"),
        };
    }
}

pub trait Scannable {
    // Move before the first record in the scan. Immediately calling `next()` after this should
    // move to the first available record.
    fn before_first(&mut self);

    fn next(&mut self) -> bool;
    fn get_int(&self, field_name: &str) -> ScanResult<i32>;
    fn get_string(&self, field_name: &str) -> ScanResult<String>;
    fn get_val(&self, field_name: &str) -> ScanResult<Value>;
    fn has_field(&self, field_name: &str) -> bool;

    /// Close the scan and clean up as necessary.
    fn close(&mut self);
}

pub trait UpdateScannable: Scannable {
    fn set_int(&mut self, field_name: &str, val: i32);
    fn set_string(&mut self, field_name: &str, val: &str);
    fn set_val(&mut self, field_name: &str, val: &Value);
    fn insert(&mut self);
    fn delete(&mut self);
    fn get_rid(&self) -> RID;
    fn move_to_rid(&mut self, rid: RID);
}
