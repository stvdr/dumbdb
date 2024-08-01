use crate::{parser::constant::Value, rid::RID, tuple::Tuple};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Error {
    NonExistentField(String),

    UpdateNotSupported,
}

pub type ScanResult<T> = Result<T, Error>;

pub trait Scan {
    // Move before the first record in the scan. Immediately calling `next()` after this should
    // move to the first available record.
    fn before_first(&mut self);

    fn next(&mut self) -> bool;
    fn get_tuple(&self) -> ScanResult<Tuple>;
    //fn get_int(&self, field_name: &str) -> ScanResult<i32>;
    //fn get_string(&self, field_name: &str) -> ScanResult<String>;
    //fn get_val(&self, field_name: &str) -> ScanResult<Value>;
    //fn has_field(&self, field_name: &str) -> bool;

    // Update support
    //fn set_int(&mut self, field_name: &str, val: i32) -> ScanResult<()> {
    //    Err(Error::UpdateNotSupported)
    //}

    //fn set_string(&mut self, field_name: &str, val: &str) -> ScanResult<()> {
    //    Err(Error::UpdateNotSupported)
    //}

    //fn set_val(&mut self, field_name: &str, val: &Value) -> ScanResult<()> {
    //    Err(Error::UpdateNotSupported)
    //}

    fn insert(&mut self) -> ScanResult<()> {
        Err(Error::UpdateNotSupported)
    }

    fn delete(&mut self) -> ScanResult<()> {
        Err(Error::UpdateNotSupported)
    }

    fn get_rid(&self) -> ScanResult<RID> {
        Err(Error::UpdateNotSupported)
    }

    fn move_to_rid(&mut self, rid: RID) -> ScanResult<()> {
        Err(Error::UpdateNotSupported)
    }

    /// Close the scan and clean up as necessary.
    fn close(&mut self);
}

// A scan that also supports updating values
//pub trait UpdateScan: Scan {
//    fn set_int(&mut self, field_name: &str, val: i32);
//    fn set_string(&mut self, field_name: &str, val: &str);
//    fn set_val(&mut self, field_name: &str, val: Constant);
//    fn insert(&mut self);
//    fn delete(&mut self);
//    fn get_rid(&self) -> RID;
//    fn move_to_rid(&mut self, rid: RID);
//}
