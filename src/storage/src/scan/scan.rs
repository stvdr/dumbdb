use crate::rid::RID;

use super::constant::Constant;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Error {
    NonExistentField(String),
}

pub type ScanResult<T> = Result<T, Error>;

//pub struct Constant {}

pub trait Scan {
    // Move before the first record in the scan. Immediately calling `next()` after this should
    // move to the first available record.
    fn before_first(&mut self);

    fn next(&mut self) -> bool;
    fn get_int(&self, field_name: &str) -> ScanResult<i32>;
    fn get_string(&self, field_name: &str) -> ScanResult<String>;
    fn get_val(&self, field_name: &str) -> ScanResult<Constant>;
    fn has_field(&self, field_name: &str) -> bool;

    /// Close the scan and clean up as necessary.
    fn close(&mut self);
}

//pub trait IntoSuper<Super: ?Sized> {
//    fn as_super(&self) -> &Super;
//    fn as_super_mut(&mut self) -> &mut Super;
//    fn into_super(self: Box<Self>) -> Box<Super>;
//}
//
//impl<'a, T: 'a + Scan> IntoSuper<dyn Scan + 'a> for T {
//    fn as_super(&self) -> &(dyn Scan + 'a) {
//        self
//    }
//    fn as_super_mut(&mut self) -> &mut (dyn Scan + 'a) {
//        self
//    }
//    fn into_super(self: Box<Self>) -> Box<dyn Scan + 'a> {
//        self
//    }
//}

// A scan that also supports updating values
pub trait UpdateScan: Scan {
    //+ IntoSuper<dyn Scan> {
    fn set_int(&mut self, field_name: &str, val: i32);
    fn set_string(&mut self, field_name: &str, val: &str);
    fn set_val(&mut self, field_name: &str, val: Constant);
    fn insert(&mut self);
    fn delete(&mut self);
    fn get_rid(&self) -> RID;
    fn move_to_rid(&mut self, rid: RID);
}
