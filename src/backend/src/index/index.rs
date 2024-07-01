use xxhash_rust::xxh3::xxh3_64;

use crate::{parser::constant::Value, rid::RID};

pub trait Hashable {
    fn hash(&self) -> u64;
}

impl Hashable for Value {
    fn hash(&self) -> u64 {
        match self {
            Value::Int(v) => xxh3_64(&v.to_le_bytes()),
            Value::Varchar(v) => xxh3_64(v.as_bytes()),
        }
    }
}

pub trait Index {
    fn before_first(&mut self, search_key: &Value);
    fn next(&mut self) -> bool;
    fn get_rid(&self) -> Option<RID>;
    fn insert(&mut self, key: &Value, rid: RID);
    fn delete(&mut self, key: &Value, rid: RID);
    fn close(&mut self);
}
