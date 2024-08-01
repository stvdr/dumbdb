use std::fmt::Display;

use crate::{scan::scan::Scan, schema::Schema};

pub trait Plan: Display {
    //fn open(&mut self) -> Box<dyn Scan>;
    fn open(&mut self) -> Scan;
    fn blocks_accessed(&self) -> u64;
    fn records_output(&self) -> u64;
    fn distinct_values(&self, field_name: &str) -> u64;
    fn schema(&self) -> &Schema;
}
