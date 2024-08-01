use std::fmt::Display;

use crate::{
    scan::{product::ProductScan, scan::Scan},
    schema::Schema,
};

use super::plan::Plan;

pub struct ProductPlan {
    p1: Box<dyn Plan>,
    p2: Box<dyn Plan>,
    schema: Schema,
}

impl ProductPlan {
    pub fn new(p1: Box<dyn Plan>, p2: Box<dyn Plan>) -> Self {
        let mut schema = Schema::new();
        schema.add_all(p1.schema());
        schema.add_all(p2.schema());

        Self { p1, p2, schema }
    }
}

impl Plan for ProductPlan {
    fn open(&mut self) -> Scan {
        let mut s1 = self.p1.open();
        let mut s2 = self.p2.open();

        //match s1 {
        //    Scan::Select(scan) =>
        //}
        //
        Scan::Select(&mut ProductScan::new(s1.as_scannable(), s2.as_scannable()))
    }

    fn blocks_accessed(&self) -> u64 {
        self.p1.blocks_accessed() + (self.p1.records_output() * self.p2.blocks_accessed())
    }

    fn records_output(&self) -> u64 {
        self.p1.records_output() * self.p2.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> u64 {
        if self.p1.schema().has_field(field_name) {
            self.p1.distinct_values(field_name)
        } else {
            self.p2.distinct_values(field_name)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Display for ProductPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
