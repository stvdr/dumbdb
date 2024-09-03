use std::fmt::Display;

use crate::{
    parser::predicate::Predicate,
    scan::{
        scan::{Scan, UpdateScannable},
        select::SelectScan,
    },
    schema::Schema,
};

use super::plan::Plan;

pub struct SelectPlan {
    plan: Box<dyn Plan>,
    predicate: Predicate,
}

impl SelectPlan {
    pub fn new(plan: Box<dyn Plan>, predicate: Predicate) -> Self {
        Self { plan, predicate }
    }
}

impl Plan for SelectPlan {
    fn open(&mut self) -> Scan {
        let scan = Box::new(self.plan.open());

        Scan::Select(SelectScan::new(self.predicate.clone(), scan))
    }

    fn blocks_accessed(&self) -> u64 {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> u64 {
        self.plan.records_output() / self.predicate.reduction_factor(&*self.plan)
    }

    fn distinct_values(&self, field_name: &str) -> u64 {
        // TODO: refer to pg. 276 in the text
        1
    }

    fn schema(&self) -> &Schema {
        self.plan.schema()
    }
}

impl Display for SelectPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
