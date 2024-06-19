use crate::scan::{predicate::Predicate, scan::Scan, select::SelectScan};

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
    fn open(&mut self) -> Box<dyn Scan> {
        let scan = self.plan.open();
        Box::new(SelectScan::new(self.predicate.clone(), scan))
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

    fn schema(&self) -> crate::schema::Schema {
        self.plan.schema()
    }
}
