use std::fmt::Display;

use crate::{
    metadata::index_manager::IndexInfo,
    parser::constant::Value,
    planning::plan::Plan,
    scan::{index_select_scan::IndexSelectScan, scan::Scan},
    schema::Schema,
};

pub struct IndexSelectPlan {
    plan: Box<dyn Plan>,
    index_info: IndexInfo,
    val: Value,
}

impl IndexSelectPlan {
    pub fn new(plan: Box<dyn Plan>, index_info: IndexInfo, val: Value) -> Self {
        Self {
            plan,
            index_info,
            val,
        }
    }
}

impl Plan for IndexSelectPlan {
    fn open(&mut self) -> Scan {
        if let Scan::Table(scan) = self.plan.open() {
            let idx = Box::new(self.index_info.open());
            Scan::IndexSelect(IndexSelectScan::new(Box::new(scan), idx, self.val.clone()))
        } else {
            panic!("An index select plan can only wrap a TableScan");
        }
    }

    fn blocks_accessed(&self) -> u64 {
        self.index_info.blocks_accessed()
    }

    fn records_output(&self) -> u64 {
        self.index_info.records_outputs()
    }

    fn distinct_values(&self, field_name: &str) -> u64 {
        self.index_info.distinct_values(field_name)
    }

    fn schema(&self) -> &Schema {
        self.plan.schema()
    }
}

impl Display for IndexSelectPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
