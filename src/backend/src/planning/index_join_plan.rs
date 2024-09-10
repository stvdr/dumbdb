use std::fmt::{Display, Formatter};
use crate::metadata::index_manager::IndexInfo;
use crate::planning::plan::Plan;
use crate::scan::index_join_scan::IndexJoinScan;
use crate::scan::scan::Scan;
use crate::schema::Schema;

pub struct IndexJoinPlan {
    lhs_plan: Box<dyn Plan>,
    rhs_plan: Box<dyn Plan>,
    index_info: IndexInfo,
    join_field: String,
    schema: Schema,
}

impl IndexJoinPlan {
    pub fn new(lhs_plan: Box<dyn Plan>, rhs_plan: Box<dyn Plan>, index_info: IndexInfo, join_field: &str) -> Self {
        let mut schema = Schema::new();
        schema.add_all(lhs_plan.schema());
        schema.add_all(rhs_plan.schema());
        Self {
            lhs_plan,
            rhs_plan,
            index_info,
            join_field: join_field.to_string(),
            schema,
        }
    }
}

impl Plan for IndexJoinPlan {
    fn open(&mut self) -> Scan {
        let lhs_scan = Box::new(self.lhs_plan.open());
        let rhs_scan = Box::new(self.rhs_plan.open());
        let index = Box::new(self.index_info.open());

        Scan::IndexJoin(IndexJoinScan::new(lhs_scan, rhs_scan, index, &self.join_field))
    }

    fn blocks_accessed(&self) -> u64 {
        self.lhs_plan.blocks_accessed() + (self.lhs_plan.records_output() * self.index_info.blocks_accessed()) + self.records_output()
    }

    fn records_output(&self) -> u64 {
        self.lhs_plan.records_output() * self.index_info.records_outputs()
    }

    fn distinct_values(&self, field_name: &str) -> u64 {
        if self.lhs_plan.schema().has_field(field_name) {
            self.lhs_plan.distinct_values(field_name)
        } else {
            self.rhs_plan.distinct_values(field_name)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Display for IndexJoinPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}