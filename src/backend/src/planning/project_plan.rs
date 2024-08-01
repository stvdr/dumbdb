use std::fmt::Display;

use crate::{
    scan::{
        project::ProjectScan,
        scan::{Scan, Scannable},
    },
    schema::Schema,
};

use super::plan::Plan;

pub struct ProjectPlan {
    plan: Box<dyn Plan>,
    schema: Schema,
}

impl ProjectPlan {
    pub fn new(plan: Box<dyn Plan>, field_list: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for field in field_list {
            schema.add_from(&field, &plan.schema());
        }
        Self { plan, schema }
    }
}

impl Plan for ProjectPlan {
    fn open(&mut self) -> Scan {
        let mut scan = self.plan.open();
        match scan {
            Scan::Select(scan) | Scan::Update(scan) => {
                Scan::Select(&mut ProjectScan::new(self.schema.fields(), scan))
            }
        }
    }

    fn blocks_accessed(&self) -> u64 {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> u64 {
        self.plan.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> u64 {
        self.plan.distinct_values(field_name)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Display for ProjectPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
