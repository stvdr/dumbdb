//use std::fmt::Display;
//
//use crate::{
//    metadata::index_manager::IndexInfo,
//    parser::constant::Value,
//    planning::plan::Plan,
//    scan::{index_select::IndexSelectScan, scan::Scan},
//};
//
//pub struct IndexSelectPlan {
//    //scan: Box<dyn Scan>,
//    plan: Box<dyn Plan>,
//    index_info: IndexInfo,
//    val: Value,
//}
//
//impl IndexSelectPlan {
//    pub fn new(plan: Box<dyn Plan>, index_info: IndexInfo, val: Value) -> Self {
//        Self {
//            plan,
//            index_info,
//            val,
//        }
//    }
//}
//
//impl Plan for IndexSelectPlan {
//    fn open(&mut self) -> Box<dyn Scan> {
//        let scan = self.plan.open();
//        let idx = Box::new(self.index_info.open());
//        Box::new(IndexSelectScan::new(scan, idx, self.val.clone()))
//    }
//
//    fn blocks_accessed(&self) -> u64 {
//        todo!()
//    }
//
//    fn records_output(&self) -> u64 {
//        todo!()
//    }
//
//    fn distinct_values(&self, field_name: &str) -> u64 {
//        todo!()
//    }
//
//    fn schema(&self) -> &crate::schema::Schema {
//        todo!()
//    }
//}
//
//impl Display for IndexSelectPlan {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        todo!()
//    }
//}
