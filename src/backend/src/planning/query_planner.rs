use std::sync::{Arc, Mutex};

use crate::{parser::parser::SelectNode, transaction::Transaction};

use super::plan::Plan;

// TODO: figure out the result type

pub trait QueryPlanner {
    fn create_plan(
        &self,
        data: &SelectNode,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Box<dyn Plan>, String>;
}
