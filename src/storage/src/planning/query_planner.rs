use std::sync::{Arc, Mutex};

use crate::{parser::parser::SelectNode, transaction::Transaction};

use super::plan::Plan;

// TODO: figure out the result type

pub trait QueryPlanner {
    fn create_plan<const P: usize>(
        &mut self,
        data: &SelectNode,
        tx: Arc<Mutex<Transaction<P>>>,
    ) -> Result<Box<dyn Plan>, String>;
}
