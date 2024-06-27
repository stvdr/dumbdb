use std::sync::{Arc, Mutex};

use crate::{
    parser::parser::{parse, CreateNode, RootNode},
    transaction::Tx,
};

use super::{
    plan::Plan,
    query_planner::QueryPlanner,
    update_planner::{RowCount, UpdatePlanner},
};

pub struct Planner {
    query_planner: Box<dyn QueryPlanner>,
    update_planner: Box<dyn UpdatePlanner>,
}

impl Planner {
    pub fn new(
        query_planner: Box<dyn QueryPlanner>,
        update_planner: Box<dyn UpdatePlanner>,
    ) -> Self {
        Self {
            query_planner,
            update_planner,
        }
    }

    pub fn create_query_plan(
        &self,
        cmd: &str,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<Box<dyn Plan>, String> {
        let ast = parse(cmd)?;
        match ast {
            RootNode::Select(select_node) => self.query_planner.create_plan(&select_node, tx),
            _ => Err("provided query does not support plan creation".to_string()),
        }
    }

    pub fn execute_update(&mut self, cmd: &str, tx: Arc<Mutex<Tx>>) -> Result<RowCount, String> {
        let ast = parse(cmd)?;
        match ast {
            RootNode::Create(create_node) => self.update_planner.execute_create(&create_node, tx),
            RootNode::Insert(insert_node) => self.update_planner.execute_insert(&insert_node, tx),
            RootNode::Delete(delete_node) => self.update_planner.execute_delete(&delete_node, tx),
            RootNode::Update(update_node) => self.update_planner.execute_modify(&update_node, tx),
            _ => Err("provided query does not support plan-less execution".to_string()),
        }
    }
}
