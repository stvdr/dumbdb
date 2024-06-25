use std::sync::{Arc, Mutex};

use crate::{
    parser::parser::{CreateNode, DeleteNode, InsertNode, UpdateNode},
    transaction::Tx,
};

pub type RowCount = u64;

pub trait UpdatePlanner {
    fn execute_insert(&mut self, insert: &InsertNode, tx: Arc<Mutex<Tx>>);

    fn execute_delete(
        &mut self,
        delete: &DeleteNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String>;

    fn execute_modify(&mut self, modify: &UpdateNode, tx: Arc<Mutex<Tx>>);

    fn execute_create(&mut self, create: &CreateNode, tx: Arc<Mutex<Tx>>);
}
