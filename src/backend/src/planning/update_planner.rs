use std::sync::{Arc, Mutex};

use crate::{
    parser::parser::{CreateNode, DeleteNode, InsertNode, UpdateNode},
    transaction::Transaction,
};

pub trait UpdatePlanner {
    fn execute_insert<const P: usize>(
        &mut self,
        insert: &InsertNode,
        tx: Arc<Mutex<Transaction<P>>>,
    );

    fn execute_delete<const P: usize>(
        &mut self,
        insert: &DeleteNode,
        tx: Arc<Mutex<Transaction<P>>>,
    );

    fn execute_modify<const P: usize>(
        &mut self,
        insert: &UpdateNode,
        tx: Arc<Mutex<Transaction<P>>>,
    );

    fn execute_create<const P: usize>(
        &mut self,
        insert: &CreateNode,
        tx: Arc<Mutex<Transaction<P>>>,
    );
}
