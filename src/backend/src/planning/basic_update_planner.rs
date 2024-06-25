use std::sync::{Arc, Mutex, RwLock};

use crate::{
    metadata::metadata_manager::MetadataManager,
    parser::parser::{
        CreateNode, DeleteNode, FieldDefinitions, InsertNode, SelectNode, UpdateNode,
    },
    planning::table_plan::TablePlan,
    schema::Schema,
    transaction::Tx,
};

use super::{
    plan::Plan,
    select_plan::SelectPlan,
    update_planner::{RowCount, UpdatePlanner},
};

struct BasicUpdatePlanner {
    metadata_mgr: Arc<RwLock<MetadataManager>>,
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute_create(&mut self, create: &CreateNode, tx: Arc<Mutex<Tx>>) {
        match create {
            CreateNode::Table(name, fields) => self.create_table(name, fields, &tx),
            CreateNode::View(name, select) => self.create_view(name, select),
            CreateNode::Index(name, tblname, fieldname) => {
                self.create_index(name, tblname, fieldname)
            }
        }
    }

    fn execute_delete(
        &mut self,
        delete: &DeleteNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let mut lmm = self.metadata_mgr.write().unwrap();
        let mut plan: Box<dyn Plan> = Box::new(TablePlan::new(tx, &delete.0, &mut lmm));

        if let Some(pred) = &delete.1 {
            plan = Box::new(SelectPlan::new(plan, pred.clone()));
        }

        let mut scan = plan.open();
        let mut count = 0;
        while scan.next() {
            // TODO: error handling
            scan.delete();
            count += 1;
        }

        Ok(count)
    }

    fn execute_insert(&mut self, insert: &InsertNode, tx: Arc<Mutex<Tx>>) {
        todo!()
    }

    fn execute_modify(&mut self, update: &UpdateNode, tx: Arc<Mutex<Tx>>) {
        todo!()
    }
}

impl BasicUpdatePlanner {
    pub fn new(metadata_mgr: Arc<RwLock<MetadataManager>>) -> Self {
        Self { metadata_mgr }
    }

    fn create_index(&mut self, name: &str, tblname: &str, fieldname: &str) {
        todo!()
    }

    fn create_view(&mut self, name: &str, select: &SelectNode) {
        todo!()
    }

    fn create_table(&self, name: &str, fields: &FieldDefinitions, tx: &Arc<Mutex<Tx>>) {
        let schema = Schema::from_field_defs(fields);
        let mm = self.metadata_mgr.write().unwrap();
        mm.create_table(name, &schema, tx);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        layout::Layout,
        make_schema,
        parser::{
            lexer::Lexer,
            parser::{parse, CreateNode, DeleteNode, FieldDefinition, FieldType, Parser, RootNode},
        },
        planning::update_planner::UpdatePlanner,
        tests::test_utils::{create_default_tables, test_db},
    };

    use super::BasicUpdatePlanner;

    #[test]
    fn test_plan_delete_records() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = BasicUpdatePlanner::new(mm.clone());

        if let Ok(RootNode::Delete(del)) = parse("DELETE FROM student WHERE sid = 5") {
            let tx = Arc::new(Mutex::new(db.new_tx()));
            let count = planner
                .execute_delete(&del, tx)
                .expect("failed to execute delete statement");

            // Assert that 1 row was deleted
            assert_eq!(count, 1);
        } else {
            panic!("failed to parse delete statement");
        }
    }

    #[test]
    fn test_plan_create_table() {
        let testdir = tempdir().unwrap();
        let db = test_db(&testdir);
        let mm = db.metadata_manager();
        let mut planner = BasicUpdatePlanner::new(mm.clone());

        {
            // Create a test table
            let tx = Arc::new(Mutex::new(db.new_tx()));
            if let Ok(RootNode::Create(create)) =
                parse("CREATE TABLE test ( f1 int, f2 varchar(10) )")
            {
                planner.execute_create(&create, tx.clone());
            }

            tx.lock().unwrap().commit();
        }

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let layout = mm
            .read()
            .unwrap()
            .get_table_layout("test", &tx)
            .expect("test table was not created");

        let expected_schema = make_schema! {
            "f1" => i32,
            "f2" => varchar(10)
        };

        let expected_layout = Layout::from_schema(expected_schema);

        assert_eq!(layout, expected_layout);
    }

    #[test]
    fn test_plan_create_view() {}

    #[test]
    fn test_plan_create_index() {}
}
