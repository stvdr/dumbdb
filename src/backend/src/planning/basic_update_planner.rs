use std::sync::{Arc, Mutex, RwLock};

use crate::{
    metadata::metadata_manager::MetadataManager,
    parser::parser::{
        CreateNode, DeleteNode, FieldDefinitions, InsertNode, SelectNode, UpdateNode,
    },
    planning::table_plan::TablePlan,
    scan::scan::{Scannable, UpdateScannable},
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
    fn execute_create(
        &mut self,
        create: &CreateNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        match create {
            CreateNode::Table(name, fields) => self.create_table(name, fields, &tx),
            CreateNode::View(name, select) => self.create_view(name, select, &tx),
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
        let mut plan: Box<dyn Plan> = {
            let mut lmm = self.metadata_mgr.write().unwrap();
            Box::new(TablePlan::new(tx, &delete.0, &mut lmm))
        };

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

    fn execute_insert(
        &mut self,
        insert: &InsertNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let layout = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_table_layout(&insert.0, &tx)
            .ok_or(format!("table '{}' does not exist", &insert.0));

        let mut plan: Box<dyn Plan> = {
            let mut lmm = self.metadata_mgr.write().unwrap();
            Box::new(TablePlan::new(tx, &insert.0, &mut lmm))
        };

        let mut scan = plan.open();
        scan.insert();
        let field_values = insert.1.iter().zip(insert.2.iter());
        for (field_name, val) in field_values {
            scan.set_val(field_name, val);
        }

        Ok(1)
    }

    fn execute_modify(
        &mut self,
        update: &UpdateNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let mut plan: Box<dyn Plan> = {
            let mut lmm = self.metadata_mgr.write().unwrap();
            let table_plan = Box::new(TablePlan::new(tx, &update.id, &mut lmm));

            if let Some(pred) = &update.where_clause {
                // Wrap the table plan in a select plan so records can be filtered by the predicate
                Box::new(SelectPlan::new(table_plan, pred.clone()))
            } else {
                table_plan
            }
        };

        let mut scan = plan.open();
        let mut count = 0;
        while scan.next() {
            let val = update.expr.evaluate(&scan);
            scan.set_val(&update.field, &val);
            count += 1;
        }
        Ok(count)
    }
}

impl BasicUpdatePlanner {
    pub fn new(metadata_mgr: Arc<RwLock<MetadataManager>>) -> Self {
        Self { metadata_mgr }
    }

    fn create_index(
        &mut self,
        name: &str,
        tblname: &str,
        fieldname: &str,
    ) -> Result<RowCount, String> {
        Ok(0)
    }

    fn create_view(
        &mut self,
        name: &str,
        select: &SelectNode,
        tx: &Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let lock = self.metadata_mgr.write().unwrap();
        let view_source = format!("{}", select);
        lock.create_view(name, &view_source, tx);

        Ok(0)
    }

    fn create_table(
        &self,
        name: &str,
        fields: &FieldDefinitions,
        tx: &Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let schema = Schema::from_field_defs(fields);
        let mm = self.metadata_mgr.write().unwrap();
        mm.create_table(name, &schema, tx);
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        assert_scan_results,
        layout::Layout,
        make_schema,
        parser::{
            lexer::Lexer,
            parser::{parse, CreateNode, DeleteNode, FieldDefinition, FieldType, Parser, RootNode},
        },
        planning::update_planner::UpdatePlanner,
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, test_db},
    };

    use super::BasicUpdatePlanner;

    #[test]
    fn test_plan_insert_statement() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = BasicUpdatePlanner::new(mm.clone());

        if let Ok(RootNode::Insert(insert)) = parse(
            "INSERT INTO student (sid, sname, grad_year, major_id) VALUES (10, 'steve', 2025, 30)",
        ) {
            let tx = Arc::new(Mutex::new(db.new_tx()));
            let count = planner
                .execute_insert(&insert, tx.clone())
                .expect("failed to execute insert statement");

            // Assert that 1 row was inserted
            assert_eq!(count, 1);

            let mut scan = TableScan::new(
                tx.clone(),
                mm.read().unwrap().get_table_layout("student", &tx).unwrap(),
                "student",
            );

            assert_scan_results![
                scan,
                (1, "joe", 2021, 10),
                (2, "amy", 2020, 20),
                (3, "max", 2022, 10),
                (4, "sue", 2022, 20),
                (5, "bob", 2020, 30),
                (6, "kim", 2020, 20),
                (7, "art", 2021, 30),
                (8, "pat", 2019, 20),
                (9, "lee", 2021, 10),
                (10, "steve", 2025, 30)
            ];
        } else {
            panic!("failed to parse insert statement");
        }
    }

    #[test]
    fn test_plan_delete_statement() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = BasicUpdatePlanner::new(mm.clone());

        if let Ok(RootNode::Delete(del)) = parse("DELETE FROM student WHERE sid = 5") {
            let tx = Arc::new(Mutex::new(db.new_tx()));
            let count = planner
                .execute_delete(&del, tx.clone())
                .expect("failed to execute delete statement");

            assert_eq!(count, 1);

            let mut scan = TableScan::new(
                tx.clone(),
                mm.read().unwrap().get_table_layout("student", &tx).unwrap(),
                "student",
            );

            assert_scan_results![
                scan,
                (1, "joe", 2021, 10),
                (2, "amy", 2020, 20),
                (3, "max", 2022, 10),
                (4, "sue", 2022, 20),
                (6, "kim", 2020, 20),
                (7, "art", 2021, 30),
                (8, "pat", 2019, 20),
                (9, "lee", 2021, 10)
            ];
        } else {
            panic!("failed to parse delete statement");
        }
    }

    #[test]
    fn test_plan_update_statement() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = BasicUpdatePlanner::new(mm.clone());

        if let Ok(RootNode::Update(update)) =
            parse("UPDATE student SET major_id=5 WHERE major_id=30 AND grad_year=2020")
        {
            let tx = Arc::new(Mutex::new(db.new_tx()));
            let count = planner
                .execute_modify(&update, tx.clone())
                .expect("failed to execute update statement");

            // Assert that 1 row was updated
            assert_eq!(count, 1);

            let mut scan = TableScan::new(
                tx.clone(),
                mm.read().unwrap().get_table_layout("student", &tx).unwrap(),
                "student",
            );

            assert_scan_results![
                scan,
                (1, "joe", 2021, 10),
                (2, "amy", 2020, 20),
                (3, "max", 2022, 10),
                (4, "sue", 2022, 20),
                (5, "bob", 2020, 5),
                (6, "kim", 2020, 20),
                (7, "art", 2021, 30),
                (8, "pat", 2019, 20),
                (9, "lee", 2021, 10)
            ];
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
    fn test_plan_create_view() {
        let testdir = tempdir().unwrap();
        let db = test_db(&testdir);
        let mm = db.metadata_manager();
        let mut planner = BasicUpdatePlanner::new(mm.clone());

        {
            // Create a test table
            let tx = Arc::new(Mutex::new(db.new_tx()));
            if let Ok(RootNode::Create(create_statement)) =
                parse("CREATE VIEW test_view AS SELECT sid FROM student")
            {
                let rows = planner
                    .execute_create(&create_statement, tx.clone())
                    .expect("failed to execute CREATE VIEW statement");
                assert_eq!(rows, 0);
            } else {
                panic!("Failed to parse CREATE VIEW statement");
            }

            tx.lock().unwrap().commit();
        }

        let tx = Arc::new(Mutex::new(db.new_tx()));
        let view_def = mm
            .read()
            .unwrap()
            .get_view_def(&"test_view", &tx)
            .expect("Did not find test view");
        assert_eq!(view_def, "SELECT sid FROM student");
    }

    #[test]
    fn test_plan_create_index() {}
}
