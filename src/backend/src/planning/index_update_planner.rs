use crate::index::index::Index;
use crate::insert;
use crate::metadata::metadata_manager::MetadataManager;
use crate::parser::parser::{
    CreateNode, DeleteNode, FieldDefinitions, InsertNode, SelectNode, UpdateNode,
};
use crate::planning::plan::Plan;
use crate::planning::select_plan::SelectPlan;
use crate::planning::table_plan::TablePlan;
use crate::planning::update_planner::{RowCount, UpdatePlanner};
use crate::scan::scan::Scan::Select;
use crate::scan::scan::{Scannable, UpdateScannable};
use crate::schema::Schema;
use crate::transaction::Tx;
use std::sync::{Arc, Mutex, RwLock};

struct IndexUpdatePlanner {
    metadata_mgr: Arc<RwLock<MetadataManager>>,
}

impl IndexUpdatePlanner {
    pub fn new(metadata_manager: Arc<RwLock<MetadataManager>>) -> Self {
        Self {
            metadata_mgr: metadata_manager,
        }
    }

    fn create_index(
        &mut self,
        name: &str,
        tblname: &str,
        fieldname: &str,
        tx: &Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let lock = self.metadata_mgr.write().unwrap();

        lock.create_index(name, tblname, fieldname, tx)?;
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
        lock.create_view(name, &view_source, tx)?;

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
        if !mm.create_table(name, &schema, tx) {
            Err("Failed to create table".to_string())
        } else {
            Ok(0)
        }
    }
}

impl UpdatePlanner for IndexUpdatePlanner {
    fn execute_insert(
        &mut self,
        insert: &InsertNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let table_name = &insert.0;
        let layout = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_table_layout(table_name, &tx)
            .ok_or(format!("table '{}' does not exist", table_name))?;

        let mut table_plan: Box<dyn Plan> = {
            let mut lmm = self.metadata_mgr.write().unwrap();
            Box::new(TablePlan::new(tx.clone(), table_name, &mut lmm))
        };

        let mut table_scan = table_plan.open();
        table_scan.insert();
        let rid = table_scan.get_rid();

        // Insert into any indexes that exist on columns
        let column_indexes = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_index_info(table_name, tx.clone());

        for (name, val) in insert.fields() {
            table_scan.set_val(name, val);

            if let Some(ii) = column_indexes.get(name) {
                let mut index = ii.open();
                index.insert(val, rid.clone());
                index.close()
            }
        }

        Ok(1)
    }

    fn execute_delete(
        &mut self,
        delete: &DeleteNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let table_name = &delete.0;
        let layout = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_table_layout(table_name, &tx)
            .ok_or(format!("table '{}' does not exist", table_name))?;

        let mut plan: Box<dyn Plan> = {
            let mut lmm = self.metadata_mgr.write().unwrap();
            Box::new(TablePlan::new(tx.clone(), table_name, &mut lmm))
        };

        if let Some(pred) = &delete.1 {
            plan = Box::new(SelectPlan::new(plan, pred.clone()));
        }

        let mut scan = plan.open();

        let column_indexes = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_index_info(table_name, tx.clone());

        let mut count = 0;
        while scan.next() {
            let rid = scan.get_rid();

            // delete any index entries
            for (col_name, ii) in column_indexes.iter() {
                let val = scan.get_val(col_name).expect(&format!(
                    "column '{}' is indexed on but does not exist in base table",
                    col_name
                ));
                ii.open().delete(&val, &rid);
            }

            scan.delete();
            count += 1;
        }

        Ok(count)
    }

    fn execute_modify(
        &mut self,
        modify: &UpdateNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        let table_name = &modify.id;
        let field_name = &modify.field;

        let layout = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_table_layout(table_name, &tx)
            .ok_or(format!("table '{}' does not exist", table_name))?;

        let mut plan: Box<dyn Plan> = {
            let mut lmm = self.metadata_mgr.write().unwrap();
            Box::new(TablePlan::new(tx.clone(), table_name, &mut lmm))
        };

        if let Some(pred) = &modify.where_clause {
            plan = Box::new(SelectPlan::new(plan, pred.clone()));
        }

        let ii = self
            .metadata_mgr
            .read()
            .unwrap()
            .get_index_info(table_name, tx.clone());

        let mut idx = ii.get(field_name).map(|i| i.open());

        let mut count = 0;
        let mut scan = plan.open();
        scan.before_first();
        while scan.next() {
            let newval = modify.expr.evaluate(&scan);

            let oldval = scan
                .get_val(field_name)
                .map_err(|err| "field not found in scan".to_string())?;

            scan.set_val(field_name, &newval);

            // If an index exists on this column, it must be updated
            if let Some(idx) = &mut idx {
                let rid = scan.get_rid();
                idx.delete(&oldval, &rid);
                idx.insert(&newval, rid);
            }

            count += 1;
        }

        Ok(count)
    }

    fn execute_create(
        &mut self,
        create: &CreateNode,
        tx: Arc<Mutex<Tx>>,
    ) -> Result<RowCount, String> {
        match create {
            CreateNode::Table(name, fields) => self.create_table(name, fields, &tx),
            CreateNode::View(name, select) => self.create_view(name, select, &tx),
            CreateNode::Index(name, tblname, fieldname) => {
                self.create_index(name, tblname, fieldname, &tx)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::layout::Layout;
    use crate::{
        assert_table_scan_results, make_schema,
        parser::parser::{parse, RootNode},
        planning::update_planner::UpdatePlanner,
        table_scan::TableScan,
        tests::test_utils::{create_default_tables, test_db},
    };

    use super::IndexUpdatePlanner;

    #[test]
    fn test_plan_insert_statement() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = IndexUpdatePlanner::new(mm.clone());

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

            assert_table_scan_results![
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
    fn test_plan_delete_all_statement() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = IndexUpdatePlanner::new(mm.clone());

        // Delete with no predicate to remove everything from the table
        if let Ok(RootNode::Delete(del)) = parse("DELETE FROM student") {
            let tx = Arc::new(Mutex::new(db.new_tx()));
            let count = planner
                .execute_delete(&del, tx.clone())
                .expect("failed to execute delete statement");

            assert_eq!(count, 9);

            let mut scan = TableScan::new(
                tx.clone(),
                mm.read().unwrap().get_table_layout("student", &tx).unwrap(),
                "student",
            );

            assert_table_scan_results![scan,];
        } else {
            panic!("failed to parse delete statement");
        }
    }

    #[test]
    fn test_plan_delete_statement() {
        let testdir = tempdir().unwrap();
        let mut db = test_db(&testdir);
        create_default_tables(&mut db);
        let mm = db.metadata_manager();
        let mut planner = IndexUpdatePlanner::new(mm.clone());

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

            assert_table_scan_results![
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
        let mut planner = IndexUpdatePlanner::new(mm.clone());

        if let Ok(RootNode::Update(update)) =
            parse("UPDATE student SET major_id=5 WHERE major_id=20 AND grad_year=2020")
        {
            let tx = Arc::new(Mutex::new(db.new_tx()));
            let count = planner
                .execute_modify(&update, tx.clone())
                .expect("failed to execute update statement");

            // Assert that 1 row was updated
            assert_eq!(count, 2);

            let mut scan = TableScan::new(
                tx.clone(),
                mm.read().unwrap().get_table_layout("student", &tx).unwrap(),
                "student",
            );

            assert_table_scan_results![
                scan,
                (1, "joe", 2021, 10),
                (2, "amy", 2020, 5),
                (3, "max", 2022, 10),
                (4, "sue", 2022, 20),
                (5, "bob", 2020, 30),
                (6, "kim", 2020, 5),
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
        let mut planner = IndexUpdatePlanner::new(mm.clone());

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
        let mut planner = IndexUpdatePlanner::new(mm.clone());

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

    //#[test]
    //fn test_plan_create_index() {}
}
