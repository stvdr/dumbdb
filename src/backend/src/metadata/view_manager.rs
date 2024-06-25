use std::sync::{Arc, Mutex};

use crate::{
    layout::Layout, scan::scan::Scan, schema::Schema, table_scan::TableScan,
    transaction::Transaction,
};

use super::table_manager::TableManager;

const MAX_VIEW_LENGTH: usize = 256;

pub struct ViewManager {
    tbl_mgr: TableManager,
}

impl ViewManager {
    fn create_metadata(&self, tx: &Arc<Mutex<Transaction>>) {
        if self.tbl_mgr.get_table_layout("viewcat", tx).is_some() {
            // If the metadata table already exists, we don't need to re-create it
            return;
        }

        let mut schema = Schema::new();
        schema.add_string_field("viewname", super::table_manager::MAX_NAME);
        schema.add_string_field("viewdef", MAX_VIEW_LENGTH as u64);
        self.tbl_mgr.create_table("viewcat", &schema, tx);
    }

    /// Create a new ViewManager. This method will create the backing metadata tables if they do
    /// not already exist.
    ///
    /// Note: This assumes that TableManager's metadata tables have already been created elsewhere.
    pub fn new(tx: &Arc<Mutex<Transaction>>) -> Self {
        let s = Self {
            tbl_mgr: TableManager::new(tx),
        };

        s.create_metadata(tx);
        s
    }

    /// Create a new view.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view.
    /// * `view_def` - The SQL definition of the view.
    /// * `tx` - The transaction where the view creation will run.
    // TODO: error checking
    pub fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        let layout = self
            .tbl_mgr
            .get_table_layout("viewcat", tx)
            .ok_or("view does not exist")?;
        let mut scan = TableScan::new(tx.clone(), layout, "viewcat");
        scan.insert();
        scan.set_string("viewname", view_name);
        scan.set_string("viewdef", view_def);
        Ok(())
    }

    /// Get a view's SQL definition.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view.
    /// * `tx` - The transaction to use when retrieving the view from the metadata tables.
    pub fn get_view_definition(
        &self,
        view_name: &str,
        tx: &Arc<Mutex<Transaction>>,
    ) -> Option<String> {
        let layout = self.tbl_mgr.get_table_layout("viewcat", tx)?;
        let mut scan = TableScan::new(tx.clone(), layout, "viewcat");
        while scan.next() {
            if scan
                .get_string("viewname")
                .expect("viewname does not exist in the metadata catalog")
                == view_name
            {
                return Some(
                    scan.get_string("viewdef")
                        .expect("viewdef does not exist in the metadata catalog"),
                );
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{tempdir, TempDir};

    use crate::tests::test_utils::test_db;

    use super::*;

    #[test]
    fn test_create_view() {
        let td = tempdir().unwrap();

        let db = test_db(&td);

        {
            let tx = &Arc::new(Mutex::new(db.new_tx()));

            // create a `TableManager` so that the table metadata is initialized
            let tbl_manager = TableManager::new(tx);
            let view_manager = ViewManager::new(tx);
            view_manager
                .create_view("view_test_1", "SELECT * FROM test_table_1;", tx)
                .expect("view_test_1 not found");
            view_manager
                .create_view("view_test_2", "SELECT * FROM test_table_2;", tx)
                .expect("view_test_2 not found");

            tx.lock().unwrap().commit();
        }

        // Verify that the view definitions can be read in a different transaction
        let tx = &Arc::new(Mutex::new(db.new_tx()));
        let view_manager = ViewManager::new(tx);
        let view1 = view_manager.get_view_definition("view_test_1", tx).unwrap();
        let view2 = view_manager.get_view_definition("view_test_2", tx).unwrap();
        let view3 = view_manager.get_view_definition("view_test_3", tx);

        assert_eq!(view1, "SELECT * FROM test_table_1;");
        assert_eq!(view2, "SELECT * FROM test_table_2;");
        assert_eq!(view3, None);
    }
}
