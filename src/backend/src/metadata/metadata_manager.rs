use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{layout::Layout, schema::Schema, transaction::Tx};

use super::{
    index_manager::{IndexInfo, IndexManager},
    stats_manager::{StatisticsInfo, StatisticsManager},
    table_manager::TableManager,
    view_manager::ViewManager,
};

pub struct MetadataManager {
    idx_mgr: IndexManager,
    stat_mgr: Arc<Mutex<StatisticsManager>>,
    tbl_mgr: TableManager,
    view_mgr: ViewManager,
}

impl MetadataManager {
    pub fn new(tx: &Arc<Mutex<Tx>>) -> Self {
        let stat_mgr = Arc::new(Mutex::new(StatisticsManager::new(tx)));

        Self {
            idx_mgr: IndexManager::new(stat_mgr.clone(), tx),
            stat_mgr,
            tbl_mgr: TableManager::new(tx),
            view_mgr: ViewManager::new(tx),
        }
    }

    /// Create a new table in the metadata catalogs. Returns boolean indicating whether or not the
    /// table was successfully created.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table.
    /// * `schema` - The table's schema.
    /// * `tx` - The transaction that table creation will run inside of.
    pub fn create_table(&self, tbl_name: &str, schema: &Schema, tx: &Arc<Mutex<Tx>>) -> bool {
        self.tbl_mgr.create_table(tbl_name, schema, tx)
    }

    /// Get a table's layout from the metadata catalogs.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table.
    /// * `tx` - The transaction used to read the table from metadata tables.
    pub fn get_table_layout(&self, tbl_name: &str, tx: &Arc<Mutex<Tx>>) -> Option<Layout> {
        self.tbl_mgr.get_table_layout(tbl_name, tx)
    }

    /// Create a new view in the metadata catalogs.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view.
    /// * `view_def` - The SQL definition of the view.
    /// * `tx` - The transaction that view creation will run inside of.
    pub fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Arc<Mutex<Tx>>,
    ) -> Result<(), String> {
        self.view_mgr.create_view(view_name, view_def, tx)
    }

    /// Get a view definition.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view.
    /// * `tx` - The transaction used to read the view from metadata tables.
    pub fn get_view_def(&self, view_name: &str, tx: &Arc<Mutex<Tx>>) -> Option<String> {
        self.view_mgr.get_view_definition(view_name, tx)
    }

    /// Create an index on a table's field.
    ///
    /// # Arguments
    ///
    /// * `idx_name` - The name of the new index.
    /// * `tbl_name` - The table that has the field to be indexed.
    /// * `field_name` - The name of the field to be indexed.
    /// * `tx` - The transaction used to write necessary metadata.
    pub fn create_index(
        &self,
        idx_name: &str,
        tbl_name: &str,
        field_name: &str,
        tx: &Arc<Mutex<Tx>>,
    ) -> Result<(), String> {
        self.idx_mgr
            .create_index(idx_name, tbl_name, field_name, tx.clone())
    }

    /// Get indexes for the specified table.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table.
    /// * `tx` - The transaction used to read metadata.
    pub fn get_index_info(&self, tbl_name: &str, tx: Arc<Mutex<Tx>>) -> HashMap<String, IndexInfo> {
        self.idx_mgr.get_index_info(tbl_name, tx)
    }

    pub fn get_stat_info(
        &mut self,
        tbl_name: &str,
        layout: &Layout,
        tx: &Arc<Mutex<Tx>>,
    ) -> Option<StatisticsInfo> {
        let mut sm = self.stat_mgr.lock().unwrap();
        sm.get_stats(tbl_name, layout, tx)
    }
}
