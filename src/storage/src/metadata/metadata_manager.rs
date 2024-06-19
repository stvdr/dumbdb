use std::sync::{Arc, Mutex};

use crate::{layout::Layout, schema::Schema, transaction::Transaction};

use super::{
    index_manager::IndexManager,
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
    pub fn new<const P: usize>(tx: &Arc<Mutex<Transaction<P>>>) -> Self {
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
    pub fn create_table<const P: usize>(
        &self,
        tbl_name: &str,
        schema: &Schema,
        tx: &Arc<Mutex<Transaction<P>>>,
    ) -> bool {
        self.tbl_mgr.create_table(tbl_name, schema, tx)
    }

    /// Get a table's layout from the metadata catalogs.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table.
    /// * `tx` - The transaction used to read the table from metadata tables.
    pub fn get_table_layout<const P: usize>(
        &self,
        tbl_name: &str,
        tx: &Arc<Mutex<Transaction<P>>>,
    ) -> Option<Layout> {
        self.tbl_mgr.get_table_layout(tbl_name, tx)
    }

    /// Create a new view in the metadata catalogs.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view.
    /// * `view_def` - The SQL definition of the view.
    /// * `tx` - The transaction that view creation will run inside of.
    pub fn create_view<const P: usize>(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Arc<Mutex<Transaction<P>>>,
    ) -> Result<(), String> {
        self.view_mgr.create_view(view_name, view_def, tx)
    }

    /// Get a view definition.
    ///
    /// # Arguments
    ///
    /// * `view_name` - The name of the view.
    /// * `tx` - The transaction used to read the view from metadata tables.
    pub fn get_view_def<const P: usize>(
        &self,
        view_name: &str,
        tx: &Arc<Mutex<Transaction<P>>>,
    ) -> Option<String> {
        self.view_mgr.get_view_definition(view_name, tx)
    }

    pub fn get_stat_info<const P: usize>(
        &mut self,
        tbl_name: &str,
        layout: &Layout,
        tx: &Arc<Mutex<Transaction<P>>>,
    ) -> Option<StatisticsInfo> {
        let mut sm = self.stat_mgr.lock().unwrap();
        sm.get_stats(tbl_name, layout, tx)
    }

    // TODO: complete index related metadata
    //pub fn create_index<const P: usize>(
    //    &self,
    //    idx_name: &str,
    //    tbl_name: &str,
    //    field_name: &str,
    //    tx: &Arc<Mutex<Transaction<P>>>,
    //) {
    //}
}
