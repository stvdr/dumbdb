use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    layout::Layout, scan::scan::Scan, schema::Schema, table_scan::TableScan,
    transaction::Transaction,
};

// The maximum length of the name of a table or a table field
pub const MAX_NAME: u64 = 16;

pub struct TableManager {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

impl TableManager {
    /// Create the layouts that are used by metadata tables for storing table and field info.
    fn create_layouts() -> (Layout, Layout) {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field("tblname", MAX_NAME);
        tcat_schema.add_int_field("slotsize");

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field("tblname", MAX_NAME);
        fcat_schema.add_string_field("fldname", MAX_NAME);
        fcat_schema.add_int_field("type");
        fcat_schema.add_int_field("length");
        fcat_schema.add_int_field("offset");

        (
            Layout::from_schema(tcat_schema),
            Layout::from_schema(fcat_schema),
        )
    }

    /// Creates a TableManager with newly defined metadata tables backing it.
    ///
    /// # Arguments
    ///
    /// * `tx` - The transaction to use when creating the backing metadata tables.
    pub fn new(tx: &Arc<Mutex<Transaction>>) -> Self {
        let (tcat_layout, fcat_layout) = Self::create_layouts();
        let mut sel = Self {
            tcat_layout,
            fcat_layout,
        };

        sel.create_table("tblcat", &sel.tcat_layout.schema(), tx);
        sel.create_table("fieldcat", &sel.fcat_layout.schema(), tx);

        sel
    }

    /// Create a new table in the metadata catalog.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table.
    /// * `schema` - The schema of the table.
    /// * `tx` - The transaction to use when inserting into the metadata tables.
    pub fn create_table(
        &self,
        tbl_name: &str,
        schema: &Schema,
        tx: &Arc<Mutex<Transaction>>,
    ) -> bool {
        let new_tbl_layout = Layout::from_schema(schema.clone());

        {
            let mut scan = TableScan::new(tx.clone(), self.tcat_layout.clone(), "tablecat");

            // verify that the table does not already exist
            while scan.next() {
                if scan
                    .get_string("tblname")
                    .expect("tblname does not exist in metadata catalog")
                    == tbl_name
                {
                    return false;
                }
            }

            scan.before_first();
            scan.insert();
            scan.set_string("tblname", tbl_name);
            scan.set_int("slotsize", new_tbl_layout.slot_size() as i32);
        }

        // TODO: error checking
        {
            let mut scan = TableScan::new(tx.clone(), self.fcat_layout.clone(), "fieldcat");
            for field in new_tbl_layout.schema().fields() {
                scan.insert();
                scan.set_string("tblname", tbl_name);
                scan.set_string("fldname", &field);
                scan.set_int(
                    "type",
                    schema.get_field_type(&field).expect("unrecognized field"),
                );
                scan.set_int(
                    "length",
                    schema.get_field_length(&field).expect("unrecognized field") as i32,
                );
                scan.set_int("offset", new_tbl_layout.offset(&field) as i32);
            }
        }

        true
    }

    /// Gets the layout of a table already defined in the metadata catalogs.
    ///
    /// # Arguments
    ///
    /// * `tbl_name` - The name of the table that already exists.
    /// * `tx` - The transaction to use when reading from the metadata tables.
    pub fn get_table_layout(&self, tbl_name: &str, tx: &Arc<Mutex<Transaction>>) -> Option<Layout> {
        let mut schema = Schema::new();
        let mut slot_size = None;
        {
            let mut scan = TableScan::new(tx.clone(), self.tcat_layout.clone(), "tablecat");
            while scan.next() {
                if scan
                    .get_string("tblname")
                    .expect("tblname does not exist in metadata catalog")
                    == tbl_name
                {
                    slot_size = Some(
                        scan.get_int("slotsize")
                            .expect("slotsize column does not exist in metadata catalog")
                            as u64,
                    );
                    break;
                }
            }
        }

        let slot_size = slot_size?;

        let mut offsets = HashMap::new();
        {
            let mut scan = TableScan::new(tx.clone(), self.fcat_layout.clone(), "fieldcat");
            while scan.next() {
                let catalog_tblname = scan
                    .get_string("tblname")
                    .expect("tblname column does not exist in metadata catalog");

                if catalog_tblname == tbl_name {
                    let field_name = scan
                        .get_string("fldname")
                        .expect("fldname column does not exist in metadata catalog");
                    let field_type = scan
                        .get_int("type")
                        .expect("type column does not exist in metadata catalog");
                    let field_length = scan
                        .get_int("length")
                        .expect("length column does not exist in metadata catalog")
                        as u64;
                    let field_offset = scan
                        .get_int("offset")
                        .expect("offset column does not exist in metadata catalog")
                        as u64;

                    offsets.insert(field_name.clone(), field_offset);
                    schema.add_field(&field_name, field_type, field_length);
                }
            }
        }

        if schema.fields().is_empty() {
            None
        } else {
            Some(Layout::new(schema, offsets, slot_size))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::tests::test_utils::test_db;

    use super::*;

    #[test]
    fn test_create_table() {
        let td = tempdir().unwrap();

        let db = test_db(&td);

        // Create first table in the catalog
        let tx = &Arc::new(Mutex::new(db.new_tx()));
        let tbl_manager = TableManager::new(tx);
        let mut schema_1 = Schema::new();
        schema_1.add_int_field("test_int");
        schema_1.add_string_field("test_str", 16);
        assert!(tbl_manager.create_table("test_table", &schema_1, tx));
        assert!(!tbl_manager.create_table("test_table", &schema_1, tx));
        tx.lock().unwrap().commit();

        // Create second table in the catalog
        let tx = &Arc::new(Mutex::new(db.new_tx()));
        let tbl_manager = TableManager::new(tx);
        let mut schema_2 = Schema::new();
        schema_2.add_int_field("test_int_2");
        schema_2.add_int_field("test_int_2_2");
        schema_2.add_string_field("test_str_2", 16);
        schema_2.add_string_field("test_str_2_2", 16);
        assert!(tbl_manager.create_table("test_table_2", &schema_2, tx));
        assert!(!tbl_manager.create_table("test_table_2", &schema_2, tx));
        tx.lock().unwrap().commit();

        // Verify existence of both tables
        let tx = &Arc::new(Mutex::new(db.new_tx()));
        let actual_layout = tbl_manager
            .get_table_layout("test_table", tx)
            .expect("table does not exist");
        let expected_layout = Layout::from_schema(schema_1);
        assert_eq!(expected_layout, actual_layout);

        let actual_layout = tbl_manager
            .get_table_layout("test_table_2", tx)
            .expect("table does not exist");
        let expected_layout = Layout::from_schema(schema_2);
        assert_eq!(expected_layout, actual_layout);

        // Verify non-existence
        assert!(tbl_manager.get_table_layout("does_not_exist", tx).is_none());
    }
}
