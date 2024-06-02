use std::{
    collections::HashMap,
    iter::Scan,
    sync::{Arc, Mutex},
};

use crate::{layout::Layout, schema::Schema, table_scan::TableScan, transaction::Transaction};

const MAX_NAME: u64 = 16;

struct TableManager {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

impl TableManager {
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

    pub fn from_existing() -> Self {
        let (tcat_layout, fcat_layout) = Self::create_layouts();
        Self {
            tcat_layout,
            fcat_layout,
        }
    }

    pub fn new<const P: usize>(tx: Arc<Mutex<Transaction<P>>>) -> Self {
        let mut sel = Self::from_existing();

        sel.create_table("tblcat", &sel.tcat_layout.schema(), tx.clone());
        sel.create_table("fieldcat", &sel.fcat_layout.schema(), tx.clone());

        sel
    }

    pub fn create_table<const P: usize>(
        &self,
        tbl_name: &str,
        schema: &Schema,
        tx: Arc<Mutex<Transaction<P>>>,
    ) {
        let layout = Layout::from_schema(schema.clone());

        {
            let mut scan = TableScan::new(tx.clone(), layout.clone(), "tblcat");
            scan.insert();
            scan.set_string("tblname", tbl_name);
            scan.set_int("slotsize", layout.slot_size() as i32);
        }

        // TODO: error checking
        {
            let mut scan = TableScan::new(tx, layout.clone(), "fieldcat");
            for field in layout.schema().fields() {
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
                scan.set_int("offset", layout.offset(&field) as i32);
            }
        }
    }

    pub fn get_table_layout<const P: usize>(
        &self,
        tbl_name: &str,
        tx: Arc<Mutex<Transaction<P>>>,
    ) -> Option<Layout> {
        let mut schema = Schema::new();
        let mut slot_size = None;
        {
            let mut scan = TableScan::new(tx.clone(), self.tcat_layout.clone(), "tablecat");
            while scan.next() {
                if scan.get_string("tblname") == tbl_name {
                    slot_size = Some(scan.get_int("slotsize") as u64);
                    break;
                }
            }
        }

        let slot_size = slot_size?;

        let mut offsets = HashMap::new();
        {
            let mut scan = TableScan::new(tx.clone(), self.fcat_layout.clone(), "fieldcat");
            while scan.next() {
                if scan.get_string("tblname") == tbl_name {
                    let field_name = scan.get_string("fldname");
                    let field_type = scan.get_int("type");
                    let field_length = scan.get_int("length") as u64;
                    let field_offset = scan.get_int("offset") as u64;

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
