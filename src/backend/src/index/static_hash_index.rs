use std::sync::{Arc, Mutex};

use crate::{
    layout::Layout, parser::constant::Value, rid::RID, scan::scan::Scan, table_scan::TableScan,
    transaction::Tx,
};

use super::index::{Hashable, Index};

// TODO: this is modeled on the API from the text (pg. 321), but it doesn't feel like it fits well
// into Rust. e.g. having the search_key & table_scan be Option types that change after calling
// `before_first` means that most of the method calls are invalid if called before `before_first`.

pub struct StaticHashIndex {
    tx: Arc<Mutex<Tx>>,
    index_name: String,
    num_buckets: u32,
    layout: Layout,
    search_key: Option<Value>,
    table_scan: Option<TableScan>,
}

impl StaticHashIndex {
    pub fn new(num_buckets: u32, tx: Arc<Mutex<Tx>>, index_name: &str, layout: Layout) -> Self {
        Self {
            tx,
            index_name: index_name.to_string(),
            num_buckets,
            layout,
            search_key: None,
            table_scan: None,
        }
    }
}

impl Index for StaticHashIndex {
    fn before_first(&mut self, search_key: &Value) {
        self.close();

        let bucket = search_key.hash() % self.num_buckets as u64;
        self.search_key = Some(search_key.clone());
        let tblname = format!("{}{}", self.index_name, bucket);
        self.table_scan = Some(TableScan::new(
            self.tx.clone(),
            self.layout.clone(),
            &tblname,
        ));
    }

    fn next(&mut self) -> bool {
        let search_key = match &self.search_key {
            Some(k) => k,
            None => return false,
        };

        if let Some(ts) = &mut self.table_scan {
            while ts.next() {
                if let Ok(val) = &ts.get_val("dataval") {
                    return val == search_key;
                }
            }
        }
        false
    }

    fn get_rid(&self) -> Option<crate::rid::RID> {
        if let Some(ts) = &self.table_scan {
            // TODO: store correct integer sizes
            let block = ts.get_int("block").expect("could not read block num");
            let id = ts.get_int("id").expect("could not read id");
            Some(RID::new(block as u64, id as i16))
        } else {
            None
        }
    }

    fn insert(&mut self, key: &Value, rid: crate::rid::RID) {
        self.before_first(key);
        match &mut self.table_scan {
            Some(ts) => {
                ts.insert();
                // TODO: correct integer size
                ts.set_int("block", rid.block_num() as i32);
                ts.set_int("id", rid.slot() as i32);
                ts.set_val("dataval", key);
            }
            None => return,
        }
    }

    fn delete(&mut self, key: &Value, rid: crate::rid::RID) {
        self.before_first(key);
        while self.next() {
            if let Some(r) = self.get_rid()
                && r == rid
            {
                match &mut self.table_scan {
                    Some(ts) => {
                        ts.delete();
                        return;
                    }
                    None => return,
                }
            }
        }
    }

    fn close(&mut self) {
        // Setting to None will cause the `Drop` trait to be utilized to close scan
        self.table_scan = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        index::index::Index, layout::Layout, make_schema, parser::constant::Value, rid::RID,
        schema::Schema, tests::test_utils::test_db,
    };

    use super::StaticHashIndex;

    #[test]
    fn test_static_hash() {
        let td = tempdir().unwrap();
        let db = test_db(&td);

        let tx = Arc::new(Mutex::new(db.new_tx()));

        let schema = make_schema! {
            "block" => i32,
            "id" => i32,
            "dataval" => i32
        };

        let mut idx = StaticHashIndex::new(100, tx, "test_idx", Layout::from_schema(schema));
        idx.insert(&Value::Int(13), RID::new(32, 10));
        idx.insert(&Value::Int(15), RID::new(33, 11));
        idx.insert(&Value::Int(15), RID::new(33, 12));
        idx.insert(&Value::Int(15), RID::new(33, 13));
        idx.insert(&Value::Int(100), RID::new(34, 12));

        idx.before_first(&Value::Int(13));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(32, 10)));
        assert!(!idx.next());

        idx.before_first(&Value::Int(15));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(33, 11)));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(33, 12)));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(33, 13)));
        assert!(!idx.next());

        idx.before_first(&Value::Int(100));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(34, 12)));
        assert!(!idx.next());

        idx.delete(&Value::Int(15), RID::new(33, 12));
        idx.delete(&Value::Int(100), RID::new(34, 12));

        idx.before_first(&Value::Int(13));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(32, 10)));
        assert!(!idx.next());

        idx.before_first(&Value::Int(15));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(33, 11)));
        assert!(idx.next());
        assert_eq!(idx.get_rid(), Some(RID::new(33, 13)));
        assert!(!idx.next());

        idx.before_first(&Value::Int(100));
        assert!(!idx.next());
    }
}
