use std::sync::{Arc, Mutex};

use log::Record;

use crate::{
    block_id::BlockId,
    layout::Layout,
    parser::constant::Value,
    record_page::RecordPage,
    rid::RID,
    scan::scan::{Error, Scan, ScanResult},
    transaction::Tx,
};

pub struct TableScan {
    tx: Arc<Mutex<Tx>>,
    layout: Layout,
    record_page: RecordPage,
    file_name: String,
    current_slot: i16,
    is_closed: bool,
}

pub trait SetDynamic {
    fn set_dynamic(&mut self, field: &str, value: &dyn FromDynamic);
}

pub trait FromDynamic {
    fn as_int(&self) -> Option<i32> {
        None
    }
    fn as_string(&self) -> Option<&str> {
        None
    }
}

impl FromDynamic for i32 {
    fn as_int(&self) -> Option<i32> {
        Some(*self)
    }
}

impl FromDynamic for &str {
    fn as_string(&self) -> Option<&str> {
        Some(self)
    }
}

impl FromDynamic for String {
    fn as_string(&self) -> Option<&str> {
        Some(self)
    }
}

impl Scan for TableScan {
    /// Move to the next record.
    ///
    /// Iterate through all records in a table. Each call to `next` will find the next slot with a
    /// valid record in it. Iteration will continue until there are no remaining Record pages.
    fn next(&mut self) -> bool {
        self.current_slot = self.record_page.next_after(self.current_slot);

        while self.current_slot == -1 {
            if self.at_last_block() {
                return false;
            }

            self.move_to_block(self.record_page.block_number() + 1);
            self.current_slot = self.record_page.next_after(self.current_slot);
        }

        true
    }

    fn before_first(&mut self) {
        self.move_to_block(0);
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        if !self.has_field(field_name) {
            Err(Error::NonExistentField(field_name.to_string()))
        } else {
            Ok(self.record_page.get_int(self.current_slot, field_name))
        }
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        if !self.has_field(field_name) {
            Err(Error::NonExistentField(field_name.to_string()))
        } else {
            Ok(self.record_page.get_string(self.current_slot, field_name))
        }
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Value> {
        match self.layout.schema().get_field_type(field_name) {
            Some(0) => self.get_int(field_name).map(Value::Int),
            Some(1) => self.get_string(field_name).map(Value::Varchar),
            _ => Err(Error::NonExistentField(field_name.to_string())),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> ScanResult<()> {
        self.record_page.set_int(self.current_slot, field_name, val);
        Ok(())
    }

    fn set_string(&mut self, field_name: &str, val: &str) -> ScanResult<()> {
        self.record_page
            .set_string(self.current_slot, field_name, val);
        Ok(())
    }

    fn set_val(&mut self, field_name: &str, val: Value) -> ScanResult<()> {
        //self.record_page.set
        match &val {
            Value::Int(i) => self.record_page.set_int(self.current_slot, field_name, *i),
            Value::Varchar(s) => self
                .record_page
                .set_string(self.current_slot, field_name, s),
        }

        Ok(())
    }

    /// Move to the next slot available for insertion and mark it USED.
    ///
    /// If there is no slot available in the current `RecordPage`, creates a new `RecordPage` and
    /// uses the first slot there.
    fn insert(&mut self) -> ScanResult<()> {
        self.current_slot = self.record_page.insert_after(self.current_slot);

        while self.current_slot == -1 {
            if self.at_last_block() {
                self.move_to_new_block();
            } else {
                self.move_to_block(self.record_page.block_number() + 1);
            }

            self.current_slot = self.record_page.insert_after(self.current_slot);
        }

        Ok(())
    }

    fn delete(&mut self) -> ScanResult<()> {
        self.record_page.delete(self.current_slot);

        Ok(())
    }

    fn move_to_rid(&mut self, rid: RID) -> ScanResult<()> {
        self.close();
        let blk = BlockId::new(&self.file_name, rid.block_num());
        self.record_page = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        self.current_slot = rid.slot();

        Ok(())
    }

    fn get_rid(&self) -> ScanResult<RID> {
        Ok(RID::new(self.record_page.block_number(), self.current_slot))
    }

    fn close(&mut self) {
        if !self.is_closed {
            self.tx.lock().unwrap().unpin(&self.record_page.block());
            self.is_closed = true;
        }
    }
}

impl TableScan {
    pub fn new(tx: Arc<Mutex<Tx>>, layout: Layout, file_name: &str) -> Self {
        let blk = {
            let mut ltx = tx.lock().unwrap();
            if ltx.size(file_name) == 0 {
                ltx.append(file_name)
            } else {
                BlockId::new(file_name, 0)
            }
        };

        Self {
            record_page: RecordPage::new(tx.clone(), blk, layout.clone()),
            tx,
            layout,
            file_name: file_name.to_string(),
            current_slot: -1,
            is_closed: false,
        }
    }

    pub fn get_layout(&self) -> &Layout {
        &self.layout
    }

    fn at_last_block(&self) -> bool {
        let num_blocks = self.tx.lock().unwrap().size(&self.file_name);
        self.record_page.block_number() == num_blocks - 1
    }

    fn move_to_new_block(&mut self) {
        self.close();
        let blk = self.tx.lock().unwrap().append(&self.file_name);
        self.record_page = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        self.current_slot = -1;
    }

    fn move_to_block(&mut self, block_num: u64) {
        self.close();
        let blk = BlockId::new(&self.file_name, block_num);
        self.record_page = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        self.current_slot = -1;
    }
}

impl Drop for TableScan {
    fn drop(&mut self) {
        self.close();
    }
}

impl SetDynamic for TableScan {
    fn set_dynamic(&mut self, field: &str, value: &dyn FromDynamic) {
        if let Some(val) = value.as_int() {
            self.set_int(field, val);
        } else if let Some(val) = value.as_string() {
            self.set_string(field, val);
        } else {
            panic!("Unsupported value type");
        }
    }
}

#[macro_export]
macro_rules! insert {
    ($scan:expr, $( ($($val:expr),*) ),*) => {{
        use crate::scan::scan::{Scan};
        use crate::table_scan::{SetDynamic, FromDynamic};
        let fields = $scan.get_layout().schema().fields();
        $scan.before_first();
        $(
            $scan.insert();
            let mut index = 0;
            $(
                $scan.set_dynamic(&fields[index], &$val);
                index += 1;
            )*
        )*

    }};
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::mpsc, thread};

    use tempfile::tempdir;

    use crate::{
        buffer_manager::{self, BufferManager},
        eviction_policy::SimpleEvictionPolicy,
        file_manager::FileManager,
        layout,
        lock_table::LockTable,
        log_manager::LogManager,
        schema::Schema,
    };

    use super::*;

    #[test]
    fn test_simple_scan() {
        let _ = env_logger::try_init();

        let td = tempdir().unwrap();
        let data_dir = td.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let log_dir = td.path().join("log");
        fs::create_dir_all(&log_dir).unwrap();

        let lm = Arc::new(Mutex::new(LogManager::new(&log_dir)));
        let fm = Arc::new(FileManager::new(&data_dir));
        let bm = Arc::new(Mutex::new(BufferManager::new(
            10,
            fm.clone(),
            lm.clone(),
            SimpleEvictionPolicy::new(),
        )));
        let lt = Arc::new(LockTable::new());
        let t = Arc::new(Mutex::new(Tx::new(fm.clone(), lm.clone(), bm.clone(), lt)));

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::from_schema(schema);

        let mut scan = TableScan::new(t.clone(), layout, "T");
        scan.before_first();
        for i in 0..50 {
            scan.insert();
            scan.set_int("A", i);
            scan.set_string("B", &format!("string {}", 49 - i));
        }

        let mut count = 0;
        scan.before_first();
        while scan.next() {
            let a = scan.get_int("A").unwrap();
            let _b = scan.get_string("B").unwrap();

            if a < 10 {
                count += 1;
                scan.delete();
            }
        }

        assert_eq!(count, 10);

        scan.before_first();
        count = 0;
        let mut i = 10;
        while scan.next() {
            let a = scan.get_int("A").unwrap();
            let b = scan.get_string("B").unwrap();
            assert_eq!(i, a);
            assert_eq!(format!("string {}", 49 - i), b);

            // Assert that selecting the values as constants works as expected.
            let a_const = scan.get_val("A").unwrap();
            let b_const = scan.get_val("B").unwrap();
            assert_eq!(a_const, Value::Int(i));
            assert_eq!(b_const, Value::Varchar(format!("string {}", 49 - i)));

            count += 1;
            i += 1;
        }

        assert_eq!(count, 40);

        scan.close();
        t.lock().unwrap().commit();
    }
}
