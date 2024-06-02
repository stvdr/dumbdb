use std::sync::{Arc, Mutex};

use log::Record;

use crate::{
    file_manager::BlockId, layout::Layout, record_page::RecordPage, rid::RID,
    transaction::Transaction,
};

pub struct TableScan<const P: usize> {
    tx: Arc<Mutex<Transaction<P>>>,
    layout: Layout,
    record_page: RecordPage<P>,
    file_name: String,
    current_slot: i16,
    is_closed: bool,
}

impl<const P: usize> TableScan<P> {
    pub fn new(tx: Arc<Mutex<Transaction<P>>>, layout: Layout, file_name: &str) -> Self {
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

    /// Move to the next record.
    ///
    /// Iterate through all records in a table. Each call to `next` will find the next slot with a
    /// valid record in it. Iteration will continue until there are no remaining Record pages.
    pub fn next(&mut self) -> bool {
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

    pub fn before_first(&mut self) {
        self.move_to_block(0);
    }

    pub fn get_int(&self, field_name: &str) -> i32 {
        self.record_page.get_int(self.current_slot, field_name)
    }

    pub fn get_string(&self, field_name: &str) -> String {
        self.record_page.get_string(self.current_slot, field_name)
    }

    // TODO
    //pub fn get_val() -> Constant {}
    //

    pub fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }

    pub fn set_int(&mut self, field_name: &str, val: i32) {
        self.record_page.set_int(self.current_slot, field_name, val);
    }

    pub fn set_string(&mut self, field_name: &str, val: &str) {
        self.record_page
            .set_string(self.current_slot, field_name, val);
    }

    // TODO
    //pub fn set_val(&mut self, field_name: &str, val: Constant) { }
    //

    /// Move to the next slot available for insertion and mark it USED.
    ///
    /// If there is no slot available in the current `RecordPage`, creates a new `RecordPage`.
    pub fn insert(&mut self) {
        self.current_slot = self.record_page.insert_after(self.current_slot);

        while self.current_slot == -1 {
            if self.at_last_block() {
                self.move_to_new_block();
            } else {
                self.move_to_block(self.record_page.block_number() + 1);
            }

            self.current_slot = self.record_page.insert_after(self.current_slot);
        }
    }

    pub fn delete(&mut self) {
        self.record_page.delete(self.current_slot);
    }

    pub fn move_to_rid(&mut self, rid: RID) {
        self.close();
        let blk = BlockId::new(&self.file_name, rid.block_num());
        self.record_page = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        self.current_slot = rid.slot();
    }

    pub fn get_rid(&self) -> RID {
        RID::new(self.record_page.block_number(), self.current_slot)
    }

    pub fn close(&mut self) {
        if !self.is_closed {
            self.tx.lock().unwrap().unpin(&self.record_page.block());
            self.is_closed = true;
        }
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

impl<const P: usize> Drop for TableScan<P> {
    fn drop(&mut self) {
        self.close();
    }
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

        let lm = Arc::new(Mutex::new(LogManager::<4096>::new(&log_dir)));
        let fm = Arc::new(FileManager::new(&data_dir));
        let bm = Arc::new(Mutex::new(BufferManager::new(
            10,
            fm.clone(),
            lm.clone(),
            SimpleEvictionPolicy::new(),
        )));
        let lt = Arc::new(LockTable::new());
        let t = Arc::new(Mutex::new(Transaction::new(
            fm.clone(),
            lm.clone(),
            bm.clone(),
            lt,
        )));

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
            let a = scan.get_int("A");
            let _b = scan.get_string("B");

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
            let a = scan.get_int("A");
            let b = scan.get_string("B");
            assert_eq!(i, a);
            assert_eq!(format!("string {}", 49 - i), b);
            count += 1;
            i += 1;
        }

        assert_eq!(count, 40);

        scan.close();
        t.lock().unwrap().commit();
    }
}
