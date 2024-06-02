use std::sync::{Arc, Mutex};

use crate::{file_manager::BlockId, layout::Layout, transaction::Transaction};

// TODO: slot should be a type

const EMPTY: i32 = 0;
const USED: i32 = 1;

pub struct RecordPage<const P: usize> {
    tx: Arc<Mutex<Transaction<P>>>,
    blk: BlockId,
    layout: Layout,
}

impl<const P: usize> RecordPage<P> {
    pub fn new(tx: Arc<Mutex<Transaction<P>>>, blk: BlockId, layout: Layout) -> Self {
        tx.lock().unwrap().pin(&blk);

        Self {
            tx: tx.clone(),
            blk,
            layout,
        }
    }

    /// Get the block number of the underlying page.
    pub fn block_number(&self) -> u64 {
        self.blk.num()
    }

    /// Get an integer value from a field.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to take the value from.
    /// * `field_name` - The field to read the i32 from.
    pub fn get_int(&self, slot: i16, field_name: &str) -> i32 {
        assert!(
            self.get_flag(slot) == USED,
            "the specified slot {} is not marked USED",
            slot
        );

        let pos = self.offset(slot) + self.layout.offset(field_name) as usize;
        self.tx.lock().unwrap().get_int(&self.blk, pos)
    }

    /// Get a String value from a field.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to take the value from.
    /// * `field_name` - The field to read the String from.
    pub fn get_string(&self, slot: i16, field_name: &str) -> String {
        assert!(
            self.get_flag(slot) == USED,
            "the specified slot {} is not marked USED",
            slot
        );

        let pos = self.offset(slot) + self.layout.offset(field_name) as usize;
        self.tx.lock().unwrap().get_string(&self.blk, pos)
    }

    /// Set an integer field in a slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot where the field will be set.
    /// * `field_name` - The name of the field to set.
    /// * `val` - The integer value.
    pub fn set_int(&mut self, slot: i16, field_name: &str, val: i32) {
        assert!(
            self.get_flag(slot) == USED,
            "the specified slot {} is not marked USED",
            slot
        );

        let pos = self.offset(slot) + self.layout.offset(field_name) as usize;
        self.tx.lock().unwrap().set_int(&self.blk, pos, val, true);
    }

    /// Set string field in a slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot where the field will be set.
    /// * `field_name` - The name of the field to set.
    /// * `val` - The string value.
    pub fn set_string(&mut self, slot: i16, field_name: &str, val: &str) {
        assert!(
            self.get_flag(slot) == USED,
            "the specified slot {} is not marked USED",
            slot
        );

        let pos = self.offset(slot) + self.layout.offset(field_name) as usize;
        self.tx
            .lock()
            .unwrap()
            .set_string(&self.blk, pos, val, true);
    }

    /// Mark the specified slot as empty.
    pub fn delete(&mut self, slot: i16) {
        self.set_flag(slot, EMPTY);
    }

    /// Get the `RecordPage`'s underlying `BlockId`.
    pub fn block(&self) -> BlockId {
        self.blk.clone()
    }

    /// Format the RecordPage so that all slots are empty with default values.
    pub fn format(&mut self) {
        let mut slot = 0i16;
        while self.is_valid_slot(slot) {
            self.tx
                .lock()
                .unwrap()
                .set_int(&self.blk, self.offset(slot), EMPTY, false);

            let schema = self.layout.schema();
            for field_name in schema.fields().iter() {
                let field_pos = self.offset(slot) + self.layout.offset(field_name) as usize;
                // TODO: terrible!
                match schema.get_field_type(field_name) {
                    Some(0) => self
                        .tx
                        .lock()
                        .unwrap()
                        .set_int(&self.blk, field_pos, 0, false),
                    Some(1) => self
                        .tx
                        .lock()
                        .unwrap()
                        .set_string(&self.blk, field_pos, "", false),
                    _ => panic!("Unsupported schema field type"),
                }
            }

            slot += 1;
        }
    }

    /// Get the next slot available for insert after the specified slot. The chosen slot will be
    /// updated with a `USED` flag.
    ///
    /// # Arguments
    ///
    /// * `slot` - The search for an EMPTY slot begins directly after this slot.
    pub fn insert_after(&mut self, slot: i16) -> i16 {
        let new_slot = self.search_after(slot, EMPTY);
        if new_slot != -1 {
            self.set_flag(new_slot, USED);
        }
        new_slot
    }

    /// Get the next used slot in the page
    ///
    /// # Arguments
    ///
    /// * `slot` - The search for a USED slot starts directly after this slot.
    pub fn next_after(&self, slot: i16) -> i16 {
        self.search_after(slot, USED)
    }

    // Search for the next slot with the given flag.
    fn search_after(&self, slot: i16, flag: i32) -> i16 {
        let mut slot = slot + 1;
        while self.is_valid_slot(slot) {
            if self.get_flag(slot) == flag {
                return slot;
            }

            slot += 1;
        }

        -1
    }

    // Returns a boolean indicating whether or not the slot fits in a record page.
    fn is_valid_slot(&self, slot: i16) -> bool {
        self.offset(slot + 1) as u64 <= self.tx.lock().unwrap().block_size() as u64
    }

    // TODO: get_string
    //pub fn get_string(&self, slot: u16, field_name: &str) -> i32 {
    //    let pos = self.offset(slot) + self.layout.offset(field_name) as usize;
    //    self.tx.lock().unwrap().get_string(&self.blk, pos)
    //}

    // Set the flag of the specified slot.
    fn set_flag(&mut self, slot: i16, flag: i32) {
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.blk, self.offset(slot), flag, true);
    }

    // Get the flag of the specified slot.
    fn get_flag(&self, slot: i16) -> i32 {
        self.tx
            .lock()
            .unwrap()
            .get_int(&self.blk, self.offset(slot))
    }

    // get the offset in the page of the specified slot.
    fn offset(&self, slot: i16) -> usize {
        self.layout.slot_size() as usize * slot as usize
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

    fn get_record_page<const P: usize>() -> RecordPage<P> {
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
        let t = Arc::new(Mutex::new(Transaction::new(
            fm.clone(),
            lm.clone(),
            bm.clone(),
            lt,
        )));

        let blk = t.lock().unwrap().append("T");

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::from_schema(schema);
        RecordPage::<P>::new(t.clone(), blk, layout)
    }

    #[test]
    fn test_insert_delete() {
        let mut rp = get_record_page::<4096>();
        let mut slot = -1;

        while slot < 3 {
            slot = rp.insert_after(slot);

            rp.set_int(slot, "A", 10 + slot as i32);
            assert_eq!(rp.get_int(slot, "A"), 10 + slot as i32);

            rp.set_string(slot, "B", &format!("str {}", 20 + slot as i32));
            assert_eq!(
                rp.get_string(slot, "B"),
                format!("str {}", 20 + slot as i32)
            );
        }

        for i in 0..4 {
            rp.delete(i);
        }

        // assert that all slots have been deleted
        assert_eq!(rp.next_after(-1), -1);
    }

    #[test]
    fn test_delete_and_insert_middle() {
        let mut rp = get_record_page::<4096>();
        let mut slot = -1;

        // insert at 0, 1, 2
        while slot < 2 {
            slot = rp.insert_after(slot);

            rp.set_int(slot, "A", 10 + slot as i32);
            assert_eq!(rp.get_int(slot, "A"), 10 + slot as i32);

            rp.set_string(slot, "B", &format!("str {}", 20 + slot as i32));
            assert_eq!(
                rp.get_string(slot, "B"),
                format!("str {}", 20 + slot as i32)
            );
        }

        // delete middle slot
        rp.delete(1);

        // verify that slot 1 is identified as insertable
        assert_eq!(rp.insert_after(-1), 1);

        // verify setting data at the slot
        rp.set_int(1, "A", 42);
        rp.set_string(1, "B", "new str");
        assert_eq!(rp.get_int(1, "A"), 42);
        assert_eq!(rp.get_string(1, "B"), "new str");
    }

    #[test]
    fn test_format() {
        let mut rp = get_record_page::<4096>();
        let mut slot = -1;

        // insert at 0, 1, 2
        while slot < 2 {
            slot = rp.insert_after(slot);

            rp.set_int(slot, "A", 10 + slot as i32);
            assert_eq!(rp.get_int(slot, "A"), 10 + slot as i32);

            rp.set_string(slot, "B", &format!("str {}", 20 + slot as i32));
            assert_eq!(
                rp.get_string(slot, "B"),
                format!("str {}", 20 + slot as i32)
            );
        }

        rp.format();
        // Nothing should be marked used now
        assert_eq!(rp.next_after(-1), -1);
    }
}
