use std::{
    mem::size_of,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    block_id::BlockId, layout::Layout, parser::constant::Value, rid::RID, transaction::Tx,
};

type Flag = u32;
type RecordCount = u32;

pub type LeafBlockNum = i32;
pub type DirectoryBlockNum = i32;

pub struct BTPage {
    tx: Arc<Mutex<Tx>>,
    current_blk: BlockId,
    layout: Layout,
}

impl BTPage {
    pub fn new(tx: Arc<Mutex<Tx>>, current_blk: BlockId, layout: Layout) -> Self {
        tx.lock().unwrap().pin(&current_blk);
        Self {
            tx,
            current_blk,
            layout,
        }
    }

    pub fn block(&self) -> BlockId {
        self.current_blk.clone()
    }

    pub fn find_slot_before(&self, key: &Value) -> i32 {
        let mut slot = 0;
        while slot < self.get_num_records() && self.get_data_val(slot) < *key {
            slot += 1;
        }

        (slot as i32 - 1)
    }

    pub fn is_full(&self) -> bool {
        self.slot_pos(self.get_num_records() + 1) >= self.tx.lock().unwrap().block_size()
    }

    /// Splits the page into two pages and divides records between them. Half of the records [0..split_pos]
    /// will remain in this page, and the other half [split_pos..] will be moved to a newly created page.
    ///
    /// # Arguments
    /// * `split_pos` - This and all following slots will be transferred to a new page.
    /// * `flag` - The flag that should be set on the new page that is created with ~half of the
    /// records.
    pub fn split(&mut self, split_pos: u32, flag: i32) -> BlockId {
        println!(
            "split leaf at split_pos: {} with flag: {} with cur num records {}",
            split_pos,
            flag,
            self.get_num_records()
        );
        let new_blk = self.append_new(flag);
        let mut new_page = BTPage::new(self.tx.clone(), new_blk.clone(), self.layout.clone());
        self.transfer_records(split_pos, &mut new_page);
        println!("setting flag {} on new page", flag);
        new_page.set_flag(flag);
        new_blk.clone()
    }

    pub fn append_new(&self, flag: i32) -> BlockId {
        let mut txl = self.tx.lock().unwrap();

        let blk = txl.append(self.current_blk.file_id());
        txl.pin(&blk);
        self.format(&blk, flag, &mut txl);
        blk
    }

    pub fn format(&self, blk: &BlockId, flag: i32, tx: &mut Tx) {
        tx.set_int(&blk, 0, flag as i32, false);
        tx.set_int(&blk, size_of::<Flag>(), 0, false);
        let recsize = self.layout.slot_size();
    }

    pub fn get_data_val(&self, slot: u32) -> Value {
        self.get_val(slot, "dataval")
    }

    pub fn get_val(&self, slot: u32, field: &str) -> Value {
        let typ = self.layout.schema().get_field_type(field);
        match typ {
            None => panic!("invalid field field name: {}", field),
            Some(0) => Value::Int(self.get_int(slot, field)),
            Some(1) => Value::Varchar(self.get_string(slot, field)),
            _ => panic!("unrecognized schema field type"),
        }
    }

    /// Set the number of records currently stored in this page.
    fn set_num_records(&self, n: u32) {
        println!("setting number of records to {}", n);
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.current_blk, size_of::<Flag>(), n as i32, true);
    }

    /// Get the number of records currently stored in the page
    pub fn get_num_records(&self) -> u32 {
        let num_records =
            self.tx
                .lock()
                .unwrap()
                .get_int(&self.current_blk, size_of::<Flag>() as usize) as u32;

        println!("got num records: {}", num_records);

        num_records
    }

    pub fn set_flag(&self, val: i32) {
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.current_blk, 0, val as i32, true);
    }

    pub fn get_flag(&self) -> i32 {
        self.tx.lock().unwrap().get_int(&self.current_blk, 0)
    }

    /// Delete the record at the provided slot by shifting all records [slot+1..] to the left by 1.
    pub fn delete(&mut self, slot: u32) {
        let mut i = slot + 1;
        while i < self.get_num_records() {
            self.copy_record(i, i - 1);
            i += 1;
        }

        let num_recs = self.get_num_records();
        self.set_num_records(num_recs - 1);
    }

    /// TODO: only called by btreedir
    /// Gets the num of the BlockId at the specified slot.
    pub fn get_child_num(&self, slot: u32) -> i32 {
        self.get_int(slot, "block")
    }

    /// TODO: only called by btreedir
    pub fn insert_dir(&mut self, slot: u32, val: &Value, blknum: i32) {
        self.insert(slot);
        self.set_val(slot, "dataval", val);
        self.set_int(slot, "block", blknum);
    }

    /// TODO: only called by btreeleaf
    pub fn get_data_rid(&self, slot: u32) -> RID {
        let block = self.get_int(slot, "block");
        let id = self.get_int(slot, "id");
        RID::new(block as u64, id as i16)
    }

    /// TODO: only called by btreeleaf
    /// Inserts a value into a BTree leaf page.
    pub fn insert_leaf(&mut self, slot: u32, val: &Value, rid: &RID) {
        self.insert(slot);
        self.set_val(slot, "dataval", val);
        self.set_int(slot, "block", rid.block_num() as i32);
        self.set_int(slot, "id", rid.slot() as i32);
    }

    /// Transfers all records starting at `slot` (inclusive) to the specified destination page.
    ///
    /// # Arguments
    ///
    /// * `slot` - The first slot that will be transferred.
    /// * `dest` - The destination page where records will be transferred.
    fn transfer_records(&mut self, slot: u32, dest: &mut BTPage) {
        println!("transferring records starting at slot {}", slot);
        let mut slot = slot;
        let mut dest_slot = 0;
        println!(
            "slot: {}, dest_slot: {}, num_recs: {}",
            slot,
            dest_slot,
            self.get_num_records()
        );
        while slot < self.get_num_records() {
            println!("inserting into destination at slot {}", dest_slot);
            dest.insert(dest_slot);
            let schema = self.layout.schema();
            for fld in schema.fields() {
                dest.set_val(dest_slot, &fld, &self.get_val(slot, &fld));
            }
            println!("deleting slot {}", slot);
            self.delete(slot);
            dest_slot += 1;
        }
    }

    /// Inserts a new record into the page at the provided slot by first shifting all records
    /// in [slot..] to the right by 1.
    fn insert(&mut self, slot: u32) {
        println!("inserting at slot {}", slot);
        let mut i = self.get_num_records();
        while i > slot {
            self.copy_record(i - 1, i);
            i -= 1;
        }
        let num_recs = self.get_num_records();
        self.set_num_records(num_recs + 1);
    }

    fn copy_record(&mut self, from: u32, to: u32) {
        let schema = self.layout.schema();
        for field in schema.fields() {
            let existing_val = self.get_val(from, &field);
            self.set_val(to, &field, &existing_val);
        }
    }

    /// Get the raw position of a field in a slot in the page.
    fn field_pos(&self, slot: u32, field: &str) -> usize {
        let offset = self.layout.offset(field);
        self.slot_pos(slot) + offset as usize
    }

    /// Get the raw position of a slot in the page.
    fn slot_pos(&self, slot: u32) -> usize {
        let slot_size = self.layout.slot_size();
        size_of::<RecordCount>() * 2 + (slot as usize * slot_size as usize)
    }

    fn get_int(&self, slot: u32, field: &str) -> i32 {
        let pos = self.field_pos(slot, field);
        self.tx.lock().unwrap().get_int(&self.current_blk, pos)
    }

    fn get_string(&self, slot: u32, field: &str) -> String {
        let pos = self.field_pos(slot, field);
        self.tx.lock().unwrap().get_string(&self.current_blk, pos)
    }

    fn set_int(&self, slot: u32, field: &str, val: i32) {
        let pos = self.field_pos(slot, field);
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.current_blk, pos, val, true);
    }

    fn set_string(&self, slot: u32, field: &str, val: &str) {
        let pos = self.field_pos(slot, field);
        self.tx
            .lock()
            .unwrap()
            .set_string(&self.current_blk, pos, val, true);
    }

    fn set_val(&self, slot: u32, field: &str, val: &Value) {
        let pos = self.field_pos(slot, field);
        match val {
            Value::Int(v) => self.set_int(slot, field, *v),
            Value::Varchar(v) => self.set_string(slot, field, v),
        }
    }

    pub fn close(&self) {
        self.tx.lock().unwrap().unpin(&self.current_blk);
    }
}

impl Drop for BTPage {
    fn drop(&mut self) {
        self.close();
    }
}
