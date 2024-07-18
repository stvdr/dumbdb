use std::sync::{Arc, Mutex};

use crate::{
    block_id::BlockId, layout::Layout, parser::constant::Value, rid::RID, transaction::Tx,
};

use super::{btree_directory::DirectoryEntry, btree_page::BTPage};

pub struct BTreeLeaf {
    tx: Arc<Mutex<Tx>>,
    layout: Layout,
    search_key: Value,
    contents: BTPage,
    current_slot: i32,
    file_id: String,
}

impl BTreeLeaf {
    pub fn new(tx: Arc<Mutex<Tx>>, blk: BlockId, layout: Layout, search_key: Value) -> Self {
        let file_id = blk.file_id().to_string();
        let contents = BTPage::new(tx.clone(), blk.clone(), layout.clone());
        Self {
            tx,
            layout,
            current_slot: contents.find_slot_before(&search_key),
            search_key,
            contents,
            file_id,
        }
    }

    pub fn next(&mut self) -> bool {
        self.current_slot += 1;
        if self.current_slot >= self.contents.get_num_records() as i32 {
            self.try_overflow()
        } else if self.contents.get_data_val(self.current_slot as u32) == self.search_key {
            true
        } else {
            self.try_overflow()
        }
    }

    pub fn get_data_rid(&self) -> RID {
        assert!(
            self.current_slot >= 0,
            "Must call `next()` before accessing data"
        );
        self.contents.get_data_rid(self.current_slot as u32)
    }

    pub fn delete(&mut self, rid: &RID) {
        while self.next() {
            if self.get_data_rid() == *rid {
                self.contents.delete(self.current_slot as u32);
                return;
            }
        }
    }

    pub fn insert(&mut self, rid: &RID) -> Option<DirectoryEntry> {
        // inserting to the "left"
        if self.contents.get_flag() >= 0
            && let first_val = self.contents.get_data_val(0)
            && first_val > self.search_key
        {
            let newblk = self.contents.split(0, self.contents.get_flag());
            self.current_slot = 0;
            self.contents.set_flag(-1);
            self.contents
                .insert_leaf(self.current_slot as u32, &self.search_key, rid);
            return Some(DirectoryEntry::new(&first_val, newblk.num()));
        }

        // Move to the next slot and insert the RID.
        // At this point, it is guaranteed that the page has space, because splitting happens pre-emptively.
        self.current_slot += 1;
        self.contents
            .insert_leaf(self.current_slot as u32, &self.search_key, rid);

        if !self.contents.is_full() {
            // Page is not full so no new directory page created.
            return None;
        }

        // Page is full, split.
        let first_key = self.contents.get_data_val(0);
        let last_key = self
            .contents
            .get_data_val(self.contents.get_num_records() - 1);

        if first_key == last_key {
            // create an overflow block to hold all but the first record
            let newblk = self.contents.split(1, self.contents.get_flag());
            self.contents.set_flag(newblk.num() as i32);

            // No new directory page created.
            None
        } else {
            // Split in the middle
            let mut split_pos = self.contents.get_num_records() / 2;
            let mut split_key = self.contents.get_data_val(split_pos);

            if split_key == first_key {
                // move to the right and look for next key
                while self.contents.get_data_val(split_pos) == split_key {
                    split_pos += 1;
                }
                split_key = self.contents.get_data_val(split_pos);
            } else {
                // move to the left and look for first entry w/ the key
                while self.contents.get_data_val(split_pos - 1) == split_key {
                    split_pos -= 1;
                }
            }

            let newblk = self.contents.split(split_pos, -1);
            Some(DirectoryEntry::new(&split_key, newblk.num()))
        }
    }

    fn try_overflow(&mut self) -> bool {
        let first_key = self.contents.get_data_val(0);
        let flag = self.contents.get_flag();

        if self.search_key != first_key || flag < 0 {
            return false;
        }

        let nextblk = BlockId::new(&self.file_id, flag as u64);
        self.contents = BTPage::new(self.tx.clone(), nextblk, self.layout.clone());
        self.current_slot = 0;
        true
    }
}
