use std::sync::{Arc, Mutex};

use tracing::trace;

use crate::{block_id::BlockId, layout::Layout, parser::constant::Value, transaction::Tx};

use super::btree_page::{BTPage, LeafBlockNum};

pub struct DirectoryEntry {
    data_val: Value,
    blk_num: u64,
}

impl DirectoryEntry {
    pub fn new(first_val: &Value, blk_num: u64) -> Self {
        Self {
            data_val: first_val.clone(),
            blk_num,
        }
    }
}

pub struct BTreeDirectory {
    tx: Arc<Mutex<Tx>>,
    layout: Layout,
    contents: BTPage,
    filename: String,
}

impl BTreeDirectory {
    pub fn new(tx: Arc<Mutex<Tx>>, blk: &BlockId, layout: Layout) -> Self {
        Self {
            tx: tx.clone(),
            contents: BTPage::new(tx, blk.clone(), layout.clone()),
            layout: layout.clone(),
            filename: blk.file_id().to_string(),
        }
    }

    /// Search for the leaf node that contains the provided key in directory nodes and return the
    /// block number of the leaf node.
    pub fn search(&mut self, key: &Value) -> LeafBlockNum {
        trace!("Searching for leaf with key: {}", key);
        let mut childblk = self.find_child_block(&key);
        while self.contents.get_flag() > 0 {
            self.contents = BTPage::new(self.tx.clone(), childblk, self.layout.clone());
            childblk = self.find_child_block(&key);
        }

        trace!("Found leaf at blocknum: {}", childblk.num());
        childblk.num() as i32
    }

    pub fn make_new_root(&mut self, entry: &DirectoryEntry) {
        let first_val = self.contents.get_data_val(0);
        let level = self.contents.get_flag();

        // transfer all records to new block
        let newblk = self.contents.split(0, level);
        let oldroot = DirectoryEntry::new(&first_val, newblk.num());
        self.insert_entry(&oldroot);
        self.insert_entry(&entry);
        self.contents.set_flag(level + 1);
    }

    pub fn insert(&mut self, entry: &DirectoryEntry) -> Option<DirectoryEntry> {
        if self.contents.get_flag() == 0 {
            // We are at a directory page that points to leaf nodes (level 0)
            return self.insert_entry(&entry);
        }

        let childblk = self.find_child_block(&entry.data_val);
        let new_entry = {
            let mut child = BTreeDirectory::new(self.tx.clone(), &childblk, self.layout.clone());
            child.insert(&entry)
        };

        if let Some(new_entry) = new_entry {
            self.insert_entry(&new_entry)
        } else {
            None
        }
    }

    fn insert_entry(&mut self, entry: &DirectoryEntry) -> Option<DirectoryEntry> {
        let newslot = (1 + self.contents.find_slot_before(&entry.data_val)) as u32;
        self.contents
            .insert_dir(newslot, &entry.data_val, entry.blk_num as i32);

        if !self.contents.is_full() {
            None
        } else {
            // The page is full, split it pre-emptively
            let level = self.contents.get_flag();
            let split_pos = self.contents.get_num_records() / 2;
            let split_val = self.contents.get_data_val(split_pos);
            let newblk = self.contents.split(split_pos, level);
            Some(DirectoryEntry::new(&split_val, newblk.num()))
        }
    }

    fn find_child_block(&self, key: &Value) -> BlockId {
        let mut slot = self.contents.find_slot_before(&key);
        if self.contents.get_data_val((slot + 1) as u32) == *key {
            slot += 1;
        }

        let blknum = self.contents.get_child_num(slot as u32);
        BlockId::new(&self.filename, blknum as u64)
    }
}
