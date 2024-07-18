use std::sync::{Arc, Mutex};

use crate::{
    block_id::BlockId, index::index::Index, layout::Layout, make_schema, parser::constant::Value,
    rid::RID, schema::Schema, transaction::Tx,
};

use super::{btree_directory::BTreeDirectory, btree_leaf::BTreeLeaf, btree_page::BTPage};

pub struct BTreeIndex {
    tx: Arc<Mutex<Tx>>,
    dir_layout: Layout,
    leaf_layout: Layout,
    leaf_tbl: String,
    leaf: Option<BTreeLeaf>,
    rootblk: BlockId,
}

impl BTreeIndex {
    pub fn new(tx: Arc<Mutex<Tx>>, index_name: &str, leaf_layout: Layout) -> Self {
        // Intialization of the leaf table
        let leaf_tbl = format!("{}-leaf", index_name);
        // TODO: in the below block we lock the transaction to check the size and then separately
        // lock it to append a new (initial) leaf page. Possible race condition where the size
        // check can happen twice and then two pages are appended?
        if tx.lock().unwrap().size(&leaf_tbl) == 0 {
            let blk = tx.lock().unwrap().append(&leaf_tbl);
            let node = BTPage::new(tx.clone(), blk.clone(), leaf_layout.clone());
            let mut tx = tx.lock().unwrap();
            node.format(&blk, -1, &mut tx);
        }

        // Initialization of the directory table
        let mut dir_schema = Schema::new();
        dir_schema.add_from("block", leaf_layout.schema());
        dir_schema.add_from("dataval", leaf_layout.schema());
        let dir_layout = Layout::from_schema(dir_schema);
        let dir_tbl = format!("{}-dir", index_name);
        let rootblk = BlockId::new(&dir_tbl, 0);
        if tx.lock().unwrap().size(&dir_tbl) == 0 {
            // create and initialize a new root block
            tx.lock().unwrap().append(&dir_tbl);

            // insert initial directory entry
            let fldtype = dir_layout
                .schema()
                .get_field_type("dataval")
                .expect("dataval not found in directory schema");

            // TODO: this is a mess
            let minval = match fldtype {
                0 => Value::Int(i32::MIN),
                1 => Value::Varchar("".to_string()),
                _ => panic!("invalid type specified for the directory schema's dataval field"),
            };

            let mut node = BTPage::new(tx.clone(), rootblk.clone(), dir_layout.clone());
            // NOTE: node doesn't need to be formatted since new blocks initialize everything to 0
            node.insert_dir(0, &minval, 0);
        }

        Self {
            leaf_tbl,
            leaf_layout,
            dir_layout,
            tx,
            rootblk,
            leaf: None,
        }
    }

    pub fn search_cost(num_blocks: usize, rpb: usize) -> i32 {
        1 + ((num_blocks as f64).log10() / (rpb as f64).log10()) as i32
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, search_key: &Value) {
        self.close();
        let blknum = {
            let mut root =
                BTreeDirectory::new(self.tx.clone(), &self.rootblk, self.dir_layout.clone());
            root.search(&search_key)
        };
        let leafblk = BlockId::new(&self.leaf_tbl, blknum as u64);
        self.leaf = Some(BTreeLeaf::new(
            self.tx.clone(),
            leafblk,
            self.leaf_layout.clone(),
            search_key.clone(),
        ));
    }

    fn next(&mut self) -> bool {
        self.leaf.as_mut().map_or(false, BTreeLeaf::next)
    }

    fn get_rid(&self) -> Option<RID> {
        self.leaf.as_ref().map(BTreeLeaf::get_data_rid)
    }

    fn insert(&mut self, key: &Value, rid: RID) {
        self.before_first(&key);
        // NOTE: we know that leaf is set at this point due to the above `before_first` call. Better way to design this?
        let leaf = self.leaf.as_mut().unwrap();
        let e = leaf.insert(&rid);
        self.leaf = None;

        if let Some(e) = e {
            let mut root =
                BTreeDirectory::new(self.tx.clone(), &self.rootblk, self.dir_layout.clone());
            let e2 = root.insert(&e);
            if let Some(e2) = e2 {
                root.make_new_root(&e2);
            }
        }
    }

    fn delete(&mut self, key: &Value, rid: RID) {
        self.before_first(&key);
        let leaf = self.leaf.as_mut().unwrap();
        leaf.delete(&rid);
        self.leaf = None;
    }

    fn close(&mut self) {
        self.leaf = None;
    }
}
