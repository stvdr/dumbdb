use std::sync::{Arc, Mutex};

use crate::{block_id::BlockId, layout::Layout, make_schema, schema::Schema, transaction::Tx};

use super::{btree_leaf::BTreeLeaf, btree_page::BTPage};

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
        let leaf_tbl = format!("{}-leaf", index_name);
        let dir_tbl = format!("{}-dir", index_name);

        if tx.lock().unwrap().size(&leaf_tbl) == 0 {
            let blk = tx.lock().unwrap().append(&leaf_tbl);
            let node = BTPage::new(tx.clone(), blk, leaf_layout.clone());
        }
        if tx.lock().unwrap().size(leaf_tbl) == 0 {}

        let dir_schema = Schema::new();
        dir_schema.add("block", leaf_layout.schema());
        dir_schema.add("dataval", leaf_layout.schema());
        let dir_layout = Layout::from_schema(dir_schema);

        Self {
            leaf_tbl,
            leaf_layout,
            dir_layout,

            tx,
            //rootblk: BlockId::new(leaftbl)
        }
    }
}
