use std::{
    fs::File,
    io::{self, Write},
    sync::{Arc, Mutex},
};

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

    pub fn generate_dot_file(&self, filename: &str) -> io::Result<()> {
        let mut file = File::create(filename)?;
        writeln!(file, "digraph BTree {{")?;
        writeln!(file, "  node [shape=record, fontsize=10];")?;
        self.write_dot_node(&mut file, &self.rootblk, true)?;
        writeln!(file, "}}")?;
        Ok(())
    }

    fn write_dot_node(&self, file: &mut File, blk: &BlockId, is_dir: bool) -> io::Result<()> {
        let layout = if is_dir {
            self.dir_layout.clone()
        } else {
            self.leaf_layout.clone()
        };
        let mut page = BTPage::new(self.tx.clone(), blk.clone(), layout);
        let flag = page.get_flag();
        let num_records = page.get_num_records();

        if is_dir {
            // Internal node
            write!(
                file,
                "  internal{} [style = filled, fillcolor=orange, label = \"l: {}",
                blk.num(),
                flag
            )?;
            for i in 0..num_records {
                let key = page.get_data_val(i);
                let block_num = page.get_child_num(i);
                write!(file, " | <p{}> {},{}", block_num, block_num, key)?;
            }
            write!(file, "\"];\n")?;

            for i in 0..num_records {
                let child_blk = page.get_child_num(i);
                let child_blk_id = if flag == 0 {
                    // this internal node points to leaves
                    writeln!(
                        file,
                        "  \"internal{}\":p{} -> \"leaf{}\";",
                        blk.num(),
                        child_blk,
                        child_blk
                    )?;
                    BlockId::new(&self.leaf_tbl, child_blk as u64)
                } else {
                    // this internal node points to other internal nodes
                    writeln!(
                        file,
                        "  \"internal{}\" -> \"internal{}\";",
                        blk.num(),
                        child_blk
                    )?;
                    BlockId::new(&self.rootblk.file_id(), child_blk as u64)
                };

                self.write_dot_node(file, &child_blk_id, flag > 0)?;
            }
        } else {
            // leaf (non-overflow)
            let overflow_blk = page.get_flag();
            write!(
                file,
                "  leaf{} [style = filled, fillcolor = lightblue, label = \"<p{}> f: {}",
                blk.num(),
                overflow_blk,
                overflow_blk,
            )?;
            for i in 0..num_records {
                let key = page.get_data_val(i);
                let rid = page.get_data_rid(i);
                write!(
                    file,
                    " | {} (RID: {}, {})",
                    key,
                    rid.block_num(),
                    rid.slot()
                )?;
            }
            writeln!(file, "\"];\n")?;

            if overflow_blk != -1 {
                // point to an overflow page
                writeln!(
                    file,
                    "  \"leaf{}\":p{} -> \"leaf{}\";",
                    blk.num(),
                    overflow_blk,
                    overflow_blk,
                )?;

                let overflow_blockid = BlockId::new(&self.leaf_tbl, overflow_blk as u64);
                self.write_dot_node(file, &overflow_blockid, false)?;
            }
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        index::{btree::btree_index::BTreeIndex, index::Index},
        layout::Layout,
        make_schema,
        parser::constant::Value,
        rid::RID,
        tests::test_utils::test_db,
    };

    #[test]
    fn test_btree_index_no_dupes() {
        let dir = tempdir().unwrap();
        let db = test_db(&dir);

        let tx = Arc::new(Mutex::new(db.new_tx()));

        let leaf_layout = Layout::from_schema(make_schema! {
            "dataval" => i32,
            "block" => i32,
            "id" => i32
        });

        let mut index = BTreeIndex::new(tx, "test-idx", leaf_layout);

        let num_recs = 50;

        for i in 0..num_recs {
            println!("inserting");
            index.insert(&Value::Int(i), RID::new(i as u64 / 100, i as i16 % 100));
        }

        for i in 0..num_recs {
            index.before_first(&Value::Int(i));
            assert!(index.next());
            assert_eq!(
                index.get_rid(),
                Some(RID::new(i as u64 / 100, i as i16 % 100))
            );
        }

        // Test deletions
        for i in 0..num_recs {
            if i % 5 == 0 {
                index.delete(&Value::Int(i), RID::new(i as u64 / 100, i as i16 % 100));
            }
        }

        index.generate_dot_file("graph.dot");

        for i in 0..num_recs {
            index.before_first(&Value::Int(i));

            if i % 5 == 0 {
                assert!(!index.next());
            } else {
                assert!(index.next());
                assert_eq!(
                    index.get_rid(),
                    Some(RID::new(i as u64 / 100, i as i16 % 100))
                );
                assert!(!index.next());
            }
        }
    }

    #[test]
    fn test_btree_index_duplicates() {
        let dir = tempdir().unwrap();
        let db = test_db(&dir);

        let tx = Arc::new(Mutex::new(db.new_tx()));

        let leaf_layout = Layout::from_schema(make_schema! {
            "dataval" => i32,
            "block" => i32,
            "id" => i32
        });

        let mut index = BTreeIndex::new(tx, "test-idx", leaf_layout);

        for i in 0..25 {
            //add duplicate values
            if i % 6 == 0 {
                for j in 0..8 {
                    //println!("inserting {}", i * 100 + j);
                    index.insert(&Value::Int(i), RID::new(i as u64, 0 as i16));
                }
            } else {
                index.insert(&Value::Int(i), RID::new(i as u64, 0));
            }
        }

        index.generate_dot_file("graph.dot");

        for i in 0..25 {
            index.before_first(&Value::Int(i));

            let repetitions = if i % 6 == 0 { 8 } else { 1 };

            for j in 0..repetitions {
                assert!(index.next());
                assert_eq!(index.get_rid(), Some(RID::new(i as u64, 0 as i16)));
            }

            assert!(!index.next());
        }
    }
}
