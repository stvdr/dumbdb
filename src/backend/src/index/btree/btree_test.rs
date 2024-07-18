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
    fn test_btree_index() {
        tracing_subscriber::fmt::init();
        //tracing_subscriber::fmt()
        //    .event_format(
        //        tracing_subscriber::fmt::format()
        //            .with_file(true)
        //            .with_line_number(true)
        //            .with_thread_ids(true),
        //    )
        //    .init();

        let dir = tempdir().unwrap();
        let db = test_db(&dir);

        let tx = Arc::new(Mutex::new(db.new_tx()));

        let leaf_layout = Layout::from_schema(make_schema! {
            "dataval" => i32,
            "block" => i32,
            "id" => i32
        });

        let mut index = BTreeIndex::new(tx, "test-idx", leaf_layout);

        for i in 0..1000 {
            index.insert(&Value::Int(i), RID::new(i as u64 / 100, i as i16 % 100));
        }

        for i in 0..1000 {
            index.before_first(&Value::Int(i));
            while index.next() {
                assert_eq!(
                    index.get_rid(),
                    Some(RID::new(i as u64 / 100, i as i16 % 100))
                );
            }
        }

        // Test deletions
        for i in 0..1000 {
            if i % 20 == 0 {
                index.delete(&Value::Int(i), RID::new(i as u64 / 100, i as i16 % 100));
            }
        }

        for i in 0..1000 {
            index.before_first(&Value::Int(i));

            if i % 20 == 0 {
                assert!(!index.next());
            } else {
                while index.next() {
                    assert_eq!(
                        index.get_rid(),
                        Some(RID::new(i as u64 / 100, i as i16 % 100))
                    );
                }
            }
        }
    }
}
