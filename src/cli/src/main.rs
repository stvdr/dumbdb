use backend::db::SimpleDB;
use backend::parser::parser::{parse, RootNode};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn initialize_db(root: &Path, num_buffers: usize) -> SimpleDB {
    // Create data and log directories where pages and WAL files will be stored
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let log_dir = root.join("log");
    fs::create_dir_all(&log_dir).unwrap();

    SimpleDB::new(&data_dir, &log_dir, num_buffers)
}

fn main() {
    let mut db = initialize_db("/tmp/db".as_ref(), 4096);

    let tx = Arc::new(Mutex::new(db.new_tx()));
    let sql = "CREATE TABLE test ( id int, name varchar(40))";
    let ast = parse(sql).unwrap();

    if let RootNode::Select(_) = ast {
        let plan = db
            .planner()
            .lock()
            .unwrap()
            .create_query_plan(&ast, tx.clone());
    } else {
        // Not a select query, execute immediately
        let result = db
            .planner()
            .lock()
            .unwrap()
            .execute_update(&ast, tx.clone());
    }
}
