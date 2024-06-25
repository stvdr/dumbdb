use std::{
    fs,
    sync::{Arc, Mutex},
};

use log::warn;
use tempfile::{tempdir, TempDir};

use crate::{
    db::SimpleDB, insert, make_schema, metadata::metadata_manager::MetadataManager,
    table_scan::TableScan,
};

const DEFAULT_BLOCK_SIZE: usize = 4096;

/// Get a `SimpleDB` with log and data storage written into temporary directories.
pub fn test_db<const PAGE_SIZE: usize>(td: &TempDir) -> SimpleDB<PAGE_SIZE> {
    let data_dir = td.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let log_dir = td.path().join("log");
    fs::create_dir_all(&log_dir).unwrap();

    SimpleDB::new(&data_dir, &log_dir, 1024)
}

pub fn default_test_db(td: &TempDir) -> SimpleDB<DEFAULT_BLOCK_SIZE> {
    test_db(td)
}

/// Create a set of default tables that can be used in unit tests.
pub fn create_default_tables<const PAGE_SIZE: usize>(db: &mut SimpleDB<PAGE_SIZE>) {
    let tx = Arc::new(Mutex::new(db.new_tx()));

    let student_schema = make_schema! {
        "sid" => i32,
        "sname" => varchar(20),
        "grad_year" => i32,
        "major_id" => i32
    };

    let dept_schema = make_schema! {
        "did" => i32,
        "dname" => varchar(20)
    };

    let enroll_schema = make_schema! {
        "eid" => i32,
        "student_id" => i32,
        "section_id" => i32,
        "grade" => varchar(10)
    };

    let course_schema = make_schema! {
        "cid" => i32,
        "title" => varchar(20),
        "dept_id" => i32
    };

    let section_schema = make_schema! {
        "sectid" => i32,
        "course_id" => i32,
        "prof" => varchar(20),
        "year" => i32
    };

    let meta_mgr = MetadataManager::new(&tx);
    meta_mgr.create_table("student", &student_schema, &tx);
    meta_mgr.create_table("dept", &dept_schema, &tx);
    meta_mgr.create_table("enroll", &enroll_schema, &tx);
    meta_mgr.create_table("course", &course_schema, &tx);
    meta_mgr.create_table("section", &section_schema, &tx);

    let mut scan = TableScan::new(
        tx.clone(),
        meta_mgr.get_table_layout("student", &tx).unwrap(),
        "student",
    );
    insert![
        scan,
        (1, "joe", 2021, 10),
        (2, "amy", 2020, 20),
        (3, "max", 2022, 10),
        (4, "sue", 2022, 20),
        (5, "bob", 2020, 30),
        (6, "kim", 2020, 20),
        (7, "art", 2021, 30),
        (8, "pat", 2019, 20),
        (9, "lee", 2021, 10)
    ];

    let mut scan = TableScan::new(
        tx.clone(),
        meta_mgr.get_table_layout("dept", &tx).unwrap(),
        "dept",
    );
    insert![scan, (10, "compsci"), (20, "math"), (30, "drama")];

    let mut scan = TableScan::new(
        tx.clone(),
        meta_mgr.get_table_layout("enroll", &tx).unwrap(),
        "enroll",
    );
    insert![
        scan,
        (14, 1, 13, "A"),
        (24, 1, 43, "C"),
        (34, 2, 43, "B+"),
        (44, 4, 33, "B"),
        (54, 4, 53, "A"),
        (64, 6, 53, "A")
    ];

    let mut scan = TableScan::new(
        tx.clone(),
        meta_mgr.get_table_layout("course", &tx).unwrap(),
        "course",
    );
    insert![
        scan,
        (12, "db systems", 10),
        (22, "compilers", 10),
        (32, "calculus", 20),
        (42, "algebra", 20),
        (52, "acting", 30),
        (62, "elocution", 30)
    ];

    let mut scan = TableScan::new(
        tx.clone(),
        meta_mgr.get_table_layout("section", &tx).unwrap(),
        "section",
    );
    insert![
        scan,
        (13, 12, "turing", 2018),
        (23, 12, "turing", 2016),
        (33, 32, "newton", 2017),
        (43, 32, "einstein", 2018),
        (53, 62, "brando", 2017)
    ];

    tx.lock().unwrap().commit();
}
