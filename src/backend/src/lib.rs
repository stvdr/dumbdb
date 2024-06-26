#![feature(assert_matches)]
#![feature(generic_const_exprs)]
#![feature(inherent_associated_types)]
#![feature(iter_intersperse)]
#![feature(let_chains)]
#![feature(trait_upcasting)]

#[cfg(test)]
mod tests;

mod block_id;
mod buffer;
mod buffer_list;
mod buffer_manager;
mod concurrency_manager;
mod db;
mod eviction_policy;
mod file_manager;
mod layout;
mod lock_table;
mod log_manager;
mod log_record;
mod page;
mod record_page;
mod rid;
mod schema;
mod table_scan;
mod transaction;

mod index;
mod metadata;
mod parser;
mod planning;
mod scan;
