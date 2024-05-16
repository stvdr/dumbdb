use std::sync::atomic::AtomicU64;

static NEXT_TRANSACTION_NUM: AtomicU64 = AtomicU64::new(0);

pub struct Transaction {}
