use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
};

use backtrace::Backtrace;

use crate::block_id::BlockId;

static MAX_TIME_MS: u32 = 10000;

struct Lock {
    count: Mutex<i16>,
    condvar: Condvar,
}

impl Lock {
    fn new_shared() -> Self {
        Self {
            count: Mutex::new(1),
            condvar: Condvar::new(),
        }
    }

    fn new_exclusive() -> Self {
        Self {
            count: Mutex::new(-1),
            condvar: Condvar::new(),
        }
    }
}

pub struct LockTable {
    locks: Arc<Mutex<HashMap<BlockId, Arc<Lock>>>>,
}

//unsafe impl Send for LockTable {}
//unsafe impl Sync for LockTable {}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Request a shared lock on a block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The BlockId that the shared lock will be held on.
    pub fn slock(&self, blk: &BlockId) {
        log::trace!("requesting an slock");
        //let backtrace = Backtrace::new();
        //log::trace!("{backtrace:?}");

        let lock = {
            let mut locks = self.locks.lock().unwrap();
            if let Some(lock) = locks.get(blk).cloned() {
                lock
            } else {
                log::trace!("adding new shared lock");
                locks.insert(blk.clone(), Arc::new(Lock::new_shared()));
                return;
            }
        };

        {
            let mut count = lock.count.lock().unwrap();
            while *count == -1 {
                log::trace!("waiting for slock. xlock already exists");
                count = lock.condvar.wait(count).unwrap();
            }

            log::trace!("successfully retrieved slock");
            *count += 1;
        }
    }

    /// Request an exclusive lock on a block.
    ///
    /// # Arguments
    ///
    /// * `blk` - The BlockId that the exclusive lock will be held on.
    pub fn xlock(&self, blk: &BlockId) {
        log::trace!("requesting an xlock");

        let lock = {
            let mut locks = self.locks.lock().unwrap();
            if let Some(lock) = locks.get(blk).cloned() {
                lock
            } else {
                log::trace!("inserting new xlock");
                locks.insert(blk.clone(), Arc::new(Lock::new_exclusive()));
                return;
            }
        };

        {
            let mut count = lock.count.lock().unwrap();
            while *count > 1 {
                log::trace!("waiting for xlock. Lock count > 1");
                // TODO: wait_timeout
                count = lock.condvar.wait(count).unwrap();
            }

            log::trace!("successfully set xlock");
            *count = -1;
        }

        // The lock will have been removed from the lock map while we were waiting for it. Move it
        // back.
        // TODO: Think more about this
        //self.locks.lock().unwrap().insert(blk.clone(), lock);
    }

    /// Removes the lock held by the current thread.
    ///
    /// # Arguments
    ///
    /// * `blk` - The BlockId that the lock will be removed for.
    pub fn unlock(&self, blk: &BlockId) {
        let lock = {
            let locks = self.locks.lock().unwrap();
            if let Some(lock) = locks.get(blk).cloned() {
                lock
            } else {
                // TODO: return a Result instead?
                panic!("attempting to unlock block that is not currently locked!");
            }
        };

        let mut count = lock.count.lock().unwrap();
        assert!(*count != 0, "Unexpected lock count value of 0");
        if *count == -1 {
            *count = 0;
        } else if *count > 0 {
            *count -= 1;
        }

        // If no locks are held, or a single shared lock (can be upgraded to an xlock) is held, notify waiting threads
        if *count == 0 || *count == 1 {
            lock.condvar.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier,
        },
        thread,
    };

    use crate::block_id::BlockId;

    use super::LockTable;

    #[test]
    fn test_threaded_locks() {
        let _ = env_logger::try_init();
        let lock_table = Arc::new(LockTable::new());

        let start_barrier = Arc::new(Barrier::new(3));

        let xlock_counter = Arc::new(AtomicUsize::new(0));
        let slock_counter = Arc::new(AtomicUsize::new(0));

        let start_barrier_x = start_barrier.clone();
        let lock_table_x = lock_table.clone();
        let xlock_counter_x = xlock_counter.clone();
        let handle_x = thread::spawn(move || {
            start_barrier_x.wait();
            for _ in 0..500 {
                lock_table_x.slock(&BlockId::new("test", 1));
                lock_table_x.xlock(&BlockId::new("test", 1));
                xlock_counter_x.fetch_add(1, Ordering::SeqCst);
                lock_table_x.unlock(&BlockId::new("test", 1));
            }
        });

        let start_barrier_s = start_barrier.clone();
        let lock_table_s = lock_table.clone();
        let slock_counter_s = slock_counter.clone();
        let handle_s = thread::spawn(move || {
            start_barrier_s.wait();
            for _ in 0..100 {
                for _ in 0..5 {
                    lock_table_s.slock(&BlockId::new("test", 1));
                    slock_counter_s.fetch_add(1, Ordering::SeqCst);
                }
                for _ in 0..5 {
                    lock_table_s.unlock(&BlockId::new("test", 1));
                }
            }
        });

        start_barrier.wait();
        handle_x.join().unwrap();
        handle_s.join().unwrap();

        assert_eq!(xlock_counter.load(Ordering::SeqCst), 500);
        assert_eq!(slock_counter.load(Ordering::SeqCst), 500);
    }
}
