use super::TxnID;
use std::collections::{HashMap, HashSet};

pub(crate) struct LockManager {
    read_locks: HashMap<Vec<u8>, HashSet<TxnID>>,
    // since write lock is exclusive
    write_locks: HashMap<Vec<u8>, TxnID>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            read_locks: HashMap::new(),
            write_locks: HashMap::new(),
        }
    }

    pub fn read_lock(&mut self, key: &[u8], txn_id: TxnID) -> bool {
        match self.write_locks.get(key) {
            // current txn hold write lock
            Some(tid) if *tid == txn_id => return true,
            // other txn hold write lock
            Some(_) => return false,
            None => {}
        }
        self.read_locks
            .entry(key.to_vec())
            .or_default()
            .insert(txn_id);
        true
    }

    pub fn write_lock(&mut self, key: &[u8], txn_id: TxnID) -> bool {
        match self.read_locks.get_mut(key) {
            Some(val) if val.len() == 0 => {
                // no txn hold read locks
            }
            Some(val) if val.len() == 1 && val.contains(&txn_id) => {
                // try to upgrade locks
                val.remove(&txn_id);
                self.write_locks.insert(key.to_vec(), txn_id);
                return true;
            }
            Some(_) => {
                // other txn is holding read lock
                return false;
            }
            None => {}
        }

        match self.write_locks.get(key) {
            Some(tid) if *tid == txn_id => {
                // txn already hold write lock
                true
            }
            Some(_) => {
                // other txn hold write lock
                false
            }
            None => {
                // no txn hold write lock
                self.write_locks.insert(key.to_vec(), txn_id);
                true
            }
        }
    }

    pub fn release(&mut self, txn_id: TxnID) {
        for val in self.read_locks.values_mut() {
            val.remove(&txn_id);
        }

        self.write_locks.retain(|_, v| *v != txn_id);
    }
}
