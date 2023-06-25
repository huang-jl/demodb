use super::DemoDB;
use crate::{
    txn::{Txn, TxnID},
    Storage,
};
use std::sync::atomic::Ordering;

impl<S: Storage + Clone + 'static> DemoDB<S> {
    pub fn begin_txn(&self) -> Txn<S> {
        let txn_id = self.inner.next_txn_id.fetch_add(1, Ordering::SeqCst);
        Txn::new(self, txn_id)
    }

    pub fn read_lock(&self, key: &[u8], txn_id: TxnID) -> bool {
        let mut lock_manager = self.inner.lock_manager.lock().unwrap();
        lock_manager.read_lock(key, txn_id)
    }

    pub fn write_lock(&self, key: &[u8], txn_id: TxnID) -> bool {
        let mut lock_manager = self.inner.lock_manager.lock().unwrap();
        lock_manager.write_lock(key, txn_id)
    }

    pub fn release_lock(&self, txn_id: TxnID) {
        let mut lock_manager = self.inner.lock_manager.lock().unwrap();
        lock_manager.release(txn_id);
    }

    pub fn allocate_txn_id(&self) -> u64 {
        self.inner.next_txn_id.fetch_add(1, Ordering::SeqCst)
    }
}
