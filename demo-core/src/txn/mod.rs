use crate::{db::DemoDB, Error, Result, Storage, WriteBatch};

mod manager;

pub(crate) use manager::LockManager;

pub(crate) type TxnID = u64;

// TODO (huang-jl): support for fault-tolerance of client
// maybe timeout ?

pub struct Txn<'a, S: Storage + Clone + 'static> {
    pub(crate) db: &'a DemoDB<S>,
    pub(crate) write_batch: WriteBatch,
    pub(crate) txn_id: TxnID,
}

impl<'a, S: Storage + Clone + 'static> Drop for Txn<'a, S> {
    fn drop(&mut self) {
        self.db.release_lock(self.txn_id);
    }
}

impl<'a, S: Storage + Clone + 'static> Txn<'a, S> {
    pub(crate) fn new(db: &'a DemoDB<S>, txn_id: TxnID) -> Self {
        Self {
            db,
            write_batch: WriteBatch::new(),
            txn_id,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.db.write_lock(key, self.txn_id) {
            return Err(Error::TxnConflict("txn cannot get write lock".to_owned()));
        }
        self.write_batch.put(key, value);
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // first try to get from write batch (local write)
        if !self.db.read_lock(key, self.txn_id) {
            return Err(Error::TxnConflict("txn cannot get read lock".to_owned()));
        }

        match self.write_batch.get_in_batch(key)? {
            Some(Some(val)) => return Ok(Some(val.to_vec())),
            Some(None) => return Ok(None),
            _ => {}
        }

        // get from database
        Ok(self.db.get(key))
    }

    pub fn del(&mut self, key: &[u8]) -> Result<()> {
        if !self.db.write_lock(key, self.txn_id) {
            return Err(Error::TxnConflict("txn cannot get write lock".to_owned()));
        }
        self.write_batch.del(key);
        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        let write_batch = std::mem::replace(&mut self.write_batch, WriteBatch::new());
        if write_batch.get_op_num() > 0 {
            self.db.write_batch(write_batch)?;
        }
        Ok(())
    }

    pub fn abort(self) -> Result<()> {
        Ok(())
    }
}
