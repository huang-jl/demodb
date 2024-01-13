use crate::sstable::{BlockIterator, Table};
use crate::storage::Storage;
use crate::{misc, Result};
use log::debug;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

/// Table cache is used by DB as a cache of SSTable.
///
/// When get operation cannot find in memtable and im memtable,
/// it need iterate through sstable. At this time,
/// the sstable will be kept by this `TableCache`
///
/// It is a vector (for simplicity) since the size will not be large.
pub(crate) struct TableCache<S: Storage + Clone> {
    /// To optmize `get()` operation, we need not block multiple readers
    /// to access TableCache simultaneously, so I choose RwLock here.
    cache: RwLock<VecDeque<(u64, Arc<Table<S::F>>)>>,
    size: usize,
    storage: S,
    /// same as db path
    path: String,
}

impl<S: Storage + Clone> TableCache<S> {
    pub fn new(path: String, storage: S, size: usize) -> Self {
        Self {
            cache: Default::default(),
            size,
            storage,
            path,
        }
    }

    /// fetch the sstable of specify file number.
    /// if not in cache, then fill the sstable into cache
    pub fn fetch_table(&self, file_num: u64) -> Result<Arc<Table<S::F>>> {
        let cache = self.cache.read().unwrap();
        let res;
        match cache.iter().position(|(file_id, _)| *file_id == file_num) {
            Some(idx) => {
                res = cache[idx].1.clone();
            }
            None => {
                let table_name = misc::generate_file_name(
                    &self.path,
                    file_num,
                    crate::storage::FileType::SSTable,
                );
                debug!("read sst table of {table_name:?} into memory");
                let table_file = self.storage.open(table_name)?;
                let table = Arc::new(Table::open(table_file)?);
                res = table.clone();
                // upgrade lock to write lock
                drop(cache);
                let mut cache = self.cache.write().unwrap();
                // double check for the duration that we drop read lock and get write lock
                if cache.iter().all(|(file_id, _)| *file_id != file_num) {
                    if cache.len() >= self.size {
                        assert!(cache.len() == self.size);
                        cache.pop_front();
                    }
                    cache.push_back((file_num, table));
                }
            }
        }
        Ok(res)
    }

    /// The return value
    pub fn seek_key_in_table(&self, file_num: u64, key: &[u8]) -> Result<Option<BlockIterator>> {
        let table = self.fetch_table(file_num)?;
        table.get(key)
    }
}
