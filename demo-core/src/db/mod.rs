use crate::batch::{BatchTask, ValType, WriteBatch};
use crate::config::Config;
use crate::iterator::Iterator;
use crate::memtable::{Memtable, MemtableIter};
use crate::record::{Reader, Writer};
use crate::sstable::TableBuilder;
use crate::storage::{File, FileType, Storage};
use crate::table_cache::TableCache;
use crate::txn::LockManager;
use crate::version::{DBVersion, FileMetaData, Version};
use crate::Result;
use crate::{misc, Error};
use log::{debug, error, info, warn};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc::SyncSender;
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard, RwLock};

mod txn;

#[derive(Clone)]
pub struct DemoDB<S: Storage + Clone + 'static> {
    // path of database
    path: String,
    config: Config,
    inner: Arc<DBInner<S>>,
}

pub(crate) struct DBInner<S: Storage + Clone> {
    // mem table
    mt: RwLock<Memtable>,
    // immutable mem table
    im_mt: RwLock<Option<Memtable>>,
    compact_signal: Condvar,

    version: Mutex<DBVersion<S::F>>,
    storage: S,
    // a cache of filenum -> Table
    table_cache: TableCache<S>,

    write_queue: Mutex<VecDeque<BatchTask>>,
    /// signal background thread that new BatchTask is arriving
    write_signal: Condvar,

    lock_manager: Mutex<LockManager>,
    next_txn_id: AtomicU64,
}

impl<S: Storage + Clone> DemoDB<S> {
    pub fn open(path: impl AsRef<Path>, storage: S, config_path: impl AsRef<Path>) -> Result<Self> {
        // TODO (recover)

        let config = Config::new(storage.open(config_path)?)?;
        info!("DemoDB load config: {config:?}");

        // init a wal writer
        let path = path.as_ref().as_os_str().to_str().unwrap().to_owned();
        let version = Version::new();

        let inner = DBInner {
            mt: RwLock::new(Memtable::new()),
            version: Mutex::new(DBVersion {
                version,
                wal_writer: None,
            }),
            table_cache: TableCache::new(path.clone(), storage.clone(), 4),
            storage,
            im_mt: RwLock::new(None),
            write_queue: Mutex::new(VecDeque::new()),
            write_signal: Condvar::new(),
            compact_signal: Condvar::new(),
            lock_manager: Mutex::new(LockManager::new()),
            next_txn_id: AtomicU64::new(0),
        };

        let this = Self {
            path,
            inner: Arc::new(inner),
            config,
        };

        if let Err(e) = this.recovery() {
            warn!("recovery from {} failed: {e}", this.path);
            this.inner.storage.mkdir_all(&this.path)?;
        }
        // no matter recovery succeed or fail
        // 1. set a wal_writer
        // 2. persist our version
        {
            let mut db_version = this.inner.version.lock().unwrap();
            let wal_writer = misc::create_wal_writer(
                &this.path,
                &this.inner.storage,
                &mut db_version,
                this.config.clone(),
            )?;
            db_version.wal_writer = Some(wal_writer);

            this.persist_version(&db_version)?;
        }

        this.start_background_task()?;

        Ok(this)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // try to get from memtable
        {
            let mt = self.inner.mt.read().unwrap();
            match mt.get(key) {
                Some(Some(val)) => return Some(val.to_vec()),
                Some(None) => return None,
                None => {}
            }
        }
        // try to get from immutable table
        {
            if let Some(im_mt) = self.inner.im_mt.read().unwrap().as_ref() {
                match im_mt.get(key) {
                    Some(Some(val)) => return Some(val.to_vec()),
                    Some(None) => return None,
                    None => {}
                }
            }
        }
        // try to get from sst
        if let Some(raw) = self.get_raw_value_from_sst(key) {
            match Memtable::parse_value(&raw) {
                Ok((ValType::Put, value)) => {
                    return Some(value.to_vec());
                }
                Ok((ValType::Del, value)) => {
                    assert_eq!(value.len(), 0);
                    return None;
                }
                Err(e) => {
                    warn!("get from sstable error occur: {e}");
                    return None;
                }
            }
        }
        None
    }

    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, val);
        self.write_batch(batch)
    }

    pub fn del(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.del(key);
        self.write_batch(batch)
    }

    pub fn update<F>(&self, key: &[u8], f: F) -> Result<()>
    where
        F: FnOnce(Option<Vec<u8>>) -> Vec<u8>,
    {
        let val = self.get(key);
        let val = f(val);
        self.put(key, &val)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let (batch_task, rx) = BatchTask::new(batch);
        {
            let mut task_queue = self.inner.write_queue.lock().unwrap();
            task_queue.push_back(batch_task);
            self.inner.write_signal.notify_all();
        }
        match rx.recv() {
            Ok(res) => res,
            Err(e) => Err(Error::Internal(format!(
                "write batch recv notify error: {e}"
            ))),
        }
    }
}

impl<S: Storage + Clone> DemoDB<S> {
    fn start_background_task(&self) -> Result<()> {
        let this = self.clone();
        let (compaction_tx, compaction_rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("compact_thread".to_owned())
            .spawn(move || {
                this.background_compact(compaction_rx);
            })?;
        let this = self.clone();
        std::thread::Builder::new()
            .name("write_batch_thread".to_owned())
            .spawn(move || {
                this.process_write_batch(compaction_tx);
            })?;
        Ok(())
    }

    fn notify_clients(senders: &[SyncSender<Result<()>>], res: Result<()>) {
        for sender in senders {
            if let Err(e) = sender.send(res.clone()) {
                panic!("error occur when notify client: {e}");
            }
        }
    }

    fn process_write_batch(&self, compaction_tx: mpsc::Sender<()>) {
        loop {
            let batch_task = self.wait_for_batch_task();
            let clients = batch_task.signal();

            // wait for memtable has room
            let mut version = {
                match self.wait_for_room_in_mt(&compaction_tx) {
                    Ok(version) => version,
                    Err(e) => {
                        error!("error occurs when wait for room in mem table: {}", e);
                        Self::notify_clients(clients, Err(e));
                        continue;
                    }
                }
            };

            // wal logs to persist
            if let Err(e) = version
                .wal_writer
                .as_mut()
                .unwrap()
                .write_record(batch_task.batch().data())
            {
                error!("error occurs when write wal log: {e}");
                Self::notify_clients(clients, Err(e));
                continue;
            }

            // update memtable
            if let Err(e) = self
                .inner
                .mt
                .write()
                .unwrap()
                .write_batch(&batch_task.batch())
            {
                error!("error occurs when memtable apply batch {e}");
                Self::notify_clients(clients, Err(e));
                continue;
            }

            // notify clients
            Self::notify_clients(clients, Ok(()));
        }
    }

    // TODO (huang-jl) try to merge multiple BatchTask
    fn wait_for_batch_task(&self) -> BatchTask {
        let mut queue = self.inner.write_queue.lock().unwrap();
        if queue.is_empty() {
            // wait until the condition is false
            queue = self
                .inner
                .write_signal
                .wait_while(queue, |q| q.is_empty())
                .unwrap();
        }
        let res = queue.pop_front().unwrap();
        res
    }

    fn wait_for_room_in_mt(
        &self,
        compaction_tx: &mpsc::Sender<()>,
    ) -> Result<MutexGuard<DBVersion<S::F>>> {
        let mut version = self.inner.version.lock().unwrap();
        loop {
            if self.inner.mt.read().unwrap().approx_size() < self.config.memtable_size() {
                return Ok(version);
            }
            // the memtable size is not enough, we need schedule to flush memtable to sst
            if self.inner.im_mt.read().unwrap().is_none() {
                let mut mt_guard = self.inner.mt.write().unwrap();
                let mut im_mt_guard = self.inner.im_mt.write().unwrap();
                let mt = &mut *mt_guard;

                match *im_mt_guard {
                    Some(_) => {
                        // someone else insert a immutable memtable (but I think it is not possible)
                        warn!("weired happend in double check immutable memtable");
                        drop(mt_guard);
                        drop(im_mt_guard);
                        version = self.inner.compact_signal.wait(version).unwrap();
                    }
                    None => {
                        im_mt_guard.replace(std::mem::replace(mt, Memtable::new()));

                        // when push memtable to immutable table
                        // we need a new log, so after persist the immutable table
                        // we can remove all previous logs safely
                        let log_number = version.allocate_next_file_num();
                        let wal_writer = Writer::new(
                            self.inner.storage.create(misc::generate_file_name(
                                &self.path,
                                log_number,
                                FileType::Log,
                            ))?,
                            self.config.clone(),
                        );
                        let mut old_writer = version.wal_writer.replace(wal_writer).unwrap();
                        old_writer.close()?;
                        version.log_number = log_number;

                        // notify compaction thread to compact im_mt
                        compaction_tx.send(()).map_err(|e| {
                            Error::Internal(format!("send on compaction channel: {e}"))
                        })?;
                        return Ok(version);
                    }
                }
            } else {
                // we have to wait for im_mt to be flush
                version = self.inner.compact_signal.wait(version).unwrap();
            }
        }
    }

    fn background_compact(&self, compaction_rx: mpsc::Receiver<()>) {
        while let Ok(_) = compaction_rx.recv() {
            if let Err(e) = self.compact_mem_table() {
                panic!("when compaction error occur: {e}")
            }
            self.inner.compact_signal.notify_all();
        }
    }

    fn compact_mem_table(&self) -> Result<()> {
        let mut version = self.inner.version.lock().unwrap();
        let mut im_mt = self.inner.im_mt.write().unwrap();

        // create sst builder for writing to a sst file
        let sst_filenum = version.allocate_next_file_num();
        let sst_filename = misc::generate_file_name(&self.path, sst_filenum, FileType::SSTable);
        let sst_file = self.inner.storage.create(&sst_filename)?;
        let mut sst_meta = FileMetaData::new(sst_filenum);
        let sst_builder = TableBuilder::new(sst_file, self.config.clone());

        let im_mt_iter = im_mt.as_ref().unwrap().iter();
        if let Err(e) = Self::write_level0(im_mt_iter, sst_builder, &mut sst_meta) {
            self.inner.storage.remove(&sst_filename)?;
            return Err(e);
        }
        debug!("write new sst: {sst_filename:?}");
        // update sst table information
        version.add_files(0, sst_meta);
        // persist the version (since we finish a minor compaction)
        // now it is safe to update the marker since we hold the lock of version
        version.stale_log_marker = version.log_number;
        self.persist_version(&version)?;

        // clean the immutable table
        *im_mt = None;

        Ok(())
    }

    fn write_level0<F: File>(
        mut im_mt_iter: MemtableIter,
        mut sst_builder: TableBuilder<F>,
        sst_meta: &mut FileMetaData,
    ) -> Result<()> {
        im_mt_iter.to_first();
        if im_mt_iter.valid() {
            let key = im_mt_iter.key();
            sst_meta.smallest_key.reserve(key.len());
            sst_meta.smallest_key.extend_from_slice(key);
        } else {
            warn!("find immutable table has no smallest keys!")
        }

        let mut last_key = None;
        // iterate through immutable memtable
        while im_mt_iter.valid() {
            let key = im_mt_iter.key();
            sst_builder.add(key, im_mt_iter.value())?;
            last_key = Some((key.as_ptr(), key.len()));
            im_mt_iter.next();
        }
        if let Some((ptr, len)) = last_key {
            // SAFETY: im_mt is protected by mutex
            let key = unsafe { std::slice::from_raw_parts(ptr, len) };
            sst_meta.largest_key.reserve(key.len());
            sst_meta.largest_key.extend_from_slice(key);
        } else {
            warn!("find immutable table has no largest keys!")
        }
        // finish building
        sst_meta.file_size = sst_builder.finish(true)? as _;
        sst_builder.close()?;

        Ok(())
    }

    fn persist_version(&self, version: &DBVersion<S::F>) -> Result<()> {
        let storage = &self.inner.storage;
        let tmp_name = misc::generate_file_name(&self.path, 0, FileType::InventoryTmp);
        {
            let mut tmp = storage.create(&tmp_name)?;
            tmp.write(&version.encode())?;
            tmp.close()?;
        }
        storage.rename(
            tmp_name,
            misc::generate_file_name(&self.path, 0, FileType::Inventory),
        )?;
        Ok(())
    }

    /// This get raw value from KV Store:
    /// need parse `ValueType` before return to client
    fn get_raw_value_from_sst(&self, key: &[u8]) -> Option<Vec<u8>> {
        // we should not hold `version` lock for too long.
        // So we just get the snapshot of sstable files at the time
        // that we acquire the lock of `version` into `files`.
        let files = {
            let version = self.inner.version.lock().unwrap();
            version.files[0]
                .iter()
                .rev()
                .filter(|sst_meta| key <= &sst_meta.largest_key && key >= &sst_meta.smallest_key)
                .map(|x| x.clone())
                .collect::<Vec<_>>()
        };
        let table_cache = &self.inner.table_cache;

        // For now, we only have level 0
        // and we need iterate throught reverse order
        for sst_meta in files {
            match table_cache.seek_key_in_table(sst_meta.file_num, key) {
                Ok(Some(iter)) => {
                    assert!(iter.valid());
                    if iter.key() == key {
                        return Some(iter.value().to_vec());
                    }
                }
                Ok(None) => {
                    warn!("Weird: cannot seek in table whose FileMeta is matching");
                }
                Err(e) => {
                    error!(
                        "error occur when seek in sst table {} {e}",
                        sst_meta.file_num
                    );
                    return None;
                }
            }
        }
        None
    }
}

// recovery
impl<S: Storage + Clone> DemoDB<S> {
    fn recovery(&self) -> Result<()> {
        let mut version = Self::parse_inventory(&self.path, &self.inner.storage)?;
        let mut recovery_log_filenum = vec![];
        let mut sst_filenum = vec![];
        // scan and collect all logs and sstable file information
        for filename in self.inner.storage.list(&self.path)? {
            let (file_num, file_type) = misc::parse_filename(filename);
            match file_type {
                FileType::Log => {
                    if file_num >= version.stale_log_marker {
                        recovery_log_filenum.push(file_num);
                    }
                }
                FileType::SSTable => {
                    // check sstable
                    sst_filenum.push(file_num);
                }
                _ => {}
            }
        }
        // make sure all sstable is avaiable
        for sst_meta in version.files[0].iter() {
            if !sst_filenum.iter().any(|num| *num == sst_meta.file_num) {
                warn!(
                    "sstable of index {} cannot be found, recovery failed",
                    sst_meta.file_num
                );
                return Err(Error::Corrupt(
                    "sstable cannot be found while recovering".to_owned(),
                ));
            }
        }
        // recovery from logs
        for log_filenum in recovery_log_filenum {
            self.recovery_log_at(log_filenum, &mut version)?;
        }
        // finnaly install and persist version
        {
            let mut db_version = self.inner.version.lock().unwrap();
            db_version.version = version;
        }
        Ok(())
    }

    fn recovery_log_at(&self, log_filenum: u64, version: &mut Version) -> Result<()> {
        let log_file = self.inner.storage.open(misc::generate_file_name(
            &self.path,
            log_filenum,
            FileType::Log,
        ))?;

        let mut reader = Reader::new(log_file, self.config.clone());
        loop {
            let mut record = vec![];
            if reader.read_record(&mut record) {
                let batch = WriteBatch::from_raw(record);
                let mut mt = self.inner.mt.write().unwrap();
                // If we have enought room for memtable, then write it to memtable directly
                if mt.approx_size() < self.config.memtable_size() {
                    mt.write_batch(&batch)?;
                    continue;
                }
                // Otherwise we need flush memtable to sstable
                let sst_filenum = version.allocate_next_file_num();
                let sst_filename =
                    misc::generate_file_name(&self.path, sst_filenum, FileType::SSTable);
                let sst_file = self.inner.storage.create(&sst_filename)?;
                let mut sst_meta = FileMetaData::new(sst_filenum);
                let sst_builder = TableBuilder::new(sst_file, self.config.clone());

                Self::write_level0(mt.iter(), sst_builder, &mut sst_meta)?;
                // update sst table information
                version.add_files(0, sst_meta);
                // persist the version (since we finish a minor compaction)
                // now it is safe to update the marker since we hold the lock of version
                version.stale_log_marker = version.log_number;

                // clean the memtable table
                *mt = Memtable::new();
            } else {
                // reader read failed
                break;
            }
        }
        Ok(())
    }

    fn parse_inventory<P: AsRef<Path>>(path: P, storage: &S) -> Result<Version> {
        let inventory_filename = misc::generate_file_name(path.as_ref(), 0, FileType::Inventory);
        let mut inventory_file = storage.open(inventory_filename)?;
        let mut buf = vec![0_u8; inventory_file.len()? as usize];
        inventory_file.read(&mut buf)?;
        Version::decode(&buf)
    }
}
