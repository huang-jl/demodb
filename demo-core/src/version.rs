use crate::{record::Writer, storage::File};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::ops::{Deref, DerefMut};

/// Version persist rather independently (not like a table or kv pairs).
/// It will be kept in a standalone files (inventory),
/// so we use a third serialize library to persist it for simplicity

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Version {
    /// file num is starting from 1
    next_file_num: u64,
    last_seq_num: u64,
    /// all log file number < marker can be delete and do not need when recovery
    pub(crate) stale_log_marker: u64,
    ///  current (i.e. newest) log number
    pub(crate) log_number: u64,
    /// Levels of SST: for now we only support level-0 sst
    ///
    /// begin: stale, end: fresh
    pub(crate) files: Vec<Vec<FileMetaData>>,
}

impl Version {
    pub fn new() -> Self {
        // file num is starting from 1
        Self {
            next_file_num: 1,
            last_seq_num: 0,
            log_number: 0,
            stale_log_marker: 0,
            files: vec![vec![]],
        }
    }

    pub fn allocate_next_file_num(&mut self) -> u64 {
        let n = self.next_file_num;
        self.next_file_num += 1;
        n
    }

    pub fn add_files(&mut self, level: usize, file: FileMetaData) {
        if level > 0 {
            unimplemented!("only support level 0 now")
        }
        self.files[level].push(file);
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(&self).expect("Version could not serialize to json")
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        serde_json::from_slice(buf)
            .map_err(|e| Error::Corrupt(format!("version deserialize failed: {e}")))
    }
}

/// The metadata of sst table
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct FileMetaData {
    pub(crate) file_num: u64,
    pub(crate) file_size: u64,
    pub(crate) smallest_key: Vec<u8>,
    pub(crate) largest_key: Vec<u8>,
}

impl FileMetaData {
    pub fn new(file_num: u64) -> Self {
        Self {
            file_num,
            ..Default::default()
        }
    }
}

pub(crate) struct DBVersion<F: File> {
    pub(crate) version: Version,
    pub(crate) wal_writer: Option<Writer<F>>,
}

impl<F: File> Deref for DBVersion<F> {
    type Target = Version;

    fn deref(&self) -> &Self::Target {
        &self.version
    }
}

impl<F: File> DerefMut for DBVersion<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.version
    }
}
