use crate::storage::File;
use crate::{Error, Result};
use serde::Deserialize;
use serde_json;

#[derive(Deserialize, Default, Clone, Debug)]
pub(crate) struct Config {
    pub(crate) block_size: usize,
    pub(crate) memtable_size: usize,
}

impl Config {
    pub(crate) fn new<F: File>(mut f: F) -> Result<Self> {
        let file_length = f.len()?;
        let mut buf = vec![0_u8; file_length as usize];
        f.read(&mut buf)?;
        let config: Config = serde_json::from_slice(&buf)
            .map_err(|e| Error::Corrupt(format!("cannot deserialze Config: {e}")))?;
        Ok(config)
    }

    #[inline]
    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }

    #[inline]
    pub(crate) fn memtable_size(&self) -> usize {
        self.memtable_size
    }
}
