use crate::{iterator::Iterator, Error, Result};
use log::error;
use std::{cmp::Ordering, sync::Arc};

pub(crate) struct Block {
    // use arc there so block iterator can live longer than block reference
    data: Arc<Vec<u8>>,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }

    pub fn iter(&self) -> BlockIterator {
        BlockIterator {
            data: self.data.clone(),
            pos: self.data.len(),
            key_off: 0,
            key_len: 0,
            value_off: 0,
            value_len: 0,
        }
    }
}

// TODO (change Block structer to use binary search)
pub(crate) struct BlockIterator {
    data: Arc<Vec<u8>>,
    pos: usize,
    key_off: usize,
    key_len: usize,
    value_off: usize,
    value_len: usize,
}

impl BlockIterator {
    fn invalidate(&mut self) {
        self.pos = self.data.len();
    }
    
    /// parse a key value pair start from self.pos
    fn parse_curr(&mut self) {
        let total = self.data.len();
        let mut curr = self.pos;
        if total < curr + 4 {
            self.invalidate();
            return;
        }
        let b = self.data[curr..curr + 4].try_into().unwrap();
        let key_len = u32::from_le_bytes(b) as usize;
        self.key_off = curr + 4;
        self.key_len = key_len;
        curr += 4 + key_len;

        if total < curr + 4 {
            self.invalidate();
            return;
        }
        let b = self.data[curr..curr + 4].try_into().unwrap();
        let value_len = u32::from_le_bytes(b) as usize;
        self.value_off = curr + 4;
        self.value_len = value_len;
        if total < self.value_off + self.value_len {
            self.invalidate();
        }
    }
}

impl Iterator for BlockIterator {
    fn valid(&self) -> bool {
        self.pos < self.data.len()
    }

    fn seek(&mut self, target: &[u8]) {
        self.to_first();
        while self.valid() && self.key().cmp(target) == Ordering::Less {
            self.next();
        }
    }

    fn to_first(&mut self) {
        self.pos = 0;
        if self.valid() {
            self.parse_curr();
        }
    }

    fn next(&mut self) {
        self.pos = self.value_off + self.value_len;
        if self.valid() {
            self.parse_curr();
        }
    }

    fn key(&self) -> &[u8] {
        &self.data[self.key_off..self.key_off + self.key_len]
    }

    fn value(&self) -> &[u8] {
        &self.data[self.value_off..self.value_off + self.value_len]
    }
}

pub(super) struct BlockBuilder {
    buf: Vec<u8>,
    // make sure the block is ordered
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        let buf = Vec::with_capacity(512);
        Self {
            buf,
            last_key: vec![],
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.last_key.is_empty() && key.cmp(&self.last_key) != Ordering::Greater {
            error!("add key ({:?}) to block build in invalid order", key);
            return Err(Error::KeyOrder);
        }

        // insert key and value
        for arr in [key, value] {
            self.buf
                .extend_from_slice(&(arr.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(arr);
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.buf.len()
    }

    /// reset will not clean last_key so the order will be kept
    pub fn reset(&mut self) {
        self.buf.clear()
    }

    /// return (`last_key`, `internal_buf`)
    pub fn finish(&mut self) -> (&[u8], &[u8]) {
        (&self.last_key, &self.buf)
    }
}
