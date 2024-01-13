use log::{debug, error};

use crate::batch::{ValType, WriteBatch};
use crate::iterator::Iterator;
use crate::Result;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};

/// this is an in memory representaion of a table
pub(crate) struct Memtable {
    table: BTreeMap<Vec<u8>, Vec<u8>>,
    // approx size in bytes
    approx_size: usize,
}

impl Default for Memtable {
    fn default() -> Self {
        Self {
            table: Default::default(),
            approx_size: 0,
        }
    }
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            table: BTreeMap::new(),
            approx_size: 0,
        }
    }

    /// The value returned from MemtableIter is raw data
    /// (e.g., without parsing `ValType`)
    pub fn iter(&self) -> MemtableIter<'_> {
        MemtableIter {
            mem_table: &self,
            inner: None,
        }
    }

    /// get key-value after parsing `ValType` in Memtable
    /// ## Return Value
    /// - Find a recent put => return Some(value)
    /// - Find a recent del => return Some(None)
    /// - Do not find any in memtable or error occur => return None
    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        match self.table.get(key) {
            Some(v) => match ValType::decode(&v) {
                Ok(ValType::Put) => Some(Some(&v[1..])),
                Ok(ValType::Del) => Some(None),
                Err(e) => {
                    error!("find corrupted value in mem table for key {:?} {e}", key);
                    None
                }
            },
            None => None,
        }
    }

    /// write batch of operation `batch`
    ///
    /// The real data been written containes other metadata apart from key-value
    pub fn write_batch(&mut self, batch: &WriteBatch) -> Result<()> {
        let mut batch_iter = batch.iter();
        loop {
            match batch_iter.next()? {
                Some((ValType::Put, k, v)) => {
                    let mut value = Vec::with_capacity(1 + v.len());
                    ValType::Put.encode(&mut value);
                    value.extend_from_slice(v);
                    debug_assert_eq!(value.len(), 1 + v.len());
                    self.approx_size += k.len() + value.len();
                    self.table.insert(k.to_vec(), value);
                }
                Some((ValType::Del, k, _)) => {
                    let mut value = vec![];
                    ValType::Del.encode(&mut value);
                    debug_assert_eq!(value.len(), 1);
                    self.approx_size += k.len() + value.len();
                    self.table.insert(k.to_vec(), value);
                }
                None => break,
            }
        }
        Ok(())
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    /// parsing a byte array according to data placement rules of `Memtable`
    ///
    /// The `buf` should be results value read from [`crate::sstable::BlockItertor`]
    /// of SSTable (i.e. [`crate::sstable::Table`])
    pub fn parse_value(buf: &[u8]) -> Result<(ValType, &[u8])> {
        let val_type = ValType::decode(buf)?;
        Ok((val_type, &buf[1..]))
    }
}

/// This iterator return raw data (e.g., without parsing `ValType`)
pub(crate) struct MemtableIter<'a> {
    mem_table: &'a Memtable,
    inner: Option<MemtableIterInner<'a>>,
}

struct MemtableIterInner<'a> {
    iter: Box<dyn std::iter::Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)> + 'a>,
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> MemtableIter<'a> {
    fn set_iter(
        &mut self,
        mut iter: Box<dyn std::iter::Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)> + 'a>,
    ) {
        match iter.next() {
            Some((key, value)) => {
                self.inner = Some(MemtableIterInner { key, value, iter });
            }
            None => {
                self.inner = None;
            }
        }
    }
}

impl<'a> Iterator for MemtableIter<'a> {
    fn valid(&self) -> bool {
        self.inner.is_some()
    }

    fn seek(&mut self, target: &[u8]) {
        let iter = self
            .mem_table
            .table
            .range::<[u8], _>((Included(target), Unbounded));
        self.set_iter(Box::new(iter));
    }

    fn to_first(&mut self) {
        let iter = self.mem_table.table.iter();
        self.set_iter(Box::new(iter));
    }

    fn key(&self) -> &[u8] {
        self.inner.as_ref().unwrap().key
    }

    fn value(&self) -> &[u8] {
        self.inner.as_ref().unwrap().value
    }

    fn next(&mut self) {
        let inner = self.inner.as_mut().unwrap();
        match inner.iter.next() {
            Some((key, value)) => {
                inner.key = key;
                inner.value = value;
            }
            None => {
                self.inner = None;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::str;

    use super::*;

    fn val_to_raw(val: u32) -> Vec<u8> {
        [
            (ValType::Put as u8).to_le_bytes().as_slice(),
            val.to_ne_bytes().as_slice(),
        ]
        .concat()
    }
    #[test]
    fn memtable_iter_simple() {
        let mut memtable = Memtable::new();
        let mut batch = WriteBatch::new();
        for i in 1..10_u32 {
            batch.put(format!("huang{i}").as_bytes(), &i.to_ne_bytes());
        }
        memtable.write_batch(&batch).unwrap();

        let mut iter = memtable.iter();
        let mut start: u32 = 5;
        iter.seek(format!("huang{start}").as_bytes());
        assert!(iter.valid());
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(key, format!("huang{start}").as_bytes(), "check {}", start);
            assert_eq!(value, val_to_raw(start), "check {}", start);
            start += 1;
            iter.next();
        }
        assert_eq!(start, 10);
    }

    #[test]
    fn memtable_iter_var_length() {
        let mut memtable = Memtable::new();
        let mut batch = WriteBatch::new();
        for i in 1..5_u32 {
            batch.put(format!("huang{}", i * 2).as_bytes(), &i.to_ne_bytes());
        }
        batch.put(
            format!("xabcdefg").as_bytes(),
            &2022310806_u32.to_ne_bytes(),
        );
        memtable.write_batch(&batch).unwrap();
        let mut iter = memtable.iter();
        iter.seek("huang7".as_bytes());
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "huang8");
        assert_eq!(iter.value(), val_to_raw(4));

        iter.seek("huang10".as_bytes());
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "huang2");
        assert_eq!(iter.value(), val_to_raw(1));

        iter.seek("huang99".as_bytes());
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "xabcdefg");
        assert_eq!(iter.value(), val_to_raw(2022310806));

        iter.next();
        assert!(!iter.valid());

        iter.to_first();
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "huang2");
        assert_eq!(iter.value(), val_to_raw(1));

        iter.to_first();
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "huang2");
        assert_eq!(iter.value(), val_to_raw(1));

        iter.next();
        assert!(iter.valid());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "huang4");
        assert_eq!(iter.value(), val_to_raw(2));

        iter.seek("xxxxxxx".as_bytes());
        assert!(!iter.valid());
    }
}
