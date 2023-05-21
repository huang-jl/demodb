use crate::Error;
use crate::Result;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

const U32_LEN: usize = std::mem::size_of::<u32>();

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum ValType {
    Put = 1,
    Del,
}

fn encode_bytes(src: &[u8], target: &mut Vec<u8>) {
    let length = src.len() as u32;
    target.extend_from_slice(&length.to_le_bytes());
    target.extend_from_slice(src);
}

// decode a byte array from src, return (decoded bytes, consumed byte size)
fn decode_bytes(src: &[u8]) -> Result<(&[u8], usize)> {
    if src.len() < U32_LEN {
        return Err(Error::Corrupt("buf length not enough for u32".to_owned()));
    }
    let length = u32::from_le_bytes(src[..U32_LEN].try_into().unwrap()) as usize;
    Ok((&src[U32_LEN..U32_LEN + length], U32_LEN + length))
}

impl ValType {
    pub fn encode(self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self as u8).to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 1 {
            return Err(Error::Corrupt(
                "buf length not enough for ValType".to_owned(),
            ));
        }
        match u8::from_le_bytes(buf[..1].try_into().unwrap()) {
            1 => Ok(Self::Put),
            2 => Ok(Self::Del),
            val => Err(Error::Corrupt(format!("Unknown val type: {val}"))),
        }
    }
}

// each entry in batch task is
// [ValType: 1B] [Key length 4B] [Key] [Value length 4B] [Value]
// and in batch, the format is:
// [Entry Num: 4B] [Entry1] [Entry2] ... [Entryn]

// batch task containes multiple put / delete op
pub struct WriteBatch {
    data: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        debug_assert_eq!(0_u32.to_le_bytes(), [0_u8; U32_LEN]);
        Self {
            data: vec![0_u8; U32_LEN],
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let val_type = ValType::Put;
        val_type.encode(&mut self.data);
        encode_bytes(key, &mut self.data);
        encode_bytes(value, &mut self.data);

        self.set_op_num(self.get_op_num() + 1);
    }

    pub fn del(&mut self, key: &[u8]) {
        let val_type = ValType::Del;
        val_type.encode(&mut self.data);
        encode_bytes(key, &mut self.data);
        self.set_op_num(self.get_op_num() + 1);
    }

    pub(crate) fn set_op_num(&mut self, num: u32) {
        self.data[..U32_LEN].copy_from_slice(&num.to_le_bytes());
    }

    pub(crate) fn get_op_num(&self) -> u32 {
        assert!(self.data.len() >= U32_LEN);
        u32::from_le_bytes(self.data[..U32_LEN].try_into().unwrap())
    }

    pub(crate) fn iter(&self) -> WriteBatchIter {
        let total = u32::from_le_bytes(self.data[..U32_LEN].try_into().unwrap());
        WriteBatchIter {
            data: &self.data[U32_LEN..],
            pos: 0,
            total: total,
        }
    }

    /// return the raw bytes of WriteBatch, which can be flush to disk directly
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn from_raw(data: Vec<u8>) -> Self {
        Self { data }
    }
}

pub(crate) struct WriteBatchIter<'a> {
    data: &'a [u8],
    pos: u32,
    total: u32,
}

impl<'a> WriteBatchIter<'a> {
    /// return (val_type, key, value)
    pub fn next(&mut self) -> Result<Option<(ValType, &[u8], &[u8])>> {
        if self.pos >= self.total {
            return Ok(None);
        }

        match ValType::decode(&self.data[..1])? {
            ValType::Put => {
                let (key, consume_size) = decode_bytes(&self.data[1..])?;
                self.data = &self.data[1 + consume_size..];
                let (value, consume_size) = decode_bytes(&self.data)?;
                self.data = &self.data[consume_size..];

                self.pos += 1;
                Ok(Some((ValType::Put, key, value)))
            }
            ValType::Del => {
                let (key, consume_size) = decode_bytes(&self.data[1..])?;
                self.data = &self.data[1 + consume_size..];
                self.pos += 1;
                Ok(Some((ValType::Del, key, &[])))
            }
        }
    }
}

pub(crate) struct BatchTask {
    batch: WriteBatch,
    signal: Vec<SyncSender<Result<()>>>,
}

impl BatchTask {
    pub fn new(batch: WriteBatch) -> (Self, Receiver<Result<()>>) {
        let (tx, rx) = sync_channel(1);
        let this = BatchTask {
            batch,
            signal: vec![tx],
        };
        (this, rx)
    }

    pub fn batch(&self) -> &WriteBatch {
        &self.batch
    }

    pub fn signal(&self) -> &[SyncSender<Result<()>>] {
        &self.signal
    }
}
