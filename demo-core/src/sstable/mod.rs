use crate::{Error, Result};

mod block;
mod table;

pub(crate) use table::Table;
pub(crate) use table::TableBuilder;
pub(crate) use block::BlockIterator;

// our format is straight-forward: without the prefix compression
//
// [block-1][block-2][block-3]...[block-n][index-block][footer]
// all keys inside block is sorted
// all keys between block is also sorted
// Within block:
// [(len(k), k, len(v), v), (len(k), k, len(v), v), ...]
// index-block
// [(lastkey-1, handle), (lastkey-2, handle), ...]
// lastkey-n means the last key (i.e. highest key in block-n) and its handle
// footer contains a index handle

#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockHandle {
    offset: u32,
    size: u32,
}

impl BlockHandle {
    const BLOCK_HANDLE_SIZE: usize = 8;
    pub fn new(offset: u32, size: u32) -> Self {
        Self { offset, size }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.size.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<BlockHandle> {
        if buf.len() < Self::BLOCK_HANDLE_SIZE {
            return Err(Error::Corrupt(
                "not enough bytes for block handle".to_owned(),
            ));
        }

        let offset = u32::from_le_bytes(buf[..4].try_into().unwrap());
        let size = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        Ok(Self { offset, size })
    }
}

pub(crate) struct Footer {
    index_handle: BlockHandle,
}

impl Footer {
    const FOOTER_SIZE: usize = BlockHandle::BLOCK_HANDLE_SIZE;

    pub fn new(index_handle: BlockHandle) -> Self {
        Self { index_handle }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        self.index_handle.encode(buf);
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        let index_handle = BlockHandle::decode(buf)?;
        Ok(Self { index_handle })
    }
}
