use super::{
    block::{Block, BlockBuilder, BlockIterator},
    BlockHandle, Footer,
};
use crate::{config::Config, iterator::Iterator, storage::File, Result};
use std::{collections::HashMap, sync::Mutex};

fn read_block<F: File>(f: &F, handle: BlockHandle) -> Result<Block> {
    let mut buf = vec![0_u8; handle.size as _];
    f.read_exact_at(&mut buf, handle.offset as u64)?;
    Ok(Block::new(buf))
}

/// `Table` is a IMMUTABLE in-memory representaion of an SSTable
pub(crate) struct Table<F: File> {
    f: F,
    // record each block's last key -> to handle
    index_block: Block,
    inner: Mutex<TableInner>,
}

struct TableInner {
    // TODO (huang-jl) use cache instead of a hashmap directly
    data_blocks: HashMap<u32, Block>,
}

impl<F: File> Table<F> {
    pub fn open(file: F) -> Result<Self> {
        let mut buf = [0_u8; Footer::FOOTER_SIZE];
        let file_len = file.len()?;
        file.read_exact_at(&mut buf, file_len - Footer::FOOTER_SIZE as u64)?;
        // first read footer
        let footer = Footer::decode(&buf)?;
        let index_handle = footer.index_handle;
        let index_block = read_block(&file, index_handle)?;

        Ok(Self {
            f: file,
            inner: Mutex::new(TableInner {
                data_blocks: HashMap::new(),
            }),
            index_block,
        })
    }

    /// try to seek at the `Table`.
    ///
    /// - Return the iterator which is seeked at correct position regarding to `key`.
    /// - If no key in table >= query `key`, return None
    pub fn get(&self, key: &[u8]) -> Result<Option<BlockIterator>> {
        let mut index_iter = self.index_block.iter();
        index_iter.seek(key);
        if !index_iter.valid() {
            return Ok(None);
        }
        let data_handle = BlockHandle::decode(index_iter.value())?;
        let data_iter = {
            self.load_data_block(data_handle)?;
            let inner = self.inner.lock().unwrap();
            let data_block = inner.data_blocks.get(&data_handle.offset).unwrap();
            let mut data_iter = data_block.iter();
            data_iter.seek(key);
            if data_iter.valid() {
                Some(data_iter)
            } else {
                None
            }
        };
        Ok(data_iter)
    }

    /// load data block represented by `handle` into memory (i.e. `Table`)
    fn load_data_block(&self, handle: BlockHandle) -> Result<()> {
        let inner = &mut *self.inner.lock().unwrap();
        let data_blocks = &mut inner.data_blocks;
        if !data_blocks.contains_key(&handle.offset) {
            let block = read_block(&self.f, handle)?;
            data_blocks.insert(handle.offset, block);
        }
        Ok(())
    }
}

/// TableBuilder is a helper struct used to build a disk-format SST
pub(crate) struct TableBuilder<F: File> {
    f: F,
    // index block build: last_key -> Data BlockHandle
    index: BlockBuilder,
    // data block builder
    block_builder: BlockBuilder,
    accumulate_size: usize,
    config: Config,
}

impl<F: File> TableBuilder<F> {
    pub fn new(file: F, config: Config) -> Self {
        Self {
            f: file,
            index: BlockBuilder::new(),
            block_builder: BlockBuilder::new(),
            accumulate_size: 0,
            config,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.block_builder.add(key, value)?;
        if self.block_builder.size() >= self.config.block_size() {
            self.accumulate_curr_block()?;
        }
        Ok(())
    }

    /// flush the block_builder's buffer into underline storage
    /// it will also update the index block
    fn accumulate_curr_block(&mut self) -> Result<()> {
        let (last_key, buf) = self.block_builder.finish();
        let data_handle = BlockHandle::new(self.accumulate_size as _, buf.len() as _);
        let mut data_handle_buf = vec![];
        data_handle.encode(&mut data_handle_buf);
        self.index.add(last_key, &data_handle_buf)?;

        self.accumulate_size += buf.len();
        self.f.write(buf)?;
        self.block_builder.reset();
        Ok(())
    }

    /// return the sst table size
    pub fn finish(&mut self, sync: bool) -> Result<usize> {
        if self.block_builder.size() > 0 {
            self.accumulate_curr_block()?;
        }
        // write index block
        let (_, buf) = self.index.finish();
        let index_handle = BlockHandle::new(self.accumulate_size as _, buf.len() as _);
        self.accumulate_size += buf.len();
        self.f.write(buf)?;
        // write Footer
        let footer = Footer::new(index_handle);
        let mut footer_buf = Vec::with_capacity(Footer::FOOTER_SIZE);
        footer.encode(&mut footer_buf);
        self.accumulate_size += footer_buf.len();
        self.f.write(&footer_buf)?;
        if sync {
            self.f.flush()?;
        }
        Ok(self.accumulate_size)
    }

    pub fn close(mut self) -> Result<()> {
        self.f.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
    use std::{
        collections::BTreeMap,
        fs::{remove_file, OpenOptions},
        ops::Bound,
    };
    fn random_bytes<R: Rng + ?Sized>(rng: &mut R, lower: usize, upper: usize) -> Vec<u8> {
        let length = rng.gen_range(lower..=upper);
        rng.sample_iter(Alphanumeric).take(length).collect()
    }

    #[test]
    fn table_simple_test() {
        let mut golden = BTreeMap::new();
        let mut rng = StdRng::seed_from_u64(0xdaedbeef);
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .open("test.table")
            .unwrap();
        let config = Config {
            block_size: 128,
            memtable_size: 1024 * 4,
        };
        let mut table_builder = TableBuilder::new(f, config);
        for _ in 0..1000 {
            let key = random_bytes(&mut rng, 4, 8);
            let value = random_bytes(&mut rng, 128, 512);
            golden.insert(key, value);
        }
        // make sure table is ordered
        for (k, v) in golden.iter() {
            table_builder.add(&k, &v).unwrap();
        }
        table_builder.finish(true).unwrap();
        table_builder.close().unwrap();

        let f = OpenOptions::new().read(true).open("test.table").unwrap();
        let table = Table::open(f).unwrap();

        for (key, value) in golden.iter() {
            let iter = table.get(key).unwrap().unwrap();
            assert!(iter.valid());
            assert_eq!(iter.value(), value);
        }

        for _ in 0..10000 {
            let key = random_bytes(&mut rng, 2, 10);
            let target = {
                let mut r =
                    golden.range::<[u8], _>((Bound::Included(key.as_slice()), Bound::Unbounded));
                r.next()
            };
            let iter = table.get(&key).unwrap();
            if target.is_none() {
                assert!(iter.is_none());
            } else {
                let target = target.unwrap();
                let iter = iter.unwrap();
                assert!(iter.valid());
                assert_eq!(iter.key(), target.0);
                assert_eq!(iter.value(), target.1);
            }
        }

        remove_file("test.table").unwrap();
    }
}
