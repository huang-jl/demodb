use crate::{config::*, storage::File, Error, Result};
use log::warn;

const RECORD_HEADER_SIZE: usize = std::mem::size_of::<u32>();

pub(crate) struct Writer<F: File> {
    file: F,
    in_block_offset: usize,
    config: Config,
}

impl<F: File> Writer<F> {
    pub fn new(file: F, config: Config) -> Self {
        Self {
            file,
            in_block_offset: 0,
            config,
        }
    }

    pub fn write_record(&mut self, record: &[u8]) -> Result<()> {
        let block_size = self.config.block_size();
        if record.len() >= u32::MAX as usize {
            return Err(Error::Exceed("writing record too large".to_string()));
        }
        debug_assert!(self.in_block_offset <= block_size);
        // fill 0 if cannot place the HEADER in current block
        if block_size - self.in_block_offset < RECORD_HEADER_SIZE {
            self.file
                .write(&[0_u8; RECORD_HEADER_SIZE][..block_size - self.in_block_offset])?;
            self.in_block_offset = 0;
        }

        // place the HEADER
        let data_length = record.len() as u32;
        self.file.write(&data_length.to_le_bytes())?;
        // write record
        self.file.write(record)?;

        self.in_block_offset += RECORD_HEADER_SIZE + record.len();
        self.in_block_offset = self.in_block_offset % block_size;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()
    }

    pub fn close(&mut self) -> Result<()> {
        self.file.close()
    }
}

impl<F: File> Drop for Writer<F> {
    fn drop(&mut self) {
        self.sync().expect("sync file failed");
        self.file.close().expect("close file failed");
    }
}

pub(crate) struct Reader<F: File> {
    file: F,
    buf: Vec<u8>,
    buf_len: usize,
    eof: bool,
    config: Config,
}

impl<F: File> Reader<F> {
    pub fn new(file: F, config: Config) -> Self {
        Self {
            file,
            buf: vec![],
            buf_len: 0,
            eof: false,
            config,
        }
    }

    fn allocate_and_fill_buf(&mut self) {
        let block_size = self.config.block_size();
        self.buf = vec![0; block_size];
        match self.file.read(&mut self.buf) {
            Ok(length) => {
                if length < block_size {
                    self.eof = true;
                }
                self.buf_len = length;
            }
            Err(_) => {
                warn!("find errors when read record file");
                self.eof = true;
                self.buf_len = 0;
            }
        }
    }

    /// Mainly used to read from a log file (which is created by [`self::Writer`]).
    /// 
    /// Each record represents a [`crate::batch::WriteBatch`]'s raw data.
    /// ## Return Value
    /// - return true means read success
    /// - return false means we reach eof and read failed
    pub fn read_record(&mut self, record: &mut Vec<u8>) -> bool {
        if self.buf_len <= RECORD_HEADER_SIZE {
            self.allocate_and_fill_buf();
            if self.buf_len <= RECORD_HEADER_SIZE {
                debug_assert!(self.eof);
                return false;
            }
        }
        let header = self
            .buf
            .drain(..RECORD_HEADER_SIZE)
            .as_slice()
            .try_into()
            .unwrap();
        self.buf_len -= RECORD_HEADER_SIZE;
        let mut left_length = (u32::from_le_bytes(header)) as usize;
        loop {
            let length = left_length.min(self.buf_len);
            record.extend_from_slice(self.buf.drain(..length).as_slice());
            self.buf_len -= length;

            left_length -= length;
            // when we read a while record
            if left_length == 0 {
                break;
            }
            // we need read more data
            debug_assert_eq!(self.buf_len, 0);
            self.allocate_and_fill_buf();
            if self.buf_len == 0 {
                warn!("reader read corrupt data, need {left_length} bytes more");
                debug_assert!(self.eof);
                return false;
            }
        }
        true
    }
}

impl<F: File> Drop for Reader<F> {
    fn drop(&mut self) {
        self.file.close().expect("close file failed")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::{remove_file, OpenOptions};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn simple_record_wr() {
        init();
        let test = [4097, 69, 4033, 128, 3232, 4096_usize];
        let config = Config {
            block_size: 128,
            ..Default::default()
        };
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open("test.txt")
            .unwrap();
        {
            let mut writer = Writer::new(file, config.clone());
            for (i, &l) in test.iter().enumerate() {
                writer.write_record(&vec![i as u8; l]).unwrap();
            }
        }

        let file = OpenOptions::new().read(true).open("test.txt").unwrap();
        {
            let mut reader = Reader::new(file, config.clone());
            for (i, &l) in test.iter().enumerate() {
                let mut record = vec![];
                assert!(reader.read_record(&mut record));
                assert_eq!(record, vec![i as u8; l]);
            }
            for _ in 0..3 {
                let mut record = vec![];
                assert!(!reader.read_record(&mut record));
                assert_eq!(record, vec![0_u8; 0]);
            }
        }

        remove_file("test.txt").unwrap();
    }
}
