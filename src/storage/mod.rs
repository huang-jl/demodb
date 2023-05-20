use crate::{Error, Result};
use std::{io::SeekFrom, path::{Path, PathBuf}};

pub mod file;

pub trait Storage: Send + Sync {
    type F: File;
    fn create<P: AsRef<Path>>(&self, path: P) -> Result<Self::F>;
    fn open<P: AsRef<Path>>(&self, path: P) -> Result<Self::F>;
    fn remove<P: AsRef<Path>>(&self, path: P) -> Result<()>;
    fn rename<P: AsRef<Path>>(&self, old: P, new: P) -> Result<()>;
    /// Returns a list of the full-path to each file in given directory
    fn list<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<PathBuf>>;
    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> Result<()>;            
}

pub trait File: Send + Sync {
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
    fn len(&self) -> Result<u64>;
    fn flush(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;

    /// Reads bytes from an offset in this source into a buffer, returning how
    /// many bytes were read.
    ///
    /// This function may yield fewer bytes than the size of `buf`, if it was
    /// interrupted or hit the "EOF".
    ///
    /// See [`Read::read()`](https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read)
    /// for details.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Reads the exact number of bytes required to fill `buf` from an `offset`.
    ///
    /// Errors if the "EOF" is encountered before filling the buffer.
    ///
    /// See [`Read::read_exact()`](https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact)
    /// for details.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) => match e {
                    Error::IO(err) => {
                        if err.kind() != std::io::ErrorKind::Interrupted {
                            return Err(Error::IO(err));
                        }
                        // retry if interrupted error
                    }
                    _ => return Err(e),
                },
            }
        }
        if !buf.is_empty() {
            let e = std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            );
            Err(Error::new_io_err(e))
        } else {
            Ok(())
        }
    }
}

pub(crate) enum FileType {
    Log,
    SSTable,
    Inventory,
    InventoryTmp,
    Unknown,
}
