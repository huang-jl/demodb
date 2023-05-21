use crate::storage::{File, Storage};
use crate::{Error, Result};
use std::fs::{create_dir_all, read_dir, remove_file, rename, File as SysFile, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Default)]
pub struct FileStorage;

impl FileStorage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Storage for FileStorage {
    type F = SysFile;
    fn create<P: AsRef<Path>>(&self, name: P) -> Result<Self::F> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(name)
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::new_io_err(e)),
        }
    }

    fn open<P: AsRef<Path>>(&self, name: P) -> Result<Self::F> {
        match OpenOptions::new().write(true).read(true).open(name) {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::new_io_err(e)),
        }
    }

    fn remove<P: AsRef<Path>>(&self, name: P) -> Result<()> {
        let r = remove_file(name);
        map_io_res!(r)
    }

    fn rename<P: AsRef<Path>>(&self, old: P, new: P) -> Result<()> {
        map_io_res!(rename(old, new))
    }

    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> Result<()> {
        let r = create_dir_all(dir);
        map_io_res!(r)
    }

    fn list<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<PathBuf>> {
        if dir.as_ref().is_dir() {
            let mut v = vec![];
            match read_dir(dir) {
                Ok(rd) => {
                    for entry in rd {
                        match entry {
                            Ok(p) => v.push(p.path()),
                            Err(e) => return Err(Error::new_io_err(e)),
                        }
                    }
                    return Ok(v);
                }
                Err(e) => return Err(Error::new_io_err(e)),
            }
        }
        Ok(vec![])
    }
}

impl File for SysFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        map_io_res!(Write::write(self, buf))
    }

    fn flush(&mut self) -> Result<()> {
        map_io_res!(Write::flush(self))
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        map_io_res!(Seek::seek(self, pos))
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        map_io_res!(Read::read(self, buf))
    }

    fn len(&self) -> Result<u64> {
        match SysFile::metadata(self) {
            Ok(v) => Ok(v.len()),
            Err(e) => Err(Error::new_io_err(e)),
        }
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let r = std::os::unix::prelude::FileExt::read_at(self, buf, offset);
        map_io_res!(r)
    }
}
