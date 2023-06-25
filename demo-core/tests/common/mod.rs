use std::path::Path;
use std::{fs::remove_dir_all, io::ErrorKind};

use demo_core::{DemoDB, Result, Storage, Txn};

pub fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

pub fn remove(path: impl AsRef<Path>) {
    match remove_dir_all(path) {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => panic!("{}", e),
    }
}

pub fn put<S: Storage + Clone>(db: &DemoDB<S>, key: impl AsRef<str>, value: i32) -> Result<()> {
    db.put(key.as_ref().as_bytes(), &value.to_le_bytes())
}

pub fn get<S: Storage + Clone>(db: &DemoDB<S>, key: impl AsRef<str>) -> Option<i32> {
    db.get(key.as_ref().as_bytes())
        .map(|val| i32::from_le_bytes(val.try_into().unwrap()))
}

pub fn del<S: Storage + Clone>(db: &DemoDB<S>, key: impl AsRef<str>) -> Result<()> {
    db.del(key.as_ref().as_bytes())
}

pub fn update<S: Storage + Clone, F>(db: &DemoDB<S>, key: impl AsRef<str>, f: F) -> Result<()>
where
    F: FnOnce(i32) -> i32,
{
    match get(db, key.as_ref()) {
        Some(res) => put(db, key.as_ref(), f(res)),
        None => Ok(()),
    }
}

pub mod txn {
    use super::*;

    pub fn put<S: Storage + Clone>(
        txn: &mut Txn<S>,
        key: impl AsRef<str>,
        value: i32,
    ) -> Result<()> {
        txn.put(key.as_ref().as_bytes(), &value.to_le_bytes())
    }

    pub fn get<S: Storage + Clone>(txn: &mut Txn<S>, key: impl AsRef<str>) -> Result<Option<i32>> {
        let res = txn
            .get(key.as_ref().as_bytes())?
            .map(|val| i32::from_le_bytes(val.try_into().unwrap()));
        Ok(res)
    }

    pub fn del<S: Storage + Clone>(txn: &mut Txn<S>, key: impl AsRef<str>) -> Result<()> {
        txn.del(key.as_ref().as_bytes())
    }

    pub fn update<S: Storage + Clone, F>(txn: &mut Txn<S>, key: &str, f: F) -> Result<()>
    where
        F: FnOnce(i32) -> i32,
    {
        let val = txn.get(key.as_bytes())?.unwrap();
        let val = i32::from_le_bytes(val.try_into().unwrap());
        txn.put(key.as_bytes(), &f(val).to_le_bytes())?;
        Ok(())
    }
}
