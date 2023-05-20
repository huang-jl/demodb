use demodb::{DemoDB, Result, Storage};

pub fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
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
        None => return Ok(()),
    }
}
