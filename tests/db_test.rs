use demodb::{DemoDB, FileStorage, WriteBatch};
use rand::{
    prelude::{SliceRandom, StdRng},
    SeedableRng,
};

mod common;

#[test]
fn multiple_put_del_in_mt() {
    common::init();

    let storage = FileStorage;
    let db = DemoDB::open("./", storage, "./config.json").unwrap();
    let mut rng = StdRng::seed_from_u64(0xdaedbeef);

    // 1. first write some kv
    let mut all_key = (0..1000_u32).collect::<Vec<_>>();
    all_key.shuffle(&mut rng);

    // we use chunk, so that it will trigger writing sst
    for chunk in all_key.chunks(7) {
        let mut batch = WriteBatch::new();
        for i in chunk {
            let key = i.to_le_bytes();
            let value = format!("{i}").repeat(1027);
            batch.put(&key, value.as_bytes());
        }
        db.write_batch(batch).unwrap();
    }

    // 2. delete some kv
    let deleted_key: Vec<u32> = vec![1, 100, 514];
    let mut batch = WriteBatch::new();
    for dk in deleted_key.iter() {
        batch.del(&dk.to_le_bytes());
    }
    db.write_batch(batch).unwrap();

    // 3. check kv and the deleted tuple is supposed to read from memtable directly
    for i in 0..1024_u32 {
        let res = db.get(&i.to_le_bytes());
        if i >= 1000 || deleted_key.contains(&i) {
            assert_eq!(res, None, "i = {i}");
        } else {
            let value = format!("{i}").repeat(1027);
            assert_eq!(
                res.as_ref().map(|v| v.as_slice()),
                Some(value.as_bytes()),
                "i = {i}"
            )
        }
    }
}

#[test]
fn multiple_put_del_in_sst() {
    common::init();

    let storage = FileStorage;
    let db = DemoDB::open("./", storage, "./config.json").unwrap();
    let mut rng = StdRng::seed_from_u64(0xdaedbeef);

    let mut all_key = (0..520_u32).collect::<Vec<_>>();
    all_key.shuffle(&mut rng);

    // 1. first write some kv
    // we use chunk, so that it will trigger writing sst
    for chunk in all_key.chunks(10) {
        let mut batch = WriteBatch::new();
        for i in chunk {
            let key = i.to_le_bytes();
            let value = format!("{i}").repeat(1027);
            batch.put(&key, value.as_bytes());
        }
        db.write_batch(batch).unwrap();
    }

    // 2. delete some kv
    let deleted_key: Vec<u32> = vec![1, 100, 514];
    let mut batch = WriteBatch::new();
    for dk in deleted_key.iter() {
        batch.del(&dk.to_le_bytes());
    }
    db.write_batch(batch).unwrap();

    // 3. write more keys to flush delete entry into sst
    let mut all_key = (520..1000_u32).collect::<Vec<_>>();
    all_key.shuffle(&mut rng);
    for chunk in all_key.chunks(10) {
        let mut batch = WriteBatch::new();
        for i in chunk {
            let key = i.to_le_bytes();
            let value = format!("{i}").repeat(1027);
            batch.put(&key, value.as_bytes());
        }
        db.write_batch(batch).unwrap();
    }

    // 3. check kv and the deleted tuple is supposed to read from sstable
    for i in 0..1024_u32 {
        let res = db.get(&i.to_le_bytes());
        if i >= 1000 || deleted_key.contains(&i) {
            assert_eq!(res, None, "i = {i}");
        } else {
            let value = format!("{i}").repeat(1027);
            assert_eq!(
                res.as_ref().map(|v| v.as_slice()),
                Some(value.as_bytes()),
                "i = {i}"
            )
        }
    }
}

#[test]
fn case1() {
    use common::*;
    init();

    let storage = FileStorage;
    let db = DemoDB::open("./case1/", storage, "./config.json").unwrap();
    put(&db, "A", 3).unwrap();
    put(&db, "B", 4).unwrap();

    update(&db, "A", |res| res + 1).unwrap();
    update(&db, "B", |res| res + 1).unwrap();

    assert_eq!(get(&db, "A"), Some(4));
    assert_eq!(get(&db, "B"), Some(5));

    del(&db, "A").unwrap();
    del(&db, "B").unwrap();

    put(&db, "A", 5).unwrap();
    assert_eq!(get(&db, "A"), Some(5));
    assert_eq!(get(&db, "B"), None);

    put(&db, "B", 5).unwrap();
    assert_eq!(get(&db, "B"), Some(5));
}

#[test]
fn case2() {
    use common::*;
    init();
    let storage = FileStorage;
    let db = DemoDB::open("./case1/", storage, "./config.json").unwrap();

    assert_eq!(get(&db, "A"), Some(5));
    assert_eq!(get(&db, "B"), Some(5));
}

#[test]
fn case3() {
    use common::*;
    init();
    let storage = FileStorage;
    let db = DemoDB::open("./case1/", storage, "./config.json").unwrap();

}