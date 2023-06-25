use demo_core::{DemoDB, FileStorage, Txn, WriteBatch};
use rand::{
    prelude::{SliceRandom, StdRng},
    SeedableRng,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

mod common;

use common::txn;
use common::{get, put};

#[test]
fn multiple_put_del_in_mt() {
    common::init();
    common::remove("./test1-dir");

    let storage = FileStorage;
    let db = DemoDB::open("./test1-dir", storage, "./config.json").unwrap();
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
    common::remove("./test2-dir");

    let storage = FileStorage;
    let db = DemoDB::open("./test2-dir", storage, "./config.json").unwrap();
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
fn simple_txn() {
    common::init();
    common::remove("./test3-dir");

    let storage = FileStorage;
    let db = DemoDB::open("./test3-dir", storage, "./config.json").unwrap();

    put(&db, "A", 0).unwrap();
    put(&db, "B", 0).unwrap();

    for _ in 0..100 {
        let mut txn = db.begin_txn();
        for _ in 0..4 {
            txn::update(&mut txn, "A", |val| val + 1).unwrap();
            txn::update(&mut txn, "B", |val| val + 1).unwrap();
        }
        txn.commit().unwrap();
    }

    let a = get(&db, "A").unwrap();
    let b = get(&db, "B").unwrap();
    assert_eq!(a, 400);
    assert_eq!(b, 400);
}

#[test]
fn case4() {
    const EXEC_TIME: Duration = Duration::from_secs(60);
    common::init();
    common::remove("./test4-dir");

    let storage = FileStorage;
    let db = DemoDB::open("./test4-dir", storage, "./config.json").unwrap();

    let step1 = |mut txn: Txn<FileStorage>| {
        for _ in 0..2 {
            match txn::update(&mut txn, "A", |val| val + 1) {
                Ok(_) => {}
                Err(demo_core::Error::TxnConflict(..)) => return false,
                _ => panic!(),
            }

            match txn::update(&mut txn, "B", |val| val + 1) {
                Ok(_) => {}
                Err(demo_core::Error::TxnConflict(..)) => return false,
                _ => panic!(),
            }
        }
        txn.commit().unwrap();
        true
    };
    let step2 = |mut txn: Txn<FileStorage>| {
        let a = match txn::get(&mut txn, "A") {
            Ok(val) => val.unwrap(),
            Err(demo_core::Error::TxnConflict(..)) => return false,
            _ => panic!(),
        };

        let b = match txn::get(&mut txn, "B") {
            Ok(val) => val.unwrap(),
            Err(demo_core::Error::TxnConflict(..)) => return false,
            _ => panic!(),
        };
        assert_eq!(a, b);
        txn.commit().unwrap();
        true
    };
    let step3 = step2;
    let step4 = |mut txn: Txn<FileStorage>| {
        match txn::update(&mut txn, "A", |val| val - 1) {
            Ok(_) => {}
            Err(demo_core::Error::TxnConflict(..)) => return false,
            _ => panic!(),
        }

        match txn::update(&mut txn, "B", |val| val - 1) {
            Ok(_) => {}
            Err(demo_core::Error::TxnConflict(..)) => return false,
            _ => panic!(),
        }
        txn.commit().unwrap();
        true
    };

    put(&db, "A", 0).unwrap();
    put(&db, "B", 0).unwrap();

    let mut handles = vec![];
    for _ in 0..3 {
        let t = {
            let db = db.clone();
            std::thread::spawn(move || {
                let begin = Instant::now();
                let mut success = [0, 0, 0, 0];
                loop {
                    if begin.elapsed() > EXEC_TIME {
                        break;
                    }
                    if step1(db.begin_txn()) {
                        success[0] += 1;
                    }
                    if step2(db.begin_txn()) {
                        success[1] += 1;
                    }
                    if step3(db.begin_txn()) {
                        success[2] += 1;
                    }
                    if step4(db.begin_txn()) {
                        success[3] += 1
                    }
                }
                success
            })
        };
        handles.push(t);
    }

    let success = handles.into_iter().fold([0, 0, 0, 0], |mut acc, h| {
        let res = h.join().unwrap();
        for (a, r) in acc.iter_mut().zip(res.iter()) {
            *a += *r;
        }
        acc
    });

    let a = common::get(&db, "A").unwrap();
    let b = common::get(&db, "B").unwrap();
    println!("success txns: {success:?}, A = {}, B = {}", a, b);

    assert_eq!(a, success[0] * 2 - success[3]);
    assert_eq!(b, success[0] * 2 - success[3]);
}

#[test]
fn case5() {
    const EXEC_TIME: Duration = Duration::from_secs(60);
    common::init();
    common::remove("./test5-dir");

    let storage = FileStorage;
    let db = DemoDB::open("./test5-dir", storage, "./config.json").unwrap();

    put(&db, "A", 0).unwrap();
    put(&db, "B", 0).unwrap();

    let step1 = |mut txn: Txn<FileStorage>| {
        match txn::update(&mut txn, "A", |val| val + 1) {
            Ok(_) => {}
            Err(demo_core::Error::TxnConflict(..)) => return false,
            _ => assert!(false),
        }
        match txn::update(&mut txn, "B", |val| val + 1) {
            Ok(_) => {}
            Err(demo_core::Error::TxnConflict(..)) => return false,
            _ => assert!(false),
        }

        let b = txn::get(&mut txn, "B").unwrap().unwrap();
        txn::put(&mut txn, "A", b + 1).unwrap();

        let a = txn::get(&mut txn, "A").unwrap().unwrap();
        txn::put(&mut txn, "B", a + 1).unwrap();

        assert_eq!(a, b + 1);

        txn.commit().unwrap();
        true
    };

    let step2 = |mut txn: Txn<FileStorage>| {
        let _ = match txn::get(&mut txn, "A") {
            Ok(v) => v.unwrap(),
            Err(demo_core::Error::TxnConflict(..)) => return false,
            Err(e) => panic!("{}", e),
        };
        let _ = match txn::get(&mut txn, "B") {
            Ok(v) => v.unwrap(),
            Err(demo_core::Error::TxnConflict(..)) => return false,
            Err(e) => panic!("{}", e),
        };

        let a = txn::get(&mut txn, "A").unwrap().unwrap();
        let b = txn::get(&mut txn, "B").unwrap().unwrap();
        if a + 1 != b {
            println!("a = {a}, b = {b}");
        }
        assert_eq!(a + 1, b, "a = {}, b = {}", a, b);
        txn.commit().unwrap();
        true
    };

    let mut g1 = vec![];
    for _ in 0..2 {
        let db = db.clone();
        let t = std::thread::spawn(move || {
            let begin = Instant::now();
            let mut success = 0;
            let mut failed = 0;
            loop {
                if begin.elapsed() >= EXEC_TIME {
                    break;
                }
                if step1(db.begin_txn()) {
                    success += 1;
                } else {
                    failed += 1;
                }
            }
            println!("step1 failed {failed}");
            success
        });
        g1.push(t);
    }

    let mut g2 = vec![];
    for _ in 0..8 {
        let db = db.clone();
        let t = std::thread::spawn(move || {
            let begin = Instant::now();
            let mut success = 0;
            loop {
                if begin.elapsed() >= EXEC_TIME {
                    break;
                }
                if step2(db.begin_txn()) {
                    success += 1;
                }
                std::thread::sleep(Duration::from_micros(5));
            }
            success
        });
        g2.push(t);
    }

    let success = g1.into_iter().fold(0, |acc, h| acc + h.join().unwrap());
    println!("read write txn success {}", success);
    println!(
        "read only txn success {}",
        g2.into_iter().fold(0, |acc, h| acc + h.join().unwrap())
    );

    let a = get(&db, "A").unwrap();
    let b = get(&db, "B").unwrap();

    assert_eq!(a, success * 3 - 1);
    assert_eq!(b, a + 1);
}
