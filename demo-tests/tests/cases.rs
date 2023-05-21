use std::time::Duration;

use demo_client::Client;
use demo_core::{DemoDB, FileStorage};
use demo_daemon::start_daemon;
use tokio;

mod common;

#[test]
fn case1() {
    use common::*;
    init();

    let storage = FileStorage;
    let db = DemoDB::open("./case1-dir/", storage, "config.json").unwrap();
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
    let db = DemoDB::open("./case1-dir/", storage, "config.json").unwrap();

    assert_eq!(get(&db, "A"), Some(5));
    assert_eq!(get(&db, "B"), Some(5));
}

#[tokio::test]
async fn case3() {
    use common::*;
    init();
    let addr = "127.0.0.1:8080";

    {
        let addr = addr.parse().unwrap();
        tokio::spawn(async move {
            start_daemon("./case3-dir/", "config.json", addr).await.unwrap();
        });
    }

    // wait for db daemon to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut handles = vec![];
    for (k1, k2) in [("A", "B"), ("C", "D"), ("E", "F")] {
        let handle = tokio::spawn(async move {
            let client = Client::new(&format!("http://{}", addr));
            for i in 0..10 {
                client.put(k1, i).await.unwrap();
                client.put(k2, i).await.unwrap();
                client.update(k1, |val| val + 1).await.unwrap();
                client.update(k2, |val| val + 1).await.unwrap();
                assert_eq!(client.get(k1).await.unwrap(), Some(i + 1));
                client.del(k1).await.unwrap();

                assert_eq!(client.get(k2).await.unwrap(), Some(i + 1));
                client.del(k2).await.unwrap();

                assert_eq!(client.get(k1).await.unwrap(), None);
                assert_eq!(client.get(k2).await.unwrap(), None);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }
}
