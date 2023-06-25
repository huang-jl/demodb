use demo_core::{Error, Result};
use reqwest::{header::HeaderValue, Client as RawClient, Response};

pub struct Client {
    client: RawClient,
    base_url: String,
}

// pub struct RemoteTxn {
//     write_batch: WriteBatch,
//     txn_id: u64,
// }

impl Client {
    /// Create a new DemoDB client
    /// - `base_url`: baseurl of DemoDB server
    pub fn new(base_url: &str) -> Self {
        let base_url = base_url.trim_end_matches("/").to_owned();
        Self {
            client: RawClient::new(),
            base_url,
        }
    }

    pub async fn get_raw(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let target_url = format!("{}/get", self.base_url);
        let res: Response = self
            .client
            .post(target_url)
            .body(key)
            .send()
            .await
            .map_err(|e| Error::Client(e.to_string()))?;
        match res.headers().get("X-key-exist") {
            Some(val) => {
                if val == HeaderValue::from(1) {
                    let res = res
                        .bytes()
                        .await
                        .map_err(|e| Error::Client(e.to_string()))?;
                    Ok(Some(res.to_vec()))
                } else {
                    Ok(None)
                }
            }
            _ => Err(Error::Client(
                "invalid response: withour x-key-exist header".to_owned(),
            )),
        }
    }

    pub async fn put_raw(&self, mut key: Vec<u8>, mut value: Vec<u8>) -> Result<()> {
        let target_url = format!("{}/put", self.base_url);
        let key_len = key.len();
        key.append(&mut value);

        self.client
            .post(target_url)
            .header("X-key-length", key_len)
            .body(key)
            .send()
            .await
            .map_err(|e| Error::Client(e.to_string()))?;
        Ok(())
    }

    pub async fn del_raw(&self, key: Vec<u8>) -> Result<()> {
        let target_url = format!("{}/del", self.base_url);

        self.client
            .post(target_url)
            .body(key)
            .send()
            .await
            .map_err(|e| Error::Client(e.to_string()))?;
        Ok(())
    }

    pub async fn put(&self, key: &str, val: i32) -> Result<()> {
        let raw_key = key.as_bytes().to_vec();
        self.put_raw(raw_key, val.to_le_bytes().to_vec()).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<i32>> {
        let raw_key = key.as_bytes().to_vec();
        let res = self
            .get_raw(raw_key)
            .await?
            .map(|raw_val| i32::from_le_bytes(raw_val.try_into().unwrap()));
        Ok(res)
    }

    pub async fn del(&self, key: &str) -> Result<()> {
        let raw_key = key.as_bytes().to_vec();
        self.del_raw(raw_key).await
    }

    pub async fn update<F>(&self, key: &str, f: F) -> Result<()>
    where
        F: FnOnce(i32) -> i32,
    {
        match self.get(key).await? {
            Some(val) => {
                self.put(key, f(val)).await?;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

// impl Client {
//     pub async fn begin_txn(&self) -> Result<RemoteTxn> {
//         let target_url = format!("{}/begin-txn", self.base_url);
//         let res = self
//             .client
//             .post(target_url)
//             .send()
//             .await
//             .map_err(|e| Error::Client(e.to_string()))?
//             .text()
//             .await
//             .map_err(|e| Error::Client(e.to_string()))?;
//         let txn_id = res
//             .parse::<u64>()
//             .map_err(|e| Error::Client(format!("parse txn id failed {:?}", e)))?;
//         Ok(RemoteTxn {
//             txn_id,
//             write_batch: WriteBatch::new(),
//         })
//     }
// }
