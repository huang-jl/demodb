use bytes::Bytes;
use demo_core::{DemoDB, FileStorage, Result};
use std::net::SocketAddr;
use std::path::Path;
use warp::{
    self,
    http::{Response, StatusCode},
    Filter,
};

const KEY_SIZE_LIMIT: u64 = 16 * 1024;
const VALUE_SIZE_LIMIT: u64 = 1 * 1024 * 1024;

pub async fn start_daemon(
    path: impl AsRef<Path>,
    config_path: impl AsRef<Path>,
    addr: SocketAddr,
) -> Result<()> {
    let storage = FileStorage::new();
    let db: DemoDB<_> = DemoDB::open(path, storage, config_path)?;
    // get request
    // for now we only support 16KB keys and 1MB values at most
    let get = {
        let db = db.clone();
        warp::post()
            .and(warp::path("get"))
            .and(warp::body::content_length_limit(KEY_SIZE_LIMIT))
            .and(warp::body::bytes())
            .map(move |key: Bytes| {
                let response = Response::builder().status(StatusCode::OK);
                let res = match db.get(key.as_ref()) {
                    Some(val) => response.header("X-key-exist", 1).body(val),

                    None => response.header("X-key-exist", 0).body(vec![]),
                };
                res
            })
    };

    let put = {
        let db = db.clone();
        warp::post()
            .and(warp::path("put"))
            .and(warp::body::content_length_limit(
                KEY_SIZE_LIMIT + VALUE_SIZE_LIMIT,
            ))
            .and(warp::header::<usize>("X-key-length"))
            .and(warp::body::bytes())
            .map(move |key_len, data: Bytes| {
                let response = Response::builder();
                let (key, value) = data.split_at(key_len);
                let res = match db.put(key, value) {
                    Ok(_) => response.status(StatusCode::OK).body("ok".to_owned()),
                    Err(e) => response
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("error occur when put {e}")),
                };
                res
            })
    };

    let del = {
        let db = db.clone();
        warp::post()
            .and(warp::path("del"))
            .and(warp::body::content_length_limit(KEY_SIZE_LIMIT))
            .and(warp::body::bytes())
            .map(move |key: Bytes| {
                let response = Response::builder();
                let res = match db.del(key.as_ref()) {
                    Ok(_) => response.status(StatusCode::OK).body("ok".to_owned()),
                    Err(e) => response
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("error occur when del {e}")),
                };
                res
            })
    };

    warp::serve(get.or(put).or(del)).run(addr).await;

    Ok(())
}
