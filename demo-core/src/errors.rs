use std::io;
use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("io error: {0}")]
    IO(Arc<io::Error>),
    #[error("data corrupt: {0}")]
    Corrupt(String),
    #[error("data size is too much: {0}")]
    Exceed(String),
    #[error("reach end of file")]
    EOF,
    #[error("key order not matched")]
    KeyOrder,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("config error: {0}")]
    Config(String),
    #[error("client error: {0}")]
    Client(String),

    #[error("Txn has conflict: {0}")]
    TxnConflict(String)
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::IO(Arc::new(value))
    }
}

impl Error {
    pub fn new_io_err(e: io::Error) -> Self {
        Self::IO(Arc::new(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

macro_rules! map_io_res {
    ($result:expr) => {
        match $result {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::IO(std::sync::Arc::new(e))),
        }
    };
}
