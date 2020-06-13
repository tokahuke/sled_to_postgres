use failure_derive::Fail;
use std::io;

/// Error kind for `sled_to_postgres`.
#[derive(Debug, Fail)]
pub enum Error {
    /// An underlying error from `sled`.
    #[fail(display = "sled error: {}", _0)]
    Sled(sled::Error),
    /// An underlying IO error.
    #[fail(display = "io error: {}", _0)]
    Io(io::Error),
    /// An underlying error from Postgres.
    #[fail(display = "postgres error: {}", _0)]
    Postgres(tokio_postgres::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

impl From<sled::Error> for Error {
    fn from(error: sled::Error) -> Error {
        Error::Sled(error)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(error: tokio_postgres::Error) -> Error {
        Error::Postgres(error)
    }
}
