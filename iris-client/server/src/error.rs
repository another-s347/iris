use std::backtrace::Backtrace;

use pyo3::PyDowncastError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("user error")] // should not unwrap at server side
    UserPyError{
        #[from]
        source: pyo3::PyErr,
        backtrace: Backtrace,
    },
    #[error("user python cast error: {msg:#?}")] // should not unwrap at server side
    UserPyCastError {
        msg: String
    },
    #[error("server critical error")]
    ServerError{
        #[from]
        source: anyhow::Error,
        backtrace: Backtrace,
    },
    #[error("tonic error: {source:#?}")]
    TonicError{
        #[from]
        source: tonic::Status,
        backtrace: Backtrace,
    },
    #[error("tokio join error: {source:#?}")]
    JoinError{
        #[from]
        source: tokio::task::JoinError,
        backtrace: Backtrace,
    },
    #[error("tokio timeout error: {source:#?}")]
    TimeoutError{
        #[from]
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    }
}

pub type Result<T> = std::result::Result<T, Error>;