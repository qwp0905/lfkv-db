use std::{any::Any, error, io, result};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("not found")]
  NotFound,

  #[error("invalid format")]
  InvalidFormat(&'static str),

  #[error("io error")]
  IO(io::Error),

  #[error("end of file")]
  EOF,

  #[error("transaction already closed")]
  TransactionClosed,

  #[error("engine unavailable")]
  EngineUnavailable,

  #[error("worker closed")]
  WorkerClosed,

  #[error("flush failed")]
  FlushFailed,

  #[error("write conflict detected")]
  WriteConflict,

  #[error("thread conflict detected")]
  ThreadConflict,

  #[error("channel disconnected")]
  ChannelDisconnected,

  #[error("panic")]
  Panic(String),

  #[error("unknown")]
  Unknown(String),
}
impl Error {
  pub fn unknown<E>(err: E) -> Self
  where
    E: error::Error + Send + Sync + 'static,
  {
    Self::Unknown(err.to_string())
  }
  pub fn panic(err: Box<dyn Any + Send>) -> Self {
    if let Some(str) = err.downcast_ref::<&str>() {
      return Error::Panic(str.to_string());
    }
    if let Some(str) = err.downcast_ref::<String>() {
      return Error::Panic(str.clone());
    }

    Error::Panic(format!("unknown panic."))
  }
}
impl Clone for Error {
  fn clone(&self) -> Self {
    match self {
      Self::NotFound => Self::NotFound,
      Self::InvalidFormat(err) => Self::InvalidFormat(err),
      Self::Unknown(err) => Self::Unknown(err.clone()),
      Self::IO(err) => Self::IO(io::Error::new(err.kind(), err.to_string())),
      Self::EOF => Self::EOF,
      Self::TransactionClosed => Self::TransactionClosed,
      Self::EngineUnavailable => Self::EngineUnavailable,
      Self::WorkerClosed => Self::WorkerClosed,
      Self::FlushFailed => Self::FlushFailed,
      Self::WriteConflict => Self::WriteConflict,
      Self::ThreadConflict => Self::ThreadConflict,
      Self::ChannelDisconnected => Self::ChannelDisconnected,
      Self::Panic(err) => Self::Panic(err.clone()),
    }
  }
}

pub type Result<T = ()> = result::Result<T, Error>;
