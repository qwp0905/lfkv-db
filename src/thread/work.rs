use std::{any::Any, fmt::Debug};

use crossbeam::channel::Sender;

use crate::{logger, Result};

pub enum Context<T, R> {
  Work((T, Sender<Result<R>>)),
  Finalize,
  Term,
}

pub fn handle_panic<E>(err: E)
where
  E: Any + Debug,
{
  logger::error(format!("panic in safe work {:?}", err))
}
