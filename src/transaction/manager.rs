use std::collections::HashMap;

#[derive(Debug)]
pub struct TransactionManager {
  tree_locks: HashMap<usize, ()>,
}
