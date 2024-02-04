#[derive(Debug)]
pub struct CommitInfo {
  pub tx_id: usize,
  pub commit_index: usize,
}

impl CommitInfo {
  pub fn new(tx_id: usize, commit_index: usize) -> Self {
    Self {
      tx_id,
      commit_index,
    }
  }
}
impl AsRef<CommitInfo> for CommitInfo {
  fn as_ref(&self) -> &CommitInfo {
    self
  }
}
impl Clone for CommitInfo {
  fn clone(&self) -> Self {
    Self::new(self.tx_id, self.commit_index)
  }
}
