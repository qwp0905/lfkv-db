pub trait Takable
where
  Self: Sized,
{
  fn take(&mut self, t: Self) -> Self {
    std::mem::replace(self, t)
  }

  fn take_default(&mut self) -> Self
  where
    Self: Default,
  {
    std::mem::replace(self, Default::default())
  }
}
