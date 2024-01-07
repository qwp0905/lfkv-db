pub enum Node {
  Interior(),
  Leaf(),
}

pub struct CursorEntry {
  node: Node,
}
