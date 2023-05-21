
/// this is an iterator abstraction for iterate through a KV data structure
/// for now it is a forward-only iterator
pub trait Iterator {
    fn valid(&self) -> bool;
    /// seek the iterator to the first entry whose key >= target
    fn seek(&mut self, target: &[u8]);
    fn to_first(&mut self);
    fn next(&mut self);

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}