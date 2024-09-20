use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// SharedVec provides shared access to a collection, along with the ability to create owned
/// slices of the collection with zero copying.
#[derive(Clone)]
pub struct SharedVec<T> {
    data: Arc<[T]>,
    start: usize,
    len: usize,
}

impl<T> std::ops::Deref for SharedVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        // SAFETY: the pointer only points at memory contained within owned `data`.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(self.start), self.len) }
    }
}

impl<T: Debug> Debug for SharedVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedVec")
            .field("start", &self.start)
            .field("len", &self.len)
            .field("data", &self.data)
            .finish()
    }
}

impl<T> From<Arc<[T]>> for SharedVec<T> {
    fn from(value: Arc<[T]>) -> Self {
        Self {
            len: value.len(),
            start: 0,
            data: value,
        }
    }
}

impl<T> From<Vec<T>> for SharedVec<T> {
    fn from(value: Vec<T>) -> Self {
        // moves the data from the Vec into a new owned slice.
        let data: Arc<[T]> = Arc::from(value);

        SharedVec::from(data)
    }
}

impl<T> SharedVec<T> {
    /// Create a new slice of the given vec, without copying or allocation.
    pub fn slice(&self, start: usize, end: usize) -> Self {
        assert!(end <= self.len, "cannot slice beyond end of SharedVec");

        Self {
            data: self.data.clone(),
            start: self.start + start,
            len: end - start,
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::arc_slice::SharedVec;

    #[test]
    fn test_simple() {
        let data = vec!["alice".to_string(), "bob".to_string(), "carol".to_string()];
        let data: Arc<[String]> = data.into();
        let shared_vec: SharedVec<String> = data.into();

        // We get iter() for free via the Deref to slice!
        assert_eq!(
            shared_vec.iter().collect::<Vec<_>>(),
            vec!["alice", "bob", "carol"],
        );
    }

    #[test]
    fn test_slicing() {
        let data = vec!["alice".to_string(), "bob".to_string(), "carol".to_string()];
        let data: Arc<[String]> = data.into();
        let shared_vec: SharedVec<String> = data.into();

        // Original array
        assert_eq!(shared_vec.len(), 3);

        // Sliced once
        let sliced_vec = shared_vec.slice(1, 3);
        assert_eq!(sliced_vec.len(), 2);
        assert_eq!(sliced_vec.iter().collect::<Vec<_>>(), vec!["bob", "carol"]);

        // Sliced again
        let sliced_again = sliced_vec.slice(1, 2);
        assert_eq!(sliced_again.len(), 1);
        assert_eq!(sliced_again.iter().collect::<Vec<_>>(), vec!["carol"]);
    }

    #[test]
    fn test_deref() {
        let data = vec!["alice".to_string(), "bob".to_string(), "carol".to_string()];
        let data: Arc<[String]> = data.into();
        let shared_vec: SharedVec<String> = data.into();

        assert_eq!(&shared_vec[0], "alice");
        assert_eq!(&shared_vec[1], "bob");
        assert_eq!(&shared_vec[2], "carol");
    }
}
