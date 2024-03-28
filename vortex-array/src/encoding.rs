use crate::compress::EncodingCompression;
use crate::serde::EncodingSerde;
use crate::view::ArrayViewVTable;
use linkme::distributed_slice;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str);

impl EncodingId {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        self.0
    }
}

impl Display for EncodingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0, f)
    }
}

pub trait Encoding: Debug + Send + Sync + 'static {
    fn id(&self) -> EncodingId;

    /// Whether this encoding provides a compressor.
    fn compression(&self) -> Option<&dyn EncodingCompression> {
        None
    }

    /// Array serialization
    fn serde(&self) -> Option<&dyn EncodingSerde> {
        None
    }

    fn view_vtable(&self) -> Option<&dyn ArrayViewVTable> {
        None
    }
}

impl Display for dyn Encoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

pub type EncodingRef = &'static dyn Encoding;

impl PartialEq<Self> for EncodingRef {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for EncodingRef {}

impl Hash for EncodingRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

#[distributed_slice]
pub static ENCODINGS: [EncodingRef] = [..];

pub fn find_encoding(id: &str) -> Option<EncodingRef> {
    ENCODINGS.iter().find(|&x| x.id().name() == id).cloned()
}
