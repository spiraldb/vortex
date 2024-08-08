#[derive(Debug, Clone, Default)]
pub enum Projection {
    #[default]
    All,
    Partial(Vec<usize>),
}

impl Projection {
    pub fn new(indices: impl AsRef<[usize]>) -> Self {
        Self::Partial(Vec::from(indices.as_ref()))
    }
}

impl From<Vec<usize>> for Projection {
    fn from(indices: Vec<usize>) -> Self {
        Self::Partial(indices)
    }
}
