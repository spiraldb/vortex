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

    pub fn contains_idx(&self, idx: usize) -> bool {
        match self {
            Projection::All => true,
            Projection::Partial(idxs) => idxs.contains(&idx),
        }
    }
}

impl From<Vec<usize>> for Projection {
    fn from(indices: Vec<usize>) -> Self {
        Self::Partial(indices)
    }
}
