pub struct Projection {
    indices: Vec<usize>,
}

impl Projection {
    pub fn new(indices: impl AsRef<[usize]>) -> Self {
        Projection {
            indices: Vec::from(indices.as_ref()),
        }
    }

    pub fn indices(&self) -> &[usize] {
        self.indices.as_ref()
    }
}

impl From<Vec<usize>> for Projection {
    fn from(indices: Vec<usize>) -> Self {
        Self { indices }
    }
}
