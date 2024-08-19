use std::collections::HashSet;

use vortex_dtype::field::Field;

#[derive(Debug, Clone, Default)]
pub enum Projection {
    #[default]
    All,
    Flat(Vec<Field>),
}

impl Projection {
    pub fn new(indices: impl AsRef<[usize]>) -> Self {
        Self::Flat(indices.as_ref().iter().copied().map(Field::from).collect())
    }
}

impl From<Vec<Field>> for Projection {
    fn from(indices: Vec<Field>) -> Self {
        Self::Flat(indices)
    }
}

impl From<Vec<usize>> for Projection {
    fn from(indices: Vec<usize>) -> Self {
        Self::Flat(indices.into_iter().map(Field::from).collect())
    }
}

impl Extend<Field> for Projection {
    fn extend<T: IntoIterator<Item = Field>>(&mut self, new_fields: T) {
        match self {
            Projection::All => {}
            Projection::Flat(f) => {
                let unique_fields: HashSet<Field> = f.iter().cloned().collect();
                f.extend(
                    new_fields
                        .into_iter()
                        .filter(|f| !unique_fields.contains(f)),
                );
            }
        }
    }
}
