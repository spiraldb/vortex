use vortex_error::VortexResult;
use vortex_schema::Nullability;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct ScalarValue<T> {
    nullability: Nullability,
    value: Option<T>,
}

impl<T> ScalarValue<T> {
    pub fn new(value: Option<T>, nullability: Nullability) -> VortexResult<Self> {
        if value.is_none() && nullability == Nullability::NonNullable {
            return Err("Value cannot be None for NonNullable Scalar".into());
        }
        Ok(Self { value, nullability })
    }

    pub fn non_nullable(value: T) -> Self {
        Self::new(Some(value), Nullability::NonNullable).unwrap()
    }

    pub fn nullable(value: T) -> Self {
        Self::new(Some(value), Nullability::Nullable).unwrap()
    }

    pub fn some(value: T) -> Self {
        Self::new(Some(value), Nullability::default()).unwrap()
    }

    pub fn none() -> Self {
        Self::new(None, Nullability::Nullable).unwrap()
    }

    pub fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn into_value(self) -> Option<T> {
        self.value
    }

    pub fn nullability(&self) -> Nullability {
        self.nullability
    }
}
