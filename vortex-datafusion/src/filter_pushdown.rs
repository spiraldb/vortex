//! Implementation of filter pushdown for various expressions from DataFusion into our array types.

/// Flag for operations that are supported or unsupported with filter pushdown.
pub enum SupportsPushdown {
    Supported,
    Unsupported,
}
