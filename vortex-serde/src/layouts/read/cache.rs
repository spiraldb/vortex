use std::sync::{Arc, RwLock};

use ahash::HashMap;
use bytes::Bytes;
use flatbuffers::root_unchecked;
use once_cell::sync::OnceCell;
use vortex_dtype::field::Field;
use vortex_dtype::flatbuffers::{deserialize_and_project, resolve_field};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, vortex_panic, VortexExpect, VortexResult};
use vortex_flatbuffers::message;
use vortex_schema::projection::Projection;

use crate::layouts::read::{LayoutPartId, MessageId};

#[derive(Default, Debug)]
pub struct LayoutMessageCache {
    cache: HashMap<MessageId, Bytes>,
}

impl LayoutMessageCache {
    pub fn get(&self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.cache.get(path).cloned()
    }

    pub fn remove(&mut self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.cache.remove(path)
    }

    pub fn set(&mut self, path: MessageId, value: Bytes) {
        self.cache.insert(path, value);
    }
}

#[derive(Debug)]
pub struct LazyDeserializedDType {
    dtype_bytes: Option<Bytes>,
    dtype: OnceCell<DType>,
    projection: Projection,
}

impl LazyDeserializedDType {
    pub fn from_bytes(dtype_bytes: Bytes, projection: Projection) -> Self {
        Self {
            dtype_bytes: Some(dtype_bytes),
            dtype: OnceCell::new(),
            projection,
        }
    }

    pub fn from_dtype(dtype: DType, projection: Projection) -> Self {
        Self {
            dtype: OnceCell::from(dtype),
            dtype_bytes: None,
            projection,
        }
    }

    /// Restrict the underlying dtype to selected fields
    pub fn project(&self, projection: &[Field]) -> VortexResult<Arc<Self>> {
        // TODO(robert): Respect existing projection list, only really an issue for nested structs
        if let Some(ref b) = self.dtype_bytes {
            Ok(Arc::new(LazyDeserializedDType::from_bytes(
                b.clone(),
                Projection::Flat(projection.to_owned()),
            )))
        } else if let Some(d) = self.dtype.get() {
            let DType::Struct(s, n) = d else {
                vortex_bail!("Not a struct dtype")
            };
            Ok(Arc::new(LazyDeserializedDType::from_dtype(
                DType::Struct(s.project(projection)?, *n),
                Projection::Flat(projection.to_vec()),
            )))
        } else {
            vortex_bail!("Wrong state");
        }
    }

    /// Get vortex dtype out of serialized bytes
    pub fn value(&self) -> VortexResult<&DType> {
        self.dtype.get_or_try_init(|| {
            let fb_dtype = Self::fb_schema(self.dtype_bytes.as_ref().vortex_expect("Wrong state"))?
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?;
            match &self.projection {
                Projection::All => DType::try_from(fb_dtype)
                    .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e)),
                Projection::Flat(p) => deserialize_and_project(fb_dtype, p),
            }
        })
    }

    /// Convert all name based references to index based to create globally addressable filter
    pub(crate) fn resolve_field(&self, field: &Field) -> VortexResult<usize> {
        if let Some(ref b) = self.dtype_bytes {
            let fb_struct = Self::fb_schema(b.as_ref())?
                .dtype()
                .and_then(|d| d.type__as_struct_())
                .ok_or_else(|| vortex_err!("The top-level type should be a struct"))?;
            resolve_field(fb_struct, field)
        } else if let Some(d) = self.dtype.get() {
            let DType::Struct(s, _) = d else {
                vortex_bail!("Trying to resolve fields in non struct dtype")
            };
            match field {
                Field::Name(n) => s
                    .names()
                    .iter()
                    .position(|name| name.as_ref() == n.as_str())
                    .ok_or_else(|| vortex_err!("Can't find {n} in the type")),
                Field::Index(i) => Ok(*i),
            }
        } else {
            vortex_bail!("Wrong state");
        }
    }

    fn fb_schema(bytes: &[u8]) -> VortexResult<message::Schema> {
        unsafe { root_unchecked::<message::Message>(bytes) }
            .header_as_schema()
            .ok_or_else(|| vortex_err!("Message was not a schema"))
    }
}

#[derive(Debug)]
pub struct RelativeLayoutCache {
    root: Arc<RwLock<LayoutMessageCache>>,
    dtype: Arc<LazyDeserializedDType>,
    path: MessageId,
}

impl RelativeLayoutCache {
    pub fn new(root: Arc<RwLock<LayoutMessageCache>>, dtype: Arc<LazyDeserializedDType>) -> Self {
        Self {
            root,
            dtype,
            path: Vec::new(),
        }
    }

    pub fn relative(&self, id: LayoutPartId, dtype: Arc<LazyDeserializedDType>) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self {
            root: self.root.clone(),
            path: new_path,
            dtype,
        }
    }

    pub fn get(&self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.root
            .read()
            .unwrap_or_else(|poison| {
                vortex_panic!(
                    "Failed to read from layout cache at path {:?} with error {}",
                    path,
                    poison
                );
            })
            .get(&self.absolute_id(path))
    }

    pub fn remove(&mut self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.root
            .write()
            .unwrap_or_else(|poison| {
                vortex_panic!(
                    "Failed to write to layout cache at path {:?} with error {}",
                    path,
                    poison
                )
            })
            .remove(&self.absolute_id(path))
    }

    pub fn dtype(&self) -> &Arc<LazyDeserializedDType> {
        &self.dtype
    }

    pub fn absolute_id(&self, path: &[LayoutPartId]) -> MessageId {
        let mut lookup_key = Vec::with_capacity(self.path.len() + path.len());
        lookup_key.clone_from(&self.path);
        lookup_key.extend_from_slice(path);
        lookup_key
    }
}
