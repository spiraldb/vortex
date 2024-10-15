use std::sync::{Arc, RwLock};

use ahash::HashMap;
use bytes::Bytes;
use flatbuffers::root;
use once_cell::sync::OnceCell;
use vortex_dtype::field::Field;
use vortex_dtype::flatbuffers::{deserialize_and_project, resolve_field};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, vortex_panic, VortexExpect, VortexResult};
use vortex_flatbuffers::{message, ReadFlatBuffer};

use crate::layouts::read::{LayoutPartId, MessageId};
use crate::messages::IPCDType;

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
// TODO(robert): This needs to be smarter about nested column layouts where it should be possible to subselect the flatbuffer when projecting.
//  Likely have to return list of children (name, lazy type tuple)
pub struct LazyDeserializedDType {
    dtype_bytes: Option<Bytes>,
    dtype: OnceCell<DType>,
}

impl LazyDeserializedDType {
    pub fn from_bytes(dtype_bytes: Bytes) -> Self {
        Self {
            dtype_bytes: Some(dtype_bytes),
            dtype: OnceCell::new(),
        }
    }

    pub fn from_dtype(dtype: DType) -> Self {
        Self {
            dtype: OnceCell::from(dtype),
            dtype_bytes: None,
        }
    }

    pub fn project(&self, projection: &[Field]) -> VortexResult<DType> {
        if let Some(d) = self.dtype.get() {
            let DType::Struct(s, n) = d else {
                vortex_bail!("Tried to project non struct type")
            };
            Ok(DType::Struct(s.project(projection)?, *n))
        } else if let Some(ref b) = self.dtype_bytes {
            let fb_dtype = Self::fb_schema(b.as_ref())?
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?;
            Ok(deserialize_and_project(fb_dtype, projection)?)
        } else {
            vortex_bail!("Wrong state");
        }
    }

    pub fn dtype(&self) -> VortexResult<&DType> {
        self.dtype.get_or_try_init(|| {
            Ok(IPCDType::read_flatbuffer(&Self::fb_schema(
                self.dtype_bytes.as_ref().vortex_expect("Wrong state"),
            )?)?
            .0)
        })
    }

    /// Convert all name based references to index based to create globally addressable filter
    pub(crate) fn resolve_field(&self, field: &Field) -> VortexResult<usize> {
        if let Some(d) = self.dtype.get() {
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
        } else if let Some(ref b) = self.dtype_bytes {
            let fb_struct = Self::fb_schema(b.as_ref())?
                .dtype()
                .and_then(|d| d.type__as_struct_())
                .ok_or_else(|| vortex_err!("The top-level type should be a struct"))?;
            resolve_field(fb_struct, field)
        } else {
            vortex_bail!("Wrong state");
        }
    }

    fn fb_schema(bytes: &[u8]) -> VortexResult<message::Schema> {
        root::<message::Message>(bytes)
            .map_err(|e| e.into())
            .and_then(|m| {
                m.header_as_schema()
                    .ok_or_else(|| vortex_err!("Message was not a schema"))
            })
    }
}

#[derive(Debug)]
pub struct RelativeLayoutCache {
    root: Arc<RwLock<LayoutMessageCache>>,
    dtype: Option<LazyDeserializedDType>,
    path: MessageId,
}

impl RelativeLayoutCache {
    pub fn new(root: Arc<RwLock<LayoutMessageCache>>, dtype: LazyDeserializedDType) -> Self {
        Self {
            root,
            dtype: Some(dtype),
            path: Vec::new(),
        }
    }

    pub fn relative(&self, id: LayoutPartId, dtype: DType) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self {
            root: self.root.clone(),
            path: new_path,
            dtype: Some(LazyDeserializedDType::from_dtype(dtype)),
        }
    }

    pub fn inlined_schema(&self, id: LayoutPartId) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self {
            root: self.root.clone(),
            path: new_path,
            dtype: None,
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

    pub fn has_dtype(&self) -> bool {
        self.dtype.is_some()
    }

    pub(crate) fn resolve_field(&self, field: &Field) -> VortexResult<usize> {
        self.dtype
            .as_ref()
            .ok_or_else(|| vortex_err!("Must have a dtype"))?
            .resolve_field(field)
    }

    pub fn dtype(&self) -> VortexResult<DType> {
        self.dtype
            .as_ref()
            .ok_or_else(|| vortex_err!("Must have a dtype"))?
            .dtype()
            .cloned()
    }

    pub fn projected_dtype(&self, projection: &[Field]) -> VortexResult<DType> {
        self.dtype
            .as_ref()
            .ok_or_else(|| vortex_err!("Must have a dtype"))?
            .project(projection)
    }

    pub fn absolute_id(&self, path: &[LayoutPartId]) -> MessageId {
        let mut lookup_key = self.path.clone();
        lookup_key.extend_from_slice(path);
        lookup_key
    }
}
