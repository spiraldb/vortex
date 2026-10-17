// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::Any;
use std::any::TypeId;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use arcref::ArcRef;
use parking_lot::Mutex;
use vortex_array::dtype::DType;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;
use vortex_utils::aliases::hash_map::HashMap;

use crate::LayoutId;
use crate::LayoutRef;
use crate::VTable;
use crate::children::LayoutChildren;
use crate::segments::SegmentId;

/// Backwards-compatible name for a layout ID.
pub type LayoutEncodingId = LayoutId;
/// Shared reference to a registered layout-vtable plugin.
pub type LayoutVTableRef = ArcRef<dyn LayoutVTablePlugin>;
/// Backwards-compatible name for a layout-vtable reference.
pub type LayoutEncodingRef = LayoutVTableRef;

/// Common fields available while deserializing layout-specific data.
pub struct LayoutDeserializeArgs<'a> {
    /// Session used to resolve plugin-owned metadata.
    pub session: &'a VortexSession,
    /// Array read context referenced by serialized array metadata.
    pub array_read_ctx: &'a ReadContext,
    /// Logical dtype of this layout.
    pub dtype: &'a DType,
    /// Number of rows in this layout.
    pub row_count: u64,
    /// Directly referenced segments.
    pub segment_ids: Vec<SegmentId>,
    /// Lazy child access.
    pub children: &'a dyn LayoutChildren,
    /// Values shared by the layouts of the tree being deserialized.
    pub memo: &'a LayoutBuildMemo,
}

/// Context shared while recursively deserializing layouts.
pub struct LayoutBuildContext<'a> {
    /// Session used to resolve plugin-owned metadata.
    pub session: &'a VortexSession,
    /// Array read context referenced by serialized array metadata.
    pub array_read_ctx: &'a ReadContext,
    /// Values shared by the layouts of the tree being deserialized.
    pub memo: &'a LayoutBuildMemo,
}

/// Values a layout encoding derives while deserializing, shared by the layouts of one tree.
///
/// A file's layouts often repeat the same metadata, one layout per column of a wide file, so an
/// encoding can keep what it derives from that metadata here instead of deriving it again for
/// every layout. A memo belongs to one layout tree, so nothing is shared between files or reads.
#[derive(Default)]
pub struct LayoutBuildMemo(Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl LayoutBuildMemo {
    /// Returns the memo's value of type `T`, inserting a default one if it has none.
    pub fn get_or_default<T: Any + Default + Send + Sync>(&self) -> Arc<T> {
        let value = Arc::clone(
            self.0
                .lock()
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Arc::new(T::default())),
        );
        value
            .downcast::<T>()
            .ok()
            .vortex_expect("memo values are keyed by their type")
    }
}

impl Debug for LayoutBuildMemo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayoutBuildMemo").finish_non_exhaustive()
    }
}

/// Object-safe plugin registered for a layout ID.
pub trait LayoutVTablePlugin: 'static + Send + Sync + Debug {
    /// Returns this plugin as [`Any`].
    fn as_any(&self) -> &dyn Any;
    /// Returns the globally unique layout ID.
    fn id(&self) -> LayoutEncodingId;
    /// Deserializes a layout node.
    fn build(
        &self,
        dtype: &DType,
        row_count: u64,
        metadata: &[u8],
        segment_ids: Vec<SegmentId>,
        children: &dyn LayoutChildren,
        build_ctx: &LayoutBuildContext<'_>,
    ) -> VortexResult<LayoutRef>;

    /// Returns `true` if this layout is indivisible: its readers never register natural split
    /// boundaries strictly inside their row range (see [`VTable::is_indivisible`]).
    fn is_indivisible(&self) -> bool {
        false
    }
}

/// Backwards-compatible name for the object-safe layout-vtable plugin.
pub use LayoutVTablePlugin as LayoutEncoding;

impl<V: VTable> LayoutVTablePlugin for V {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn id(&self) -> LayoutEncodingId {
        VTable::id(self)
    }

    fn build(
        &self,
        dtype: &DType,
        row_count: u64,
        metadata: &[u8],
        segment_ids: Vec<SegmentId>,
        children: &dyn LayoutChildren,
        build_ctx: &LayoutBuildContext<'_>,
    ) -> VortexResult<LayoutRef> {
        let metadata = V::deserialize_metadata(metadata, build_ctx.memo)?;
        Ok(V::build(
            self,
            dtype,
            row_count,
            &metadata,
            segment_ids,
            children,
            build_ctx,
        )?
        .into_layout())
    }

    fn is_indivisible(&self) -> bool {
        VTable::is_indivisible(self)
    }
}

impl Display for dyn LayoutVTablePlugin + '_ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

impl PartialEq for dyn LayoutVTablePlugin + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for dyn LayoutVTablePlugin + '_ {}

impl dyn LayoutVTablePlugin + '_ {
    /// Returns whether this plugin is vtable `V`.
    pub fn is<V: VTable>(&self) -> bool {
        self.as_opt::<V>().is_some()
    }

    /// Downcasts this plugin to vtable `V`.
    pub fn as_<V: VTable>(&self) -> &V {
        self.as_opt::<V>()
            .vortex_expect("layout encoding type mismatch")
    }

    /// Attempts to downcast this plugin to vtable `V`.
    pub fn as_opt<V: VTable>(&self) -> Option<&V> {
        self.as_any().downcast_ref()
    }
}
