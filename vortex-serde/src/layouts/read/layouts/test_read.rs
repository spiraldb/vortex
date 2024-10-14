use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use vortex::Array;

use crate::layouts::read::selection::RowSelector;
use crate::layouts::{LayoutMessageCache, LayoutReader, RangeResult, ReadResult};

pub fn read_layout_ranges(
    layout: &mut dyn LayoutReader,
    cache: Arc<RwLock<LayoutMessageCache>>,
    buf: &Bytes,
) -> Vec<RowSelector> {
    let mut s = Vec::new();
    while let Some(rr) = layout.read_range().unwrap() {
        match rr {
            RangeResult::ReadMore(mut m) => {
                let mut write_cache_guard = cache.write().unwrap();
                let (id, range) = m.remove(0);
                write_cache_guard.set(id, buf.slice(range.to_range()));
            }
            RangeResult::Range(r) => s.push(r),
        }
    }
    s
}

pub fn read_layout_data(
    layout: &mut dyn LayoutReader,
    cache: Arc<RwLock<LayoutMessageCache>>,
    buf: &Bytes,
    selector: RowSelector,
) -> Vec<Array> {
    let mut arr = Vec::new();
    while let Some(rr) = layout.read_next(selector.clone()).unwrap() {
        match rr {
            ReadResult::ReadMore(mut m) => {
                let mut write_cache_guard = cache.write().unwrap();
                let (id, range) = m.remove(0);
                write_cache_guard.set(id, buf.slice(range.to_range()));
            }
            ReadResult::Batch(a) => arr.push(a),
        }
    }

    arr
}

pub fn read_layout(
    filter_layout: &mut dyn LayoutReader,
    layout: &mut dyn LayoutReader,
    cache: Arc<RwLock<LayoutMessageCache>>,
    buf: &Bytes,
) -> VecDeque<Array> {
    read_layout_ranges(filter_layout, cache.clone(), buf)
        .into_iter()
        .flat_map(|s| read_layout_data(layout, cache.clone(), buf, s))
        .collect()
}
