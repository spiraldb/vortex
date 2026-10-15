// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! How an opened file is described to the `VORTEX_SCAN_V2` executor.

use std::sync::Arc;

use vortex_layout::scan::planning::SegmentLocation;
use vortex_layout::scan::v2::ScanFile;

use crate::Footer;
use crate::VortexFile;

/// The location of every segment the footer describes, indexed by segment id.
pub(crate) fn segment_locations(footer: &Footer) -> Arc<[SegmentLocation]> {
    footer
        .segment_specs_with_metadata()
        .iter()
        .map(|spec| SegmentLocation {
            offset: spec.offset,
            length: spec.length,
            alignment: spec.alignment,
        })
        .collect()
}

/// The parts of an opened file that the `VORTEX_SCAN_V2` executor reads through, or `None` when
/// the file has no segment IO, because its segment source was replaced. Such files are scanned by
/// the default executor.
pub fn scan_file(file: &VortexFile) -> Option<ScanFile> {
    let io = file.segment_io()?.clone();
    let create = || ScanFile {
        layout: Arc::clone(file.footer().layout()),
        locations: segment_locations(file.footer()),
        io,
        plans: file.scan_file_cache.as_ref().map(|_| Arc::default()),
    };
    Some(match &file.scan_file_cache {
        Some(cache) => cache.get_or_init(create).clone(),
        None => create(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_array::IntoArray;
    use vortex_buffer::ByteBufferMut;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_layout::segments::SegmentSource;
    use vortex_layout::segments::TestSegments;

    use super::scan_file;
    use crate::OpenOptionsSessionExt;
    use crate::WriteOptionsSessionExt;
    use crate::tests::SESSION;

    #[tokio::test]
    async fn v2_reader_cache_is_opt_in_and_cleared_when_source_changes() -> VortexResult<()> {
        let mut bytes = ByteBufferMut::empty();
        SESSION
            .write_options()
            .write(
                &mut bytes,
                buffer![1u32, 2, 3, 4].into_array().to_array_stream(),
            )
            .await?;
        let file = SESSION.open_options().open_buffer(bytes.freeze())?;
        let uncached = scan_file(&file).ok_or_else(|| vortex_err!("missing segment IO"))?;
        assert!(uncached.plans.is_none());

        let file = file.with_caching();
        let first = scan_file(&file).ok_or_else(|| vortex_err!("missing segment IO"))?;
        let second = scan_file(&file).ok_or_else(|| vortex_err!("missing segment IO"))?;
        assert!(Arc::ptr_eq(&first.locations, &second.locations));
        assert_eq!(first.io.id(), second.io.id());
        let first_plans = first
            .plans
            .as_ref()
            .ok_or_else(|| vortex_err!("missing first cache"))?;
        let second_plans = second
            .plans
            .as_ref()
            .ok_or_else(|| vortex_err!("missing second cache"))?;
        assert!(Arc::ptr_eq(first_plans, second_plans));

        // A replaced segment source has no segment IO, so V2 leaves the file to the default scan.
        let source: Arc<dyn SegmentSource> = Arc::new(TestSegments::default());
        let file = file.with_segment_source(source);
        assert!(file.segment_io().is_none());
        assert!(scan_file(&file).is_none());
        Ok(())
    }
}
