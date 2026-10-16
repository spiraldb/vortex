// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::stream;
use vortex_array::ArrayContext;
use vortex_array::IntoArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_io::session::RuntimeSessionExt;

use super::scan_file;
use crate::LayoutStrategy;
use crate::layouts::chunked::writer::ChunkedLayoutStrategy;
use crate::layouts::list::writer::ListLayoutStrategy;
use crate::scan::v2::supports;
use crate::segments::SegmentSource;
use crate::segments::TestSegments;
use crate::sequence::SequenceId;
use crate::sequence::SequentialStreamAdapter;
use crate::sequence::SequentialStreamExt;
use crate::test::new_session;

/// Lists are scanned by the default executor: their element ranges are only known once their
/// offsets are read, after a pipeline would have been compiled.
#[tokio::test]
async fn lists_are_not_supported() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let chunk = ListArray::try_new(
        PrimitiveArray::from_option_iter([Some(0i32), None, Some(2), Some(1), Some(0)])
            .into_array(),
        buffer![0u64, 2, 2, 3, 5].into_array(),
        Validity::from_iter([true, true, false, true]),
    )?
    .into_array();
    let dtype = chunk.dtype().clone();
    let segments = Arc::new(TestSegments::default());
    let (mut sequence, eof) = SequenceId::root().split();
    let chunks = vec![Ok((sequence.advance(), chunk))];
    let layout = ChunkedLayoutStrategy::new(ListLayoutStrategy::default())
        .write_stream(
            ArrayContext::empty().into(),
            Arc::<TestSegments>::clone(&segments),
            SequentialStreamAdapter::new(dtype, stream::iter(chunks)).sendable(),
            eof,
            &session,
        )
        .await?;
    let segments: Arc<dyn SegmentSource> = segments;
    assert!(!supports(&scan_file(&segments, &layout)?));
    Ok(())
}
