// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Size of wide-table files with and without [`VortexWriteOptions::with_dedup_footer`].
//!
//! The report runs with `cargo test -p vortex-file --release --lib wide_size -- --ignored
//! --nocapture`.
//!
//! [`VortexWriteOptions::with_dedup_footer`]: crate::VortexWriteOptions::with_dedup_footer

use std::sync::LazyLock;

use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::FieldNames;
use vortex_array::flatbuffers::ReadFlatBuffer;
use vortex_array::stream::ArrayStreamExt;
use vortex_array::validity::Validity;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexResult;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

use crate::EOF_SIZE;
use crate::OpenOptionsSessionExt;
use crate::VERSION;
use crate::VERSION_DEDUP_FOOTER;
use crate::WriteOptionsSessionExt;
use crate::footer::postscript::Postscript;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = array_session()
        .with::<LayoutSession>()
        .with::<RuntimeSession>();
    crate::register_default_encodings(&session);
    crate::enable_all_registered_array_encodings(&session);
    session
});

#[derive(Clone, Copy, Debug)]
enum ColumnKind {
    /// `list<i64>` with one element per row, matching the compress-bench wide table.
    ListOfI64,
    /// Random `i64`.
    I64,
    /// `i64` in `0..100`, which every column bit-packs to the same width.
    SmallI64,
    /// Each column picks a type and value range at random: integers with varying bit widths,
    /// floats, low-cardinality strings, and nullable integers.
    Mixed,
}

fn column(kind: ColumnKind, rows: usize, rng: &mut StdRng) -> VortexResult<ArrayRef> {
    Ok(match kind {
        ColumnKind::ListOfI64 => {
            let elements = PrimitiveArray::from_iter((0..rows).map(|_| rng.random::<i64>()));
            let offsets = PrimitiveArray::from_iter(0..=u32::try_from(rows)?);
            ListArray::try_new(
                elements.into_array(),
                offsets.into_array(),
                Validity::AllValid,
            )?
            .into_array()
        }
        ColumnKind::I64 => {
            PrimitiveArray::from_iter((0..rows).map(|_| rng.random::<i64>())).into_array()
        }
        ColumnKind::SmallI64 => {
            PrimitiveArray::from_iter((0..rows).map(|_| rng.random_range(0i64..100))).into_array()
        }
        ColumnKind::Mixed => match rng.random_range(0..4) {
            0 => {
                let max = 1i64 << rng.random_range(1..40);
                PrimitiveArray::from_iter((0..rows).map(|_| rng.random_range(0..max))).into_array()
            }
            1 => PrimitiveArray::from_iter((0..rows).map(|_| rng.random::<f64>())).into_array(),
            2 => {
                let card = rng.random_range(2..50);
                VarBinViewArray::from_iter_str(
                    (0..rows).map(|_| format!("category-{}", rng.random_range(0..card))),
                )
                .into_array()
            }
            _ => PrimitiveArray::from_option_iter(
                (0..rows).map(|_| rng.random_bool(0.9).then(|| rng.random_range(0i32..1000))),
            )
            .into_array(),
        },
    })
}

fn wide_table(kind: ColumnKind, ncols: usize, rows: usize) -> VortexResult<ArrayRef> {
    let names: FieldNames = (0..ncols).map(|i| i.to_string()).collect();
    let mut rng = StdRng::seed_from_u64(0);
    let fields = (0..ncols)
        .map(|_| column(kind, rows, &mut rng))
        .collect::<VortexResult<Vec<_>>>()?;
    Ok(StructArray::try_new(names, fields, rows, Validity::NonNullable)?.into_array())
}

async fn write(array: &ArrayRef, dedup: bool) -> VortexResult<ByteBuffer> {
    let mut buf = ByteBufferMut::empty();
    SESSION
        .write_options()
        .with_dedup_footer(dedup)
        .write(&mut buf, array.to_array_stream())
        .await?;
    Ok(buf.freeze())
}

/// Byte breakdown of a written file.
#[derive(Debug, Default)]
struct SizeBreakdown {
    total: usize,
    segments: usize,
    /// Bytes of array encoding trees stored in segments (flatbuffer plus its u32 length).
    segment_array_trees: usize,
    dtype: usize,
    layout: usize,
    statistics: usize,
    footer: usize,
}

impl SizeBreakdown {
    /// File bytes other than array data buffers.
    fn metadata(&self) -> usize {
        self.total - (self.segments - self.segment_array_trees)
    }
}

fn breakdown(bytes: &ByteBuffer) -> VortexResult<SizeBreakdown> {
    let eof = &bytes[bytes.len() - EOF_SIZE..];
    let version = u16::from_le_bytes([eof[0], eof[1]]);
    let ps_len = u16::from_le_bytes([eof[2], eof[3]]) as usize;
    let ps_end = bytes.len() - EOF_SIZE;
    let ps = Postscript::read_flatbuffer_bytes(&bytes[ps_end - ps_len..ps_end])?;

    let file = SESSION.open_options().open_buffer(bytes.clone())?;
    let mut segment_array_trees = 0;
    let mut segments = 0;
    for spec in file.footer().segment_map().iter() {
        let range = spec.byte_range();
        let seg = &bytes[range.start as usize..range.end as usize];
        segments += seg.len();
        // Dedup files keep array trees in the layout, so segments hold only data buffers.
        if version == VERSION
            && let [.., a, b, c, d] = *seg
        {
            segment_array_trees += u32::from_le_bytes([a, b, c, d]) as usize + 4;
        }
    }

    Ok(SizeBreakdown {
        total: bytes.len(),
        segments,
        segment_array_trees,
        dtype: ps.dtype.map_or(0, |s| s.length as usize),
        layout: ps.layout.length as usize,
        statistics: ps.statistics.map_or(0, |s| s.length as usize),
        footer: ps.footer.length as usize,
    })
}

fn kib(n: usize) -> String {
    format!("{:.1}", n as f64 / 1024.0)
}

#[rstest]
#[case::list(ColumnKind::ListOfI64, 200, 1000)]
#[case::small(ColumnKind::SmallI64, 200, 1000)]
#[case::mixed(ColumnKind::Mixed, 200, 1000)]
#[case::mixed_chunked(ColumnKind::Mixed, 20, 200_000)]
#[tokio::test]
async fn dedup_footer_round_trip(
    #[case] kind: ColumnKind,
    #[case] ncols: usize,
    #[case] rows: usize,
) -> VortexResult<()> {
    let array = wide_table(kind, ncols, rows)?;
    let baseline = write(&array, false).await?;
    let bytes = write(&array, true).await?;
    let eof = &bytes[bytes.len() - EOF_SIZE..];
    assert_eq!(u16::from_le_bytes([eof[0], eof[1]]), VERSION_DEDUP_FOOTER);
    assert!(bytes.len() < baseline.len());

    let file = SESSION.open_options().open_buffer(bytes)?;
    let actual = file.scan()?.into_array_stream()?.read_all().await?;
    assert_arrays_eq!(actual, array, &mut SESSION.create_execution_ctx());
    Ok(())
}

#[tokio::test]
#[ignore = "size report, run manually"]
async fn wide_size_report() -> VortexResult<()> {
    println!(
        "| table | baseline KiB | dedup KiB | saved | metadata share (base → dedup) | layout KiB (base → dedup) | segment array trees KiB (base → dedup) | dtype KiB | stats KiB | segment specs KiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    let cases = [
        (ColumnKind::ListOfI64, 1000, 1000),
        (ColumnKind::ListOfI64, 10000, 1000),
        (ColumnKind::I64, 1000, 1000),
        (ColumnKind::I64, 10000, 1000),
        (ColumnKind::SmallI64, 1000, 1000),
        (ColumnKind::SmallI64, 10000, 1000),
        (ColumnKind::Mixed, 1000, 1000),
        (ColumnKind::Mixed, 10000, 1000),
        (ColumnKind::SmallI64, 1000, 100_000),
        (ColumnKind::Mixed, 1000, 100_000),
    ];
    for (kind, ncols, rows) in cases {
        let array = wide_table(kind, ncols, rows)?;
        let base = breakdown(&write(&array, false).await?)?;
        let dedup = breakdown(&write(&array, true).await?)?;
        println!(
            "| {kind:?} cols={ncols} rows={rows} | {} | {} | {:.1}% | {:.1}% → {:.1}% | {} → {} | {} → {} | {} | {} | {} |",
            kib(base.total),
            kib(dedup.total),
            100.0 * (base.total - dedup.total) as f64 / base.total as f64,
            100.0 * base.metadata() as f64 / base.total as f64,
            100.0 * dedup.metadata() as f64 / dedup.total as f64,
            kib(base.layout),
            kib(dedup.layout),
            kib(base.segment_array_trees),
            kib(dedup.segment_array_trees),
            kib(dedup.dtype),
            kib(dedup.statistics),
            kib(dedup.footer),
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "timing report, run manually"]
async fn wide_open_timing() -> VortexResult<()> {
    use std::time::Instant;
    for (kind, ncols) in [(ColumnKind::SmallI64, 10000), (ColumnKind::Mixed, 10000)] {
        let array = wide_table(kind, ncols, 1000)?;
        for dedup in [false, true] {
            let bytes = write(&array, dedup).await?;
            let mut open = std::time::Duration::ZERO;
            let mut reader = std::time::Duration::ZERO;
            let mut scan = std::time::Duration::ZERO;
            let iters = 5;
            for _ in 0..iters {
                let t = Instant::now();
                let file = SESSION.open_options().open_buffer(bytes.clone())?;
                open += t.elapsed();
                let t = Instant::now();
                drop(file.layout_reader()?);
                reader += t.elapsed();
                let t = Instant::now();
                file.scan()?.into_array_stream()?.read_all().await?;
                scan += t.elapsed();
            }
            println!(
                "{kind:?} cols={ncols} dedup={dedup}: open {:?}, layout reader {:?}, full scan {:?}",
                open / iters,
                reader / iters,
                scan / iters
            );
        }
    }
    Ok(())
}
