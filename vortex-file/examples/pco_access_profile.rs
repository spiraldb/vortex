// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Public API resource harness. Run identical code on a candidate and a historical checkout.
//! Arguments: operation (write/full/slice/take/ranges/probe), pattern, rows, repetitions.
//! JSON lines retain cold and warmed iterations; Linux process accounting is read outside timing.

use std::hint::black_box;
use std::time::Instant;

use prost::Message;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayPlugin;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::PiecewiseSequenceArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_pco::Pco;
use vortex_runend::RunEnd;
use vortex_session::VortexSession;

#[derive(Clone, prost::Message)]
struct LegacyMetadata {
    #[prost(bytes, tag = "1")]
    header: Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    chunks: Vec<vortex_pco::PcoChunkInfo>,
}

fn legacy(array: &ArrayRef, session: &VortexSession) -> VortexResult<ArrayRef> {
    let wire = session
        .array_serialize(array)?
        .vortex_expect("PCO serialization");
    let metadata = LegacyMetadata::decode(wire.metadata.as_slice())?.encode_to_vec();
    let buffers = wire
        .buffers
        .into_iter()
        .map(BufferHandle::new_host)
        .collect::<Vec<_>>();
    ArrayPlugin::deserialize(
        &Pco,
        ArrayDeserialization::new(
            wire.serialized_id,
            array.dtype(),
            array.len(),
            &metadata,
            &buffers,
            &wire.children,
        ),
        session,
    )
}

fn linux_accounting() -> (Option<u64>, Option<u64>) {
    let ticks = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|stat| {
            let fields = stat
                .rsplit_once(')')?
                .1
                .split_whitespace()
                .collect::<Vec<_>>();
            Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
        });
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))?
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()
        });
    (ticks, rss)
}

fn main() -> VortexResult<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let mode = args.get(1).map(String::as_str).unwrap_or("slice");
    let pattern = args.get(2).map(String::as_str).unwrap_or("nullable");
    let rows = args
        .get(3)
        .map_or(Ok(8_388_608), |value| value.parse::<usize>())
        .map_err(|error| vortex_err!("invalid row count: {error}"))?;
    let repetitions = args
        .get(4)
        .map_or(Ok(8), |value| value.parse::<usize>())
        .map_err(|error| vortex_err!("invalid repetition count: {error}"))?;
    vortex_ensure!(rows >= 8192, "profile requires at least 8192 rows");
    let session = VortexSession::empty();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let (validity_pattern, legacy_format) = pattern
        .strip_suffix("-legacy")
        .map_or((pattern, false), |pattern| (pattern, true));
    let validity = match validity_pattern {
        "dense" => Validity::NonNullable,
        "all-null" => Validity::AllInvalid,
        "long-null" => Validity::from_iter((0..rows).map(|i| i < 2048 || i > rows - 4096)),
        "compressed" => Validity::Array(
            RunEnd::encode(
                BoolArray::from_iter((0..rows).map(|i| i % 65536 >= 32768)).into_array(),
                &mut ctx,
            )?
            .into_array(),
        ),
        "nullable" => Validity::from_iter((0..rows).map(|i| i % 7 != 0)),
        _ => return Err(vortex_err!("unknown pattern: {pattern}")),
    };
    let input = PrimitiveArray::new(
        (0..rows)
            .map(|i| i64::try_from(i / 16).vortex_expect("fixture fits i64"))
            .collect::<Vec<_>>(),
        validity,
    );
    let encoded = if mode == "write" {
        input.clone().into_array()
    } else {
        Pco::from_primitive(input.as_view(), 3, 1024, &mut ctx)?.into_array()
    };
    let encoded = if legacy_format && mode != "write" {
        legacy(&encoded, &session)?
    } else {
        encoded
    };
    let take_indices = PrimitiveArray::from_iter(
        (0..128usize).map(|i| u64::try_from((i * 104729) % rows).vortex_expect("index fits u64")),
    )
    .into_array();
    let range_starts = (0..16usize)
        .map(|i| u64::try_from((i * (rows / 17)) % (rows - 1024)).vortex_expect("range fits u64"))
        .collect::<Vec<_>>();
    let range_indices = PiecewiseSequenceArray::try_new(
        PrimitiveArray::from_iter(range_starts).into_array(),
        PrimitiveArray::from_iter(vec![512u64; 16]).into_array(),
        PrimitiveArray::from_iter(vec![1u64; 16]).into_array(),
        8192,
    )?
    .into_array();
    let large_indices = PrimitiveArray::from_iter(
        (0..rows / 8).map(|i| u64::try_from((i * 104729) % rows).vortex_expect("index fits u64")),
    )
    .into_array();
    let repeated_indices =
        PrimitiveArray::from_iter((0..rows / 8).map(|i| {
            u64::try_from(((i % 16) * (rows / 17)) % rows).vortex_expect("index fits u64")
        }))
        .into_array();
    let wire = session
        .array_serialize(&encoded)?
        .vortex_expect("prepared array serialization");
    let mut probe = encoded.repeated_probe();
    for iteration in 0..repetitions {
        let before = linux_accounting();
        let start = Instant::now();
        let output: Option<ArrayRef> = match mode {
            "write" => Some(Pco::from_primitive(input.as_view(), 3, 1024, &mut ctx)?.into_array()),
            "full" => Some(
                encoded
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "slice" => Some(
                encoded
                    .slice(rows - 4096..rows - 2048)?
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "take" => Some(
                encoded
                    .take(take_indices.clone())?
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "ranges" => Some(
                encoded
                    .take(range_indices.clone())?
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "slice-near-full" => Some(
                encoded
                    .slice(1..rows - 1)?
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "take-large" => Some(
                encoded
                    .take(large_indices.clone())?
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "take-repeated" => Some(
                encoded
                    .take(repeated_indices.clone())?
                    .execute::<PrimitiveArray>(&mut ctx)?
                    .into_array(),
            ),
            "open-only" | "open-first" => {
                // Array-plugin open with compressed buffers and the validity child already
                // available: includes protobuf decode/validation/index preparation, no file IO.
                let buffers = wire
                    .buffers
                    .iter()
                    .cloned()
                    .map(BufferHandle::new_host)
                    .collect::<Vec<_>>();
                let opened = ArrayPlugin::deserialize(
                    &Pco,
                    ArrayDeserialization::new(
                        wire.serialized_id,
                        encoded.dtype(),
                        encoded.len(),
                        &wire.metadata,
                        &buffers,
                        &wire.children,
                    ),
                    &session,
                )?;
                if mode == "open-first" {
                    black_box(opened.execute_scalar(rows - 2048, &mut ctx)?);
                }
                black_box(opened);
                None
            }
            "probe-first" => {
                black_box(encoded.probe().execute_scalar(rows - 2048, &mut ctx)?);
                None
            }
            "probe" => {
                for i in 0..1024 {
                    black_box(probe.execute_scalar(rows - 2048 + i % 256, &mut ctx)?);
                }
                None
            }
            _ => return Err(vortex_err!("unknown operation: {mode}")),
        };
        black_box(&output);
        let seconds = start.elapsed().as_secs_f64();
        let after = linux_accounting();
        // Keep results live and inspect a public scalar after the measurement. Independent value
        // equivalence is tested by the public semantics/file suites, not by timing assertions.
        if let Some(output) = output {
            black_box(output.execute_scalar(0, &mut ctx)?);
        }
        let ticks = before
            .0
            .zip(after.0)
            .map(|(before, after)| after.saturating_sub(before));
        let ticks = ticks.map_or_else(|| "null".to_owned(), |value| value.to_string());
        let rss = after
            .1
            .map_or_else(|| "null".to_owned(), |value| value.to_string());
        println!(
            "{{\"operation\":\"{mode}\",\"pattern\":\"{pattern}\",\"rows\":{rows},\"iteration\":{iteration},\"wall_seconds\":{seconds},\"cpu_ticks\":{ticks},\"rss_kib\":{rss}}}"
        );
    }
    Ok(())
}
