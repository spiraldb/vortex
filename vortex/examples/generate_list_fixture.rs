// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Generate large list rows for selection-scan diagnostics.
//!
//! Usage: `generate_list_fixture OUTPUT balanced|skewed|small primitive|nested TARGET_BYTES`.
//! Writes a new file and verifies full decoded equality through V1. Element block sizing is
//! independent of outer row count; nullable lists, empty lists, and nullable elements are included.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use anyhow::ensure;
use futures::stream;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex::VortexSessionDefault;
use vortex::array::IntoArray;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::BoolArray;
use vortex::array::arrays::ListArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::stream::ArrayStreamExt;
use vortex::array::validity::Validity;
use vortex::buffer::Buffer;
use vortex::editions::Edition;
use vortex::editions::EditionDeclaration;
use vortex::editions::EditionId;
use vortex::editions::EditionMember;
use vortex::editions::EditionSessionExt;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteStrategyBuilder;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutRef;
use vortex::layout::layouts::table::TableStrategy;
use vortex::session::VortexSession;
use vortex_arrow::ArrowSessionExt;

const INNER_LENGTH: u64 = 128;

static EDITION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: EditionId::new("large-list-fixture", 2026, 10, 0),
        min_library_version: None,
    },
    added: &[EditionMember::layout(&"vortex.list")],
};

fn count_layouts(layout: &LayoutRef, counts: &mut BTreeMap<String, usize>) -> Result<()> {
    *counts.entry(layout.encoding_id().to_string()).or_default() += 1;
    for child in layout.children()? {
        count_layouts(&child, counts)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 4,
        "Expected OUTPUT balanced|skewed|small primitive|nested TARGET_BYTES"
    );
    ensure!(
        matches!(args[1].as_str(), "balanced" | "skewed" | "small"),
        "Unknown distribution"
    );
    ensure!(
        matches!(args[2].as_str(), "primitive" | "nested"),
        "Unknown element shape"
    );
    let target_bytes = args[3].parse::<u64>()?;
    ensure!(target_bytes > 0, "TARGET_BYTES must be positive");
    let session = VortexSession::default().with_tokio();
    session.register_edition(&EDITION)?;
    session.enable_edition(EDITION.edition.id)?;

    let rows = if args[1] == "small" { 1_000_000 } else { 512 };
    let mut offsets = vec![0_u64];
    let mut total = 0_u64;
    for row in 0..rows {
        total += if row % 19 == 0 {
            0
        } else if args[1] == "small" {
            1 + row % 20
        } else if args[1] == "balanced" {
            8192
        } else if row % 16 == 0 {
            262_144
        } else {
            INNER_LENGTH
        };
        offsets.push(total);
    }
    let mut rng = StdRng::seed_from_u64(42);
    let mut elements = PrimitiveArray::from_option_iter((0..total).map(|index| {
        let value = rng.random::<i64>();
        (index % 7 != 0).then_some(value)
    }))
    .into_array();
    if args[2] == "nested" {
        let inner_count = total / INNER_LENGTH;
        elements = ListArray::try_new(
            elements,
            Buffer::from_iter((0..=inner_count).map(|index| index * INNER_LENGTH)).into_array(),
            Validity::Array(
                BoolArray::from_iter((0..inner_count).map(|index| index % 11 != 0)).into_array(),
            ),
        )?
        .into_array();
        for offset in &mut offsets {
            *offset /= INNER_LENGTH;
        }
    }
    let values = ListArray::try_new(
        elements,
        Buffer::from(offsets).into_array(),
        Validity::Array(BoolArray::from_iter((0..rows).map(|row| row % 17 != 0)).into_array()),
    )?
    .into_array();
    let input = StructArray::from_fields(&[
        ("id", Buffer::from_iter(0..rows).into_array()),
        ("values", values),
    ])?
    .into_array();
    let elements = WriteStrategyBuilder::from_session(&session)
        .with_data_block_target_bytes(Some(target_bytes))
        .build();
    let defaults = WriteStrategyBuilder::from_session(&session).build();
    let strategy = TableStrategy::new(Arc::clone(&defaults), defaults)
        .with_list_layout_factory(move |list| Arc::new(list.with_elements(Arc::clone(&elements))));
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[0])
        .await?;
    let summary = session
        .write_options()
        .with_strategy(Arc::new(strategy))
        .write(
            &mut output,
            ArrayStreamAdapter::new(input.dtype().clone(), stream::iter([Ok(input.clone())])),
        )
        .await?;
    drop(output);
    let file = session
        .open_options()
        .open_path(Path::new(&args[0]))
        .await?;
    let actual = ArrayStreamAdapter::new(file.dtype().clone(), file.scan()?.into_stream()?)
        .read_all()
        .await?;
    let mut ctx = session.create_execution_ctx();
    let arrow = session.arrow();
    let field = arrow.to_arrow_field("", input.dtype())?;
    let expected = arrow.execute_arrow(input, Some(&field), &mut ctx)?;
    let actual = arrow.execute_arrow(actual, Some(&field), &mut ctx)?;
    ensure!(
        expected.as_ref() == actual.as_ref(),
        "Decoded fixture differs from generated input"
    );
    let mut layouts = BTreeMap::new();
    count_layouts(file.footer().layout(), &mut layouts)?;
    ensure!(
        layouts.get("vortex.list").copied().unwrap_or(0) > 0,
        "Expected ListLayout"
    );
    println!(
        "{}",
        serde_json::json!({
            "output": args[0], "distribution": args[1], "shape": args[2],
            "target_bytes": target_bytes, "rows": rows, "primitive_elements": total,
            "max_row_payload_bytes": match args[1].as_str() {
                "small" => 20 * 8, "balanced" => 8192 * 8, _ => 262_144 * 8,
            },
            "bytes": summary.size(), "correct": true, "layouts": layouts,
            "layout_tree": file.footer().layout().display_tree().to_string(),
        })
    );
    Ok(())
}
