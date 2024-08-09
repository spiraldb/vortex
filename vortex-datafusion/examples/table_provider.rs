use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_execution::object_store::ObjectStoreUrl;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use tempfile::tempdir;
use tokio::fs::OpenOptions;
use url::Url;
use vortex::array::{ChunkedArray, PrimitiveArray, StructArray, VarBinArray};
use vortex::validity::Validity;
use vortex::{Context, IntoArray};
use vortex_datafusion::persistent::config::{VortexFile, VortexTableOptions};
use vortex_datafusion::persistent::provider::VortexFileTableProvider;
use vortex_serde::layouts::writer::LayoutWriter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let strings = ChunkedArray::from_iter([
        VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
    ])
    .into_array();

    let numbers = ChunkedArray::from_iter([
        PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
        PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
    ])
    .into_array();

    let st = StructArray::try_new(
        ["strings".into(), "numbers".into()].into(),
        vec![strings, numbers],
        8,
        Validity::NonNullable,
    )
    .unwrap();

    let filepath = temp_dir.path().join("a.vtx");

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&filepath)
        .await?;

    let writer = LayoutWriter::new(f);
    let writer = writer.write_array_columns(st.into_array()).await?;
    writer.finalize().await?;

    let f = tokio::fs::File::open(&filepath).await?;
    let file_size = f.metadata().await?.len();

    let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let url = ObjectStoreUrl::local_filesystem();

    let p = Path::from_filesystem_path(filepath)?;

    let config = VortexTableOptions::new(
        Arc::new(Schema::new(vec![
            Field::new("strings", DataType::Utf8, false),
            Field::new("numbers", DataType::UInt32, false),
        ])),
        vec![VortexFile::new(p, file_size)],
        Arc::new(Context::default()),
    );

    let provider = Arc::new(VortexFileTableProvider::try_new(url, config)?);

    let ctx = SessionContext::new();
    ctx.register_table("vortex_tbl", Arc::clone(&provider) as _)?;

    let url = Url::try_from("file://").unwrap();
    ctx.register_object_store(&url, object_store);

    run_query(&ctx, "SELECT * FROM vortex_tbl").await?;

    Ok(())
}

async fn run_query(ctx: &SessionContext, query_string: impl AsRef<str>) -> anyhow::Result<()> {
    let query_string = query_string.as_ref();

    ctx.sql(&format!("EXPLAIN {query_string}"))
        .await?
        .show()
        .await?;

    ctx.sql(query_string).await?.show().await?;

    Ok(())
}
