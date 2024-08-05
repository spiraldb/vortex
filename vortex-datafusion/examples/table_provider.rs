use std::env::temp_dir;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_execution::object_store::ObjectStoreUrl;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::fs::OpenOptions;
use url::Url;
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::struct_::StructArray;
use vortex::array::varbin::VarBinArray;
use vortex::validity::Validity;
use vortex::IntoArray;
use vortex_datafusion::persistent::config::{VortexFile, VortexTableConfig};
use vortex_datafusion::persistent::provider::VortexFileTableProvider;
use vortex_serde::file::file_writer::FileWriter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tmp_path = temp_dir();
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

    let f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(tmp_path.join("a.vtx"))
        .await?;

    let writer = FileWriter::new(f);
    let writer = writer.write_array_columns(st.into_array()).await?;
    writer.finalize().await?;

    let f = tokio::fs::File::open(tmp_path.join("a.vtx")).await?;
    let file_size = f.metadata().await?.len();

    let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let url = ObjectStoreUrl::local_filesystem();

    let p = Path::from_filesystem_path(tmp_path.join("a.vtx"))?;

    let config = VortexTableConfig::new(
        Arc::new(Schema::new(vec![
            Field::new("strings", DataType::Utf8, false),
            Field::new("numbers", DataType::UInt32, false),
        ])),
        vec![VortexFile::new(p, file_size)],
    );

    let provider = Arc::new(VortexFileTableProvider::try_new(url, config)?);

    let ctx = SessionContext::new();
    ctx.register_table("vortex_tbl", Arc::clone(&provider) as _)?;

    let url = Url::try_from("file://").unwrap();
    ctx.register_object_store(&url, object_store);

    ctx.sql("SELECT * from vortex_tbl").await?.show().await?;
    ctx.sql("SELECT * from vortex_tbl where numbers % 2 == 0")
        .await?
        .show()
        .await?;

    Ok(())
}
