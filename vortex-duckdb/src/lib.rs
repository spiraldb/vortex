//! DuckDB extension for querying Vortex data.
//!
//! DuckDB provides pluggable table functions. Table functions are a way for you to plugin access
//! to different data sources into an existing DuckDB instance. They are loaded as simple C
//! dynamic libraries from the file system.
//!
//! We implement here a simple embedded scan function for DuckDB.
//! Note that DuckDB doesn't easily support pushdown operation. I'm not entirely sure how we go
//! about doing that.
use std::ffi::{c_void, c_char};
use std::error::Error;
use std::ffi::CString;

use duckdb::Connection;
use duckdb::ffi;
use duckdb::core::{DataChunk, LogicalType, LogicalTypeId};
use duckdb::vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab};
use duckdb_loadable_macros::duckdb_entrypoint;

#[repr(C)]
pub struct VortexInitData {
    done: bool,
}

impl Free for VortexInitData {}

#[repr(C)]
pub struct VortexBindData {
    // What goes in here?
}

impl Free for VortexBindData {}

pub struct VortexVTab;

impl VTab for VortexVTab {
    type InitData = VortexInitData;
    type BindData = VortexBindData;

    unsafe fn bind(
        bind: &BindInfo,
        _data: *mut Self::BindData,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        // Set schema of result
        bind.add_result_column("msg", LogicalType::new(LogicalTypeId::Varchar));
        bind.set_cardinality(1, true);

        Ok(())
    }

    unsafe fn init(
        _init: &InitInfo,
        _data: *mut Self::InitData,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        println!("initializing read_vortex function");

        // What do we do here? nothing much.
        Ok(())
    }

    unsafe fn func(
        func: &FunctionInfo,
        output: &mut DataChunk,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data::<Self::InitData>();
        // let bind_data = func.get_init_data::<Self::BindData>();

        if (*init_data).done {
            output.set_len(0);
            return Ok(());
        }

        // Actually return a result
        output.set_len(1);
        let mut flat_vec = output.flat_vector(0);
        flat_vec.as_mut_slice()[0] = CString::from_vec_unchecked(b"secret message".to_vec());

        (*init_data).done = true;

        Ok(())
    }
}

#[duckdb_entrypoint]
fn libvortex_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function::<VortexVTab>("read_vortex")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;

    use super::*;

    #[test]
    fn test_scan() {
        // let array =
        //     PrimitiveArray::from_vec(vec![0u64, 1, 2, 3], Validity::NonNullable).into_array();
        // let chunked = ChunkedArray::try_new(
        //     vec![array],
        //     DType::Primitive(PType::U64, Nullability::NonNullable),
        // )
        // .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.register_table_function::<VortexVTab>("read_vortex")
            .unwrap();

        let response = conn
            .query_row("select * from read_vortex()", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap();

        assert_eq!(response, "secret message".to_string());
    }
}
