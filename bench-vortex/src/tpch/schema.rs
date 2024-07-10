/// Arrow schemas for TPC-H tables.
///
/// Adapted from the SQL definitions in https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-schema.sql
use arrow_schema::{DataType, Field, Schema};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref NATION: Schema = Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, true),
    ]);

    pub static ref REGION: Schema = Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, true),
    ]);

    pub static ref PART: Schema = Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, false),
    ]);

    pub static ref SUPPLIER: Schema = Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int32, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, false),
    ]);

    pub static ref PARTSUPP: Schema = Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int64, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]);


    pub static ref CUSTOMER: Schema = Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]);

    pub static ref ORDERS: Schema = Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Utf8, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]);

    pub static ref LINEITEM: Schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        // NOTE: Arrow doesn't have a DATE type, but YYYY-MM-DD is lexicographically ordered
        //  so we can just use Utf8 and adjust any queries that rely on date functions.
        Field::new("l_shipdate", DataType::Utf8, false),
        Field::new("l_commitdate", DataType::Utf8, false),
        Field::new("l_receiptdate", DataType::Utf8, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]);
}
