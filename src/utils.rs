use std::str::{from_utf8, FromStr};
use std::sync::Arc;

use async_std::task;
use datafusion::arrow::array::{Array, AsArray, BinaryArray, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimeUnit, UInt32Type};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use object_store::aws::AmazonS3Builder;
use pgrx::*;
use pgrx::IntoDatum;
use pgrx::pg_sys::{Datum, Oid};
use url::Url;

pub(super) trait SerdeList {
    unsafe fn serialize_to_list(state: PgBox<Self>, mut ctx: PgMemoryContexts) -> *mut pg_sys::List
        where
            Self: Sized,
    {
        let mut old_ctx = ctx.set_as_current();

        let mut ret = PgList::new();
        let val = state.into_pg() as i64;
        let cst = pg_sys::makeConst(
            pg_sys::INT8OID,
            -1,
            pg_sys::InvalidOid,
            8,
            val.into_datum().unwrap(),
            false,
            true,
        );
        ret.push(cst);
        let ret = ret.into_pg();

        old_ctx.set_as_current();

        ret
    }

    unsafe fn deserialize_from_list(list: *mut pg_sys::List) -> PgBox<Self>
        where
            Self: Sized,
    {
        let list = PgList::<pg_sys::Const>::from_pg(list);
        if list.is_empty() {
            return PgBox::<Self>::null();
        }

        let cst = list.head().unwrap();
        let ptr = i64::from_datum((*cst).constvalue, (*cst).constisnull).unwrap();
        PgBox::<Self>::from_pg(ptr as _)
    }
}

pub fn generate_test_data_for_oid(oid: Oid) -> Option<Datum> {

    // log!("{oid}");
    match oid {
        pg_sys::INT2OID => 10i16.into_datum(),
        pg_sys::INT4OID => 100i32.into_datum(),
        pg_sys::INT8OID => 1000i64.into_datum(),
        pg_sys::FLOAT4ARRAYOID => {
            let mut values = Vec::new();
            for i in 0..10 {
                values.push(i as f32)
            }
            values.into_datum()
        }
        pg_sys::FLOAT8ARRAYOID => {
            None
        }
        pg_sys::BOOLOID => (0).into_datum(),
        //CHAROID => Some(3.into()),
        pg_sys::FLOAT4OID => 2.0f32
            .into_datum(),
        pg_sys::FLOAT8OID => 8.8f64
            .into_datum(),

        pg_sys::NUMERICOID => AnyNumeric::try_from(
            1.23f32
        )
            .unwrap_or_default()
            .into_datum(),

        pg_sys::TEXTOID => String::from("hello")
            .into_datum(),

        pg_sys::VARCHAROID => String::from("hello")
            .into_datum(),

        pg_sys::BPCHAROID => String::from("hello")
            .into_datum(),

        pg_sys::DATEOID => {
            Date::from_str("1934-01-03").unwrap().into_datum()
        }
        pg_sys::TIMEOID => {
            Time::from_str("12:23:00").unwrap().into_datum()
        }
        pg_sys::TIMESTAMPOID => {
            None
        }
        pg_sys::INTERVALOID => {
            pgrx::Interval::new(0,
                                3,
                                400000).into_datum()
        }
        pg_sys::UUIDOID => {
            None
        }

        pg_sys::INETOID => {
            None
        }
        pg_sys::POINTOID => {
            None
        }
        pg_sys::BOXOID => {
            None
        }
        pg_sys::CIRCLEOID => {
            None
        }
        pg_sys::JSONOID => {
            None
        }
        pg_sys::XMLOID => None,
        _ => None,
    }
}


pub fn index_to_datum(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    index: usize,
) -> Option<Datum> {
    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(index)
            .into_datum(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(index)
            .into_datum(),
        DataType::Binary => from_utf8(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(index)).unwrap_or("none").into_datum(),

        DataType::Int16 => array.as_primitive::<Int16Type>().value(index).into_datum(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(index).into_datum(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(index).into_datum(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(index).into_datum(),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .value(index)
            .into_datum(),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .value(index)
            .into_datum(),
        DataType::Decimal128(precision, scale) => None,
        DataType::Timestamp(TimeUnit::Microsecond, None) => None,
        DataType::Date32 => None,
        DataType::Timestamp(TimeUnit::Nanosecond, None) => None,
        _ => {
            // unsupported type
            None
        }
    }
}

pub fn run_df_sql() -> datafusion::common::Result<DataFrame> {
    let config =
        SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_target_partitions(8)
            .with_information_schema(true)
            .with_parquet_pruning(true)
            .with_parquet_bloom_filter_pruning(true)
            .with_batch_size(6666)
        ;

    let ctx = SessionContext::new_with_config(config);

    // the region must be set to the region where the bucket exists until the following
    // issue is resolved
    // https://github.com/apache/arrow-rs/issues/2795
    let region = "us-east-1";
    let bucket_name = "data1";

    let s3 = AmazonS3Builder::new()
        .with_allow_http(true)
        .with_bucket_name(bucket_name)
        .with_region(region)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_endpoint("http://10.24.11.54:9000")
        .build()?;

    let path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&path).unwrap();
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));

    let path = format!("s3://{bucket_name}/");
    let list_options = ListingOptions::new(Arc::new(ParquetFormat::new())).with_file_extension(".parquet");
    task::block_on(ctx.register_listing_table("hits", &path, list_options, None, None)
    ).expect("TODO: panic message");

    // execute the query
    let df = task::block_on(ctx.sql("SELECT \"RegionID\" from hits order by 1 limit 10;"));

    return df;
}

pub fn run_df_sql_local() -> datafusion::common::Result<DataFrame> {
    let config =
        SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_target_partitions(8)
            .with_information_schema(true)
            .with_parquet_pruning(true)
            .with_parquet_bloom_filter_pruning(true)
            .with_batch_size(6666)
        ;

    let ctx = SessionContext::new_with_config(config);

    task::block_on(ctx.register_parquet(
        "hits",
        &format!("/home/liyang/hits_10.parquet"),
        ParquetReadOptions::default(),
    ));


    // execute the query
    let df = task::block_on(ctx.sql("SELECT \"RegionID\"  FROM hits order by \"RegionID\" limit 100;"));
    return df;
}