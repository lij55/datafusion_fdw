use std::fmt::Display;
use std::str::from_utf8;
use std::sync::Arc;

use async_std::task;
use datafusion::arrow::array::{Array, AsArray, BinaryArray, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimeUnit, UInt32Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use url::Url;

pub fn get_data_as_str(
    array: &Arc<dyn Array>,
    index: usize,
) -> String {
    println!("{}", array.data_type());
    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(index)
            .to_string(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(index)
            .to_string(),
        DataType::Binary => from_utf8(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(index)).unwrap_or("none").to_string(),

        DataType::Int16 => array.as_primitive::<Int16Type>().value(index).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(index).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(index).to_string(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(index).to_string(),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .value(index)
            .to_string(),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .value(index)
            .to_string(),
        DataType::Decimal128(precision, scale) => String::from("123.456"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => String::from("2022-01-01 00:00:00.000000"),
        DataType::Date32 => String::from("2022-01-01"),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => String::from("2022-01-01 00:00:00.000000"),
        _ => String::from("unknown data"),
    }
}

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
fn main2() -> Result<()> {
    // execute the query
    let df = run_df_sql().unwrap();

    // print the results
    //task::block_on(df.clone().show()).unwrap();
    let results = task::block_on(df.collect()).unwrap();


    for records in results {
        for r in 0..records.num_rows() {
            let result = records.columns().iter().map(|c| {
                get_data_as_str(c, r)
            }).collect::<Vec<String>>().join(",");
            println!("{}", result);
        }
    }

    Ok(())
}

fn transpose2<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    assert!(!v.is_empty());
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
    (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .map(|n| n.next().unwrap())
                .collect::<Vec<T>>()
        })
        .collect()
}

pub fn transpose_recordbatch(batch: RecordBatch) -> Vec<Vec<String>> {
    let ret = batch.columns().iter().map(|c| {
        c.as_primitive::<Int32Type>().iter().map(|v| format!("{}", v.unwrap_or(0))).collect()
    }
    ).collect::<Vec<Vec<String>>>();
    transpose2::<String>(ret)
}

pub fn recordbatch_to_row(batch: RecordBatch) -> Vec<Vec<String>> {
    let mut ret = vec![];
    for r in 0..batch.num_rows() {
        let result = batch.columns().iter().map(|c| {
            get_data_as_str(c, r)
        }).collect::<Vec<String>>();
        //println!("{}", result);
        ret.push(result)
    }
    ret
}

fn main() -> Result<()> {
    // execute the query
    //let df = run_df_sql().unwrap();
    let df = run_df_sql_local().unwrap();

    // print the results
    //task::block_on(df.clone().show()).unwrap();
    let results = task::block_on(df.collect()).unwrap();

    println!("{}", results.len());

    for records in results {
        println!("{}", records.num_rows());
        // for r in 0..records.num_rows() {
        //     let result = records.columns().iter().map(|c| {
        //         get_data_as_str(c, r)
        //     }).collect::<Vec<String>>().join(",");
        //     //println!("{}", result);
        // }
        let result = transpose_recordbatch(records);
        for i in result {
            // println!("{}", i.join(";"))
        }
    }

    Ok(())
}

fn run_df_sql() -> Result<DataFrame> {
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
    let df = task::block_on(ctx.sql("SELECT \"RegionID\"  FROM hits order by \"RegionID\" limit 1000000;"));
    return df;
}

fn run_df_sql_local() -> Result<DataFrame> {
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