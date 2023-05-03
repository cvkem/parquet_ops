use std::sync::Arc;
use super::rowiterext::RowIterExt;

mod parquet_key;
use parquet_key::{ParquetKey, sort_multistage_typed};
mod partition;
mod sort_algo;
use sort_algo::{sort_simple, sort_multistage};


const MAX_SIZE_SIMPLE_SORT: usize = 2_000_000_000;

/// sort the input in one pass and writer it to the sorted-path
pub fn sort(input_path: &str, sorted_path: &str, sort_field_name: &str) {
    // Open reader 'RowIterExt' such that we get access to the schema (and know the file/object is readable)
    let input = RowIterExt::new(input_path);
    assert!(input.head().is_some());
    let schema = Arc::new(input.schema().clone());

    let parquet_key = ParquetKey::new(sort_field_name.to_owned(), Arc::clone(&schema));

    // TODO: add size computation to determine the right kind of sort-algorithm
    let obj_size = 2_000_000_001;  // to be added
    if obj_size < MAX_SIZE_SIMPLE_SORT {
        sort_simple(input, schema, sorted_path, parquet_key.get_record_compare_fn());
    } else {
        sort_multistage(input, schema, input_path, sorted_path, parquet_key);
    }
}


