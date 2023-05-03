use super::parquet_key::{sort_multistage_typed, ParquetKey};
use crate::rowiterext::read_row_sample;
use itertools::Itertools;
use parquet::record::Row;

/// Get a partition over a large dataset
pub fn partitioning(input_path: &str, parquet_key: &ParquetKey, num_partition: usize) -> Vec<Row> {
    let single_column_message_type = parquet_key.get_partition_message_schema();
    // row 'account' should be flexible.
    let mut sample = read_row_sample(input_path, 1000, &single_column_message_type);

    sample.sort_unstable_by(parquet_key.get_partition_compare_fn());
    let step_size = sample.len() / (num_partition - 1);
    let partition = sample
        .into_iter()
        .batching(|it| it.skip(step_size - 1).next()) // deterministic step_by that always skips step_size -1 fields before taking the first value
        .collect::<Vec<_>>();

    partition
}
