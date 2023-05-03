use itertools::Itertools;
use parquet::{
    record::{Row, RowAccessor},
    schema::types::Type};
use std::{cmp::Ordering, sync::Arc};
use crate::rowiterext::RowIterExt;
use crate::rowwritebuffer::RowWriteBuffer;
use super::parquet_key::{ParquetKey, sort_multistage_typed};
use super::partition::partitioning;

const MAX_SORT_BLOCK: u64 = 1_000_000; //10_000  // 1 in REPORT_APPEND_STEP rows is reported on the console.

/// sort the input (parquet-file) in one pass and writer it to the sorted-path
/// Internal function: The 'input' iterator is already created by the 'sort' method that selects 'sort_simple' or 'sort_multi_stage'
pub fn sort_simple(
    mut input: RowIterExt,
    schema: Arc<Type>,
    sorted_path: &str,
    comparator: Box<dyn Fn(&Row, &Row) -> Ordering>) {
    let mut row_writer = RowWriteBuffer::new(sorted_path, schema, 10000).unwrap();

    if let Some(mut data) = input.take(MAX_SORT_BLOCK) {
        if let Some(_) = input.take(1) {
            panic!("the input-file contained more than {MAX_SORT_BLOCK} rows. Use the sort operation instead (multi-stage sort), which can handle huge files");
        };
        data.sort_by(comparator);

        row_writer.append_row_group(data);
    };

    row_writer.close();
}


/// Sort the input in two passes. The first pass returns a file with sorted row-groups. In the second pass these row-groups are merged.
/// Internal function: The 'input' iterator is already created by the 'sort' method that selects 'sort_simple' or 'sort_multi_stage'
pub fn sort_multistage(
    mut input: RowIterExt,
    schema: Arc<Type>,
    input_path: &str,
    sorted_path: &str,
    parquet_key: ParquetKey,
) {
    let partition = partitioning(input_path, &parquet_key, 3);

    let num_row_writer = partition.len()+1;  // Last row_writer is needed to store the tail (N partitions result in N+1 segments.
    let interm_paths: Vec<_> = (0..num_row_writer)
        .map(|i| sorted_path.replace(".parquet", &format!("intermediate-{}.parquet", i)).to_owned())
        .collect();
    let mut row_writer: Vec<_> = interm_paths
        .iter()
        .map(|path| RowWriteBuffer::new(&path, Arc::clone(&schema), 10000).unwrap())
        .collect();

    println!("Enter phase-1: writing to intermedidate file(s) {}.<N>", interm_paths[0]);
    while let Some(mut data) = input.take(MAX_SORT_BLOCK) {
        data.sort_by(parquet_key.get_record_compare_fn()); 
        println!("Retrieved {} rows from input-file", data.len());

        let mut i: usize = 0; // skip first field as it is the lowest value and thus seems to be a zero-partition ??
        let mut ready: bool = false;

        data.into_iter()
            .peekable()
            .batching(|it| {
                if ready {
                    return None;
                }; // early termination as end of iterator is flagged.

                let data: Vec<_> = if i < partition.len() {
                    let check_in_partition = parquet_key.get_partition_filter_fn(&partition[i]);
                    // Using iter.take_while(..) does not work, as it loses the first item of the next partition.
                    // so we implement this alternative
                    let mut data = Vec::new();
                    while let Some(r) = it.peek() {
                        if check_in_partition(r) {
                            data.push(it.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    data
                } else {
                    ready = true; // flag that next iteration should return None
                    println!("Taking the remaining rows");
                    let data: Vec<_> = it.collect();
                    if data.len() == 0 {
                        return None;
                    };
                    data
                };
                println!("partition {i}: collected a dataset of size {}", data.len());
                // move to next partition
                let idx = i;
                i = i + 1;
                Some((idx, data))
            })
            .for_each(|(idx, data)| {
                println!("Now appending a row-group of length: {}", data.len());
                let ids: Vec<_> = data.iter().map(|r| r.get_long(0)).collect();
                println!(" The collected ids are: {ids:?}");
                row_writer[idx].append_row_group(data)
            })
    }

    println!("Closing the RowWriteBuffers for base: {}", interm_paths[0].replace('0', "<N>"));
    row_writer.iter_mut()
        .for_each(|rw| rw.close());

    println!("Move intermediate data to the final file '{sorted_path}'");
    let mut row_writer = RowWriteBuffer::new(&sorted_path, Arc::clone(&schema), 10000).unwrap();

    interm_paths.iter().for_each(|interm_path| {
        let mut input = RowIterExt::new(interm_path);
        let Some(mut data) = input.take(u64::MAX) else {
                println!("The file '{interm_path}' contains no data-rows.");
                return;
            };
        data.sort_by(parquet_key.get_record_compare_fn());
        row_writer.append_row_group(data);
    });
    row_writer.close();
}
