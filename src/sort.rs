use std::{
    sync::Arc,
    cmp::Ordering};
use parquet::{
        record::{Row,
            RowAccessor
        }
};

use super::rowiterext::RowIterExt;
use super::rowwritebuffer::RowWriteBuffer;

const MAX_SORT_BLOCK: u64 = 1_000_000; //10_000  // 1 in REPORT_APPEND_STEP rows is reported on the console.

/// sort the input in one pass and writer it to the sorted-path 
pub fn sort_direct(input_path: &str, sorted_path: &str,  comparator: fn(&Row, &Row) -> Ordering) {

    let mut input =RowIterExt::new(input_path);
    assert!(input.head().is_some());

    let schema = Arc::new(input.metadata().file_metadata().schema().clone());
    let mut row_writer = RowWriteBuffer::new(sorted_path, schema, 10000).unwrap();

    if let Some(mut data) = input.take(MAX_SORT_BLOCK) {
        if let Some(_) = input.take(1) {
            panic!("the input-file contained more than {MAX_SORT_BLOCK} rows. Use the sort operation instead (2-stage sort)");
        };
        data.sort_by(comparator);

        row_writer.append_row_group(data);
    };

    println!("Closing the RowWriteBuffer  row_writer.close()");
    row_writer.close();
}


/// Sort the input in two passes. The first pass returns a file with sorted row-groups. In the second pass these row-groups are merged. 
pub fn sort(input_path: &str, sorted_path: &str,  comparator: fn(&Row, &Row) -> Ordering) {

    let mut input =RowIterExt::new(input_path);
    assert!(input.head().is_some());

    let schema = Arc::new(input.metadata().file_metadata().schema().clone());
    let mut row_writer = RowWriteBuffer::new(sorted_path, schema, 10000).unwrap();

    while let Some(mut data) = input.take(MAX_SORT_BLOCK) {
            data.sort_by(comparator);

            row_writer.append_row_group(data);
    };

    println!("Closing the RowWriteBuffer  row_writer.close()");
    row_writer.close();
}


