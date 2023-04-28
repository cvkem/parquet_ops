use std::{
    sync::Arc,
    cmp::Ordering};
use itertools::Itertools;
use parquet::{
        record::{Row,
            RowAccessor
        }
};
use crate::rowiterext::read_row_sample;

use crate::{ACCOUNT_ONLY_TYPE, ID_ONLY_TYPE};

use super::rowiterext::RowIterExt;
use super::rowwritebuffer::RowWriteBuffer;

const MAX_SORT_BLOCK: u64 = 1_000_000; //10_000  // 1 in REPORT_APPEND_STEP rows is reported on the console.

/// sort the input in one pass and writer it to the sorted-path 
pub fn sort(input_path: &str, sorted_path: &str,  comparator: fn(&Row, &Row) -> Ordering) {

    let mut input = RowIterExt::new(input_path);
    assert!(input.head().is_some());

    let schema = Arc::new(input.schema().clone());
    let mut row_writer = RowWriteBuffer::new(sorted_path, schema, 10000).unwrap();

    if let Some(mut data) = input.take(MAX_SORT_BLOCK) {
        if let Some(_) = input.take(1) {
            panic!("the input-file contained more than {MAX_SORT_BLOCK} rows. Use the sort operation instead (multi-stage sort), which can handle huge files");
        };
        data.sort_by(comparator);

        row_writer.append_row_group(data);
    };

    println!("Closing the RowWriteBuffer  row_writer.close()");
    row_writer.close();
}


/// Sort the input in two passes. The first pass returns a file with sorted row-groups. In the second pass these row-groups are merged. 
pub fn sort_multistage(input_path: &str, sorted_path: &str,  comparator: fn(&Row, &Row) -> Ordering) {

    // row 'account' should be flexible.
    let mut sample = read_row_sample(input_path, 1000, ID_ONLY_TYPE)
                    .iter()
                    .map(|r| r.get_long(0).unwrap().to_owned())
                    .collect::<Vec<_>>();
    sample.sort();
    let num_part = 2;
    let step_size = sample.len() / num_part;
    let partition = sample
        .into_iter()
        .step_by(step_size)
        .collect::<Vec<_>>();

    let mut input = RowIterExt::new(input_path);
    assert!(input.head().is_some());

    let schema = Arc::new(input.schema().clone());
    let mut row_writer = RowWriteBuffer::new(sorted_path, schema, 10000).unwrap();

    while let Some(mut data) = input.take(MAX_SORT_BLOCK) {
            data.sort_by(comparator);

            let mut i: usize = 0;  // skip first field as it is the lowest value and thus seems to be a zero-partition ??
            let mut ready: bool = false;

            data
                .into_iter()
                .peekable()
                .batching(|it| {
                    if ready 
                        { return None; };  // early termination as end of iterator is flagged.

                    let data: Vec<_> = if i < partition.len() {
                        let bar = &partition[i];
                        println!("{i}:  Create batch for upper bound {bar}");
                        // using it.take_while(..) does not work, as it loses the first item of the next partition.
                        // so we implement this alternative 
                        let mut data = Vec::new();
                        while let Some(r) = it.peek() {
                            if r.get_long(0).unwrap() < *bar {
                                data.push(it.next().unwrap());
                            } else {
                                break;
                            }
                        };
                        data
//                        it.take_while(|r| r.get_long(0).unwrap() <= *bar).collect()
                    } else {
                        ready = true; // flag that next iteration should return None
                        println!("Taking the remaining rows");
                        let data: Vec<_> =  it.collect();
                        if data.len() == 0 {
                            return None
                        };
                        data
                    };
                    println!("collected a dataset of size {}", data.len());
                    // move to next partition
                    i = i+1;
                    Some(data)})
                .for_each(|data| {
                    println!("Now appending a row-group of length: {}", data.len());
                    let ids: Vec<_> = data.iter().map(|r| r.get_long(0)).collect();
                    println!(" The collected ids are: {ids:?}");
                    row_writer.append_row_group(data)})
    };

    println!("Closing the RowWriteBuffer  row_writer.close()");
    row_writer.close();
}


