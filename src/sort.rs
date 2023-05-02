use itertools::Itertools;
use parquet::{
    record::{Row, RowAccessor},
    schema::types::Type as Parquet_type,
};
use std::{cmp::Ordering, marker::PhantomData, sync::Arc};

use crate::rowiterext::read_row_sample;

use crate::{ACCOUNT_ONLY_TYPE, ID_ONLY_TYPE};

use super::rowiterext::RowIterExt;
use super::rowwritebuffer::RowWriteBuffer;

const MAX_SORT_BLOCK: u64 = 1_000_000; //10_000  // 1 in REPORT_APPEND_STEP rows is reported on the console.

/// sort the input in one pass and writer it to the sorted-path
pub fn sort(input_path: &str, sorted_path: &str, comparator: fn(&Row, &Row) -> Ordering) {
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

pub trait sort_multistage_typed {
    type Output;
    fn get_key_partition_fn(&self) -> Box<dyn Fn(&Row) -> Self::Output>;
    fn get_key_record_fn(&self) -> Box<dyn Fn(&Row) -> Self::Output>;
    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool>;
    fn get_partition_message_schema(&self) -> String;
}

struct ParquetKey_i32 {
    col: usize,
    name: String,
}

impl ParquetKey_i32 {
    fn new(col: usize, name: String) -> Self {
        Self{col, name}
    }
}

impl sort_multistage_typed for ParquetKey_i32 {
    type Output = i32;
    fn get_key_partition_fn(&self) -> Box<dyn Fn(&Row) -> Self::Output> {
        Box::new(|row: &Row| row.get_int(0).unwrap())
    }

    fn get_key_record_fn(&self) -> Box<dyn Fn(&Row) -> Self::Output> {
        let col = self.col;
        Box::new(move |row: &Row| row.get_int(col).unwrap())
    }

    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool> {
        let partition_value = self.get_key_partition_fn()(partition_row);
        let get_key = self.get_key_record_fn();
        Box::new(move |row: &Row| get_key(row) <= partition_value)
    }

    fn get_partition_message_schema(&self) -> String {
        format!("
        message schema {{
          REQUIRED INT32 {};
        }}", self.name)
    }
}


struct ParquetKey_i64 {
    col: usize,
    name: String,
}

impl ParquetKey_i64 {
    fn new(col: usize, name: String) -> Self {
        Self{col, name}
    }
}

impl sort_multistage_typed for ParquetKey_i64 {
    type Output = i64;
    fn get_key_partition_fn(&self) -> Box<dyn Fn(&Row) -> Self::Output> {
        Box::new(|row: &Row| row.get_long(0).unwrap())
    }

    fn get_key_record_fn(&self) -> Box<dyn Fn(&Row) -> Self::Output> {
        let col = self.col;
        Box::new(move |row: &Row| row.get_long(col).unwrap())
    }

    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool> {
        let partition_value = self.get_key_partition_fn()(partition_row);
        let get_key = self.get_key_record_fn();
        Box::new(move |row: &Row| get_key(row) <= partition_value)
    }

    fn get_partition_message_schema(&self) -> String {
        format!("
        message schema {{
          REQUIRED INT64 {};
        }}", self.name)
    }
}



/// Sort the input in two passes. The first pass returns a file with sorted row-groups. In the second pass these row-groups are merged.
pub fn sort_multistage(
    input_path: &str,
    sorted_path: &str,
    comparator: fn(&Row, &Row) -> Ordering,
) {
    // Open reader 'RowIterExt' such that we get access to the schema (and know the file/object is readable)
    let mut input = RowIterExt::new(input_path);
    assert!(input.head().is_some());
    let schema = Arc::new(input.schema().clone());

    println!("#####\nSchema = {:#?}\n###", schema);

    let single_column_message_type = ID_ONLY_TYPE; // to be moved to interface-trait
                                                   // row 'account' should be flexible.
    let mut sample = read_row_sample(input_path, 1000, single_column_message_type);
    // .iter()
    // .map(|r| r.get_long(0).unwrap().to_owned())
    // .collect::<Vec<_>>();
    let sort_key = |r: &Row| r.get_long(0).unwrap().to_owned(); // to be moved to interface-trait
    sample.sort_unstable_by_key(sort_key);
    let num_part = 3;
    let step_size = sample.len() / (num_part - 1);
    let partition = sample
        .into_iter()
        .batching(|it| it.skip(step_size - 1).next()) // deterministic step_by that always skips step_size -1 fields before taking the first value
        //        .step_by(step_size)
        .collect::<Vec<_>>();

    let path_stage_1 = sorted_path.to_owned() + ".intermediate";
    // let mut row_writer = Vec::new();
    // for i in 0..num_part {
    //     let path = format!("{}-{}", path_stage_1, i);
    //     let schema = Arc::new(input.schema().clone());
    //     let mut row_writer_elem = RowWriteBuffer::new(&path, schema, 10000).unwrap();
    //     row_writer.push(row_writer_elem);
    // };
    let interm_paths: Vec<_> = (0..num_part)
        .map(|i| format!("{}-{}", path_stage_1, i))
        .collect();
    let mut row_writer: Vec<_> = interm_paths
        .iter()
        .map(|path| RowWriteBuffer::new(&path, Arc::clone(&schema), 10000).unwrap())
        .collect();

    println!("Enter phase-1: writing to intermedidate file(s) {path_stage_1}");
    while let Some(mut data) = input.take(MAX_SORT_BLOCK) {
        data.sort_by(comparator); // to be moved to interface-trait (or use sort_by_key)

        let mut i: usize = 0; // skip first field as it is the lowest value and thus seems to be a zero-partition ??
        let mut ready: bool = false;

        data.into_iter()
            .peekable()
            .batching(|it| {
                if ready {
                    return None;
                }; // early termination as end of iterator is flagged.

                let data: Vec<_> = if i < partition.len() {
                    let check_in_partition = {
                        // to be moved to interface-trait
                        let bar = partition[i].get_long(0).unwrap();
                        println!("{i}:  Create batch for upper bound {bar}");
                        move |r: &Row| r.get_long(0).unwrap() < bar
                    };
                    // using it.take_while(..) does not work, as it loses the first item of the next partition.
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
                println!("collected a dataset of size {}", data.len());
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

    println!("Closing the RowWriteBuffer: {path_stage_1}");
    // for i in 0..num_part {
    //     row_writer[i].close();
    // }
    (0..num_part).for_each(|i| row_writer[i].close());

    println!("Move intermediate data to the final file '{sorted_path}'");
    let mut row_writer = RowWriteBuffer::new(&sorted_path, Arc::clone(&schema), 10000).unwrap();

    interm_paths.iter().for_each(|interm_path| {
        let mut input = RowIterExt::new(interm_path);
        assert!(input.head().is_some());
        let Some(mut data) = input.take(u64::MAX) else {
                println!("The file '{interm_path}' contains no data-rows.");
                return;
            };
        data.sort_by(comparator);
        row_writer.append_row_group(data);
    });
    row_writer.close();
}
