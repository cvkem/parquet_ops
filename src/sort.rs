use itertools::Itertools;
use parquet::{
    basic::Type as PhysType,
    record::{Row, RowAccessor},
    schema::types::Type,
};
use std::{cmp::Ordering, sync::Arc};
use crate::{rowiterext::read_row_sample, find_field};
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
    fn get_partition_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering>;
    fn get_record_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering>;
    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool>;
    fn get_partition_message_schema(&self) -> String;
}

struct ParquetKey {
    name: String,
    sort_col: usize,
    phys_type: PhysType,
}

impl ParquetKey {
    fn new(name: String, schema: Arc<Type>) -> Self {
        let (sort_col, tpe) = find_field(schema, &name);
        let phys_type = tpe.get_physical_type();

        Self{name, sort_col, phys_type}
    }
}

impl sort_multistage_typed for ParquetKey {
    fn get_partition_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering> {
        match self.phys_type {
            PhysType::INT64 => Box::new(|left: &Row, right: &Row| left.get_long(0).unwrap().cmp(&right.get_long(0).unwrap())),
            PhysType::INT32 => Box::new(|left: &Row, right: &Row| left.get_int(0).unwrap().cmp(&right.get_int(0).unwrap())),
            other =>  panic!("columns of type '{other}' are not supported (yet)!")
        }
    }

    fn get_record_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering> {
        let col = self.sort_col;
        match self.phys_type {
            PhysType::INT64 => Box::new(move |left: &Row, right: &Row| left.get_long(col).unwrap().cmp(&right.get_long(col).unwrap())),
            PhysType::INT32 => Box::new(move |left: &Row, right: &Row| left.get_int(col).unwrap().cmp(&right.get_int(col).unwrap())),
            other =>  panic!("columns of type '{other}' are not supported (yet)!")
        }
    }

    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool> {
        match self.phys_type {
            PhysType::INT64 => {
                let col = self.sort_col;
                let upper_bound = partition_row.get_long(col).unwrap();
                Box::new(move|row: &Row| row.get_long(col).unwrap() <= upper_bound)},
            PhysType::INT32 => {
                let col = self.sort_col;
                let upper_bound = partition_row.get_int(col).unwrap();
                Box::new(move|row: &Row| row.get_int(col).unwrap() <= upper_bound)},
            other =>  panic!("columns of type '{other}' are not supported (yet)!")
        }
    }

    fn get_partition_message_schema(&self) -> String {
        let type_label = match self.phys_type {
            PhysType::INT64 => "INT64",
            PhysType::INT32 => "INT32",
            other =>  panic!("columns of type '{other}' are not supported (yet)!")
        };
        
        format!("
        message schema {{
          REQUIRED {type_label} {};
        }}", self.name)
    }
}

/// Sort the input in two passes. The first pass returns a file with sorted row-groups. In the second pass these row-groups are merged.
pub fn sort_multistage(
    input_path: &str,
    sorted_path: &str,
    sort_field_name: &str,
) {
    // Open reader 'RowIterExt' such that we get access to the schema (and know the file/object is readable)
    let mut input = RowIterExt::new(input_path);
    assert!(input.head().is_some());
    let schema = Arc::new(input.schema().clone());

    let parquet_key = ParquetKey::new(sort_field_name.to_owned(), Arc::clone(&schema));

    let single_column_message_type = parquet_key.get_partition_message_schema();
                                                   // row 'account' should be flexible.
    let mut sample = read_row_sample(input_path, 1000, &single_column_message_type);

    sample.sort_unstable_by(parquet_key.get_partition_compare_fn());
    let num_part = 3;
    let step_size = sample.len() / (num_part - 1);
    let partition = sample
        .into_iter()
        .batching(|it| it.skip(step_size - 1).next()) // deterministic step_by that always skips step_size -1 fields before taking the first value
        //        .step_by(step_size)
        .collect::<Vec<_>>();

    let base_path_stage_1 = sorted_path.to_owned() + ".intermediate";

    let interm_paths: Vec<_> = (0..num_part)
        .map(|i| format!("{}-{}", base_path_stage_1, i))
        .collect();
    let mut row_writer: Vec<_> = interm_paths
        .iter()
        .map(|path| RowWriteBuffer::new(&path, Arc::clone(&schema), 10000).unwrap())
        .collect();

    println!("Enter phase-1: writing to intermedidate file(s) {base_path_stage_1}.<N>");
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

    println!("Closing the RowWriteBuffers for base: {base_path_stage_1}");
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
