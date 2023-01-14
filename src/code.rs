/// Collection of different pieces of code. 
/// We need to split out the test-generation code and then throw out the remainder of this file as it is old code.
/// Generation of Parquet test-files whould be based on the rowwritebuffer, which is more generic.
/// 
use std::{ 
    any::type_name,
    cmp,
    fs,
    io,
    path::Path, 
    sync::Arc,
    time::Instant};
use parquet::{
    basic::Compression,
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    file::{
        properties::WriterProperties,
        writer::SerializedFileWriter,
        reader::SerializedFileReader,
        reader::FileReader
    },
    schema::{parser::parse_message_type,
        types::Type}
};



// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

const NESTED: bool = false;


use super::ttypes;

const NUM_TX_PER_ACCOUNT: u64 = 10;

// extract a label from a number
fn make_label(idx: u64) -> String {
    let mut idx = idx / NUM_TX_PER_ACCOUNT;  // first part is amount.
    let mut stack = Vec::new();
    for _i in 1..7 {
        let val =  (idx % 26) as u32 + ('a' as u32);
        stack.push(char::from_u32(val).unwrap());
        idx /= 26;
    }
    let s: String = stack.into_iter().rev().collect();
    s
}


// extract an amount from a number
fn find_amount(idx: u64) -> i32 {
    (idx % NUM_TX_PER_ACCOUNT) as i32 - (NUM_TX_PER_ACCOUNT as i32)/2 + 1
}

const BASE: u64 = 123456789;

fn find_text(col: i16, idx: u64) -> String {
    let mut seed = col as u64 * BASE + idx;
    let mut chars = Vec::new();
    for _i in 0..100 {
        let val =  (seed % 26) as u32 + ('a' as u32);
        chars.push(char::from_u32(val).unwrap());
        seed /= 2;
    } 
    chars.into_iter().collect()
}


fn write_parquet_row_group_nested(writer: &mut SerializedFileWriter<fs::File>, start: u64, end: u64) {
    let mut row_group_writer = writer.next_row_group().unwrap();
    let mut col_nr = 0;

    // // ugly construct
    // let x = [0_i16];
    // let mut repetition: &mut &[i16] = &mut &x[0..];
    let mut repetition: Vec<i16> = Vec::new();
    let mut definition_levels: Vec<i16> = Vec::new();

    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // ... write values to a column writer
        match col_nr {
            0 => {
                let values: Vec<_> = (start..end)
                    .map(|i| {
//                        let bs = ;
                        ByteArray::from(make_label(i).as_bytes().to_vec())
                    } )
                    .collect();

// better solution: use   dedup_with_count
// https://docs.rs/itertools/latest/itertools/trait.Itertools.html#method.dedup
                // update values to remove duplicates and determine a repitition level for subsequent columns
                let values = values.into_iter().fold(Vec::new(), 
                    |mut vals, v| {
                        if vals.len() == 0 || vals[vals.len()-1] != v { 
                            vals.push(v);
                            repetition.push(0); 
                        } else { 
                            repetition.push(1);
                        }
                        vals
                    });

                // compute the definition level corresponding to the repetition-level
                definition_levels = (0..repetition.len()).map(|_| 1_i16).collect::<Vec<i16>>();

                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                    .expect("writing String column");
            },
            1 => {
                    let distinct = 1000 as u64;
                    let values: Vec<i32> =  (start..end)
                        .map(|i| find_amount(i))
                        .collect();

                        col_writer
                        .typed::<Int32Type>()
                        .write_batch_with_statistics(&values, Some(&definition_levels[0..]), Some(&repetition[0..]), Some(&-499), Some(&500), Some(distinct))
                        .expect("writing i32 column");
                },
            col_idx => {
                    let values: Vec<_> =  (start..end)
                        .map(|i| ByteArray::from(find_text(col_idx, i).as_bytes().to_vec()))
                        .collect();
                // println!("Now writing values with repetition {:?} of length {}", &repetition, &repetition.len());
                col_writer
                        .typed::<ByteArrayType>()
                        .write_batch_with_statistics(&values, Some(&definition_levels[0..]), Some(&repetition[0..]), None, None, None)
                        .expect("writing i32 column");
                },
            
//            _ => panic!("incorrect column number")
            }
            col_nr += 1;
            col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();

}



fn write_parquet_row_group(writer: &mut SerializedFileWriter<fs::File>, 
        start: u64, end: u64,
        selection: Option<fn(&u64) -> bool>) {
    let mut row_group_writer = writer.next_row_group().unwrap();
    let mut col_nr = 0;
    let selection = selection.unwrap_or(|_i| true);

    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // ... write values to a column writer
        let rec_ids = (start..end).
            filter(selection);

        match col_nr {
            0 => {
                let values: Vec<i64> = rec_ids
                    .map(|i| (i as i64))
                    .collect();
                col_writer
                    .typed::<Int64Type>()
                    .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                    .expect("writing Int64 ID-column");
            },
            1 => {
                let values: Vec<_> = rec_ids
                    .map(|i| {
                        ByteArray::from(make_label(i).as_bytes().to_vec())
                    } )
                    .collect();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch_with_statistics(&values[..], None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                    .expect("writing String column");
            },
            2 => {
                    let distinct = 1000 as u64;
                    let values: Vec<i32> =  rec_ids
                        .map(find_amount)
                        .collect();
                col_writer
                        .typed::<Int32Type>()
                        .write_batch_with_statistics(&values, None, None, Some(&-499), Some(&500), Some(distinct))
                        .expect("writing i32 column");
                },
            3 => {
                let timebase = 1644537600;   // epoch-secs on 11-12-2022 
                let values: Vec<i64> = rec_ids
                    .map(|i| ((i as i64) + timebase) * 1000)
                    .collect();
                col_writer
                    .typed::<Int64Type>()
                    .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                    .expect("writing TimeStamp column");
            },
                col_idx => {
                let values: Vec<_> = rec_ids
                    .map(|i| ByteArray::from(find_text(col_idx, i).as_bytes().to_vec()))
                    .collect();
                col_writer
                        .typed::<ByteArrayType>()
                        .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                        .expect("writing String column");
                },
//                _ => panic!("incorrect column number")
            }
            col_nr += 1;
            col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();

}



pub fn write_parquet(path: &Path, extra_columns: usize, num_recs: Option<u64>, group_size: Option<u64>,
    selection: Option<fn(&u64) -> bool>) -> Result<(), io::Error> {
    let long_schema =  ttypes::get_schema_str(extra_columns);
    let message_type = if NESTED {ttypes::LONG_NESTED_MESSAGE_TYPE} else { &long_schema };
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    let now = Instant::now();

    let mut start = 0;
    let end = num_recs.unwrap_or(1000);
    let group_size = group_size.unwrap_or(60);
    let mut ng = 1;
    while start < end {
        let group_end = cmp::min(start+group_size, end);

        if NESTED {
            write_parquet_row_group_nested(&mut writer, start, group_end);
        } else {
            write_parquet_row_group(&mut writer, start, group_end, selection);
        }

        let last = group_end -1;
        println!("End of group {} in {:?} has value {} is account {} with amount {}", ng, now.elapsed(), last, make_label(last), find_amount(last));
        start += group_size;
        ng += 1;
    }

    writer.close().unwrap();
    Ok(())
}


const SHOW_FIRST_GROUP_ONLY: bool = true;

fn print_schema(schema: &Type) {
    for (idx, fld) in schema.get_fields().iter().enumerate() {
        println!("idx={}   {:?}", &idx, &fld);
        let nme = fld.get_basic_info().name();
        let conv_type = fld.get_basic_info().converted_type();
        let phys_type = fld.get_physical_type();
        println!("idx={}   name={}, conv_type={} and physicalType={}", &idx, &nme, &conv_type, &phys_type);
    }
}

pub fn read_parquet_metadata(path: &Path) {
    if let Ok(file) = fs::File::open(path) {

        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let file_metadata = parquet_metadata.file_metadata();

        println!("For path={:?} found file-metadata:\n {:?}", &path, &file_metadata);

        {
            let schema = file_metadata.schema();
            println!(" Schema = {:#?}  of type {}", &schema, type_of(&schema));

            print_schema(schema);

        }
//        let rows = file_metadata.num_rows();
        
        for (idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
            println!("  rowgroup: {} has meta {:#?}", idx, rg);
            if SHOW_FIRST_GROUP_ONLY {
                println!(" {} groups in total, but only first shown.", parquet_metadata.row_groups().len());
                break;
            }
        }

        let fields = parquet_metadata.file_metadata().schema().get_fields();

        println!("\nAnd fields:\n{:#?}", &fields)
    } else {
        println!("Failed to open file {:?}", path);
    }
}


// async fn read_parquet(path: &Path, acc_name: Option<String>) {
//     if let Ok(file) = fs::File::open(path) {

//         // let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
//         // // let metadata = parse_metadata(&builder).unwrap();
//         // // let parquet_schema = metadata.file_metadata().schema_descr_ptr();
//         // // println!("Converted arrow parquet_schema_ptr is: {:?}", parquet_schema);


//         // println!("Converted arrow builder.schema is: {:?}", builder.schema());
//         // println!("Converted arrow builder.parquet_schema is: {:?}", builder.parquet_schema());
//         // let parquet_schema = builder.parquet_schema();

//         let a_filter = ArrowPredicateFn::new(
//             ProjectionMask::leaves(&parquet_schema, vec![0]),
//             |batch| arrow::compute::eq_dyn_utf8_scalar(batch.column(0), &"hello"),
//         );

//         let row_filter = RowFilter::new(vec![Box::new(a_filter)]);

//         let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 1]);

//         // let mask = ProjectionMask::leaves(&parquet_schema, vec![0, 2]);

//         let options = ArrowReaderOptions::new().with_page_index(true);
//         let async_reader = AsyncReader(file);
//         let stream =
//             ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
//                 .await
//                 .unwrap()
//                 .with_projection(mask.clone())
//                 .with_batch_size(1024)
//                 .with_row_filter(row_filter)
//                 .build()
//                 .unwrap();

// //         let predicate: dyn ArrowPredicate = Box::new(ArrowPredicateFn::new(ProjectionMask::roots(&schema_descr, [0]), filter));
// //         // where
// //         //    F: FnMut(RecordBatch) -> ArrowResult<BooleanArray> + Send + 'static,
// //         let predicates = Vec::new();
// //         predicates.push(Box::new(predicate));
// // //        predicates.push(predicate);
// //         let row_filter = RowFilter::new(predicates);

//         let mut reader = builder
//             .with_projection(mask.clone())
//             .with_batch_size(100)
//             .with_row_filter(row_filter)
//             .build()
//             .unwrap();

//         let record_batch = reader.next().unwrap().unwrap();

//         println!("Read {} records.", record_batch.num_rows());

//         println!(" Type of reader = {}", type_of(&record_batch));
//         for (col_idx, arr) in record_batch.columns().iter().enumerate() {
//             println!("\ncolumnn {col_idx}  has first 5 values {:?}", &arr);
//         }
//     } else {
//         println!("Failed to open file {:?}", path);
//     }
// }
