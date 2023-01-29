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
    time::Instant};
use parquet::{
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    file::{
        writer::SerializedFileWriter,
        reader::SerializedFileReader,
        reader::FileReader
    },
    schema::types::Type
};

use super::ttypes;

use super::parquet_writer::{self, ParquetWriter};


// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}


fn write_parquet_row_group_nested<W: io::Write>(writer: &mut SerializedFileWriter<W>, start: u64, end: u64) {
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
                        ByteArray::from(ttypes::make_label(i).as_bytes().to_vec())
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
                        .map(|i| ttypes::find_amount(i))
                        .collect();

                        col_writer
                        .typed::<Int32Type>()
                        .write_batch_with_statistics(&values, Some(&definition_levels[0..]), Some(&repetition[0..]), Some(&-499), Some(&500), Some(distinct))
                        .expect("writing i32 column");
                },
            col_idx => {
                    let values: Vec<_> =  (start..end)
                        .map(|i| ByteArray::from(ttypes::find_text(col_idx, i).as_bytes().to_vec()))
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



fn write_parquet_row_group<W: io::Write>(writer: &mut SerializedFileWriter<W>, 
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
                        ByteArray::from(ttypes::make_label(i).as_bytes().to_vec())
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
                        .map(ttypes::find_amount)
                        .collect();
                col_writer
                        .typed::<Int32Type>()
                        .write_batch_with_statistics(&values, None, None, Some(&-499), Some(&500), Some(distinct))
                        .expect("writing i32 column");
                },
            3 => {
                let values: Vec<i64> = rec_ids
                    .map(ttypes::find_time)
                    .collect();
                col_writer
                    .typed::<Int64Type>()
                    .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                    .expect("writing TimeStamp column");
            },
                col_idx => {
                let values: Vec<_> = rec_ids
                    .map(|i| ByteArray::from(ttypes::find_text(col_idx, i).as_bytes().to_vec()))
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


pub fn write_parquet(path: &str, extra_columns: usize, num_recs: Option<u64>, group_size: Option<u64>,
    selection: Option<fn(&u64) -> bool>) -> Result<(), io::Error> {

    let now = Instant::now();
    let schema = ttypes::get_test_schema(extra_columns.try_into().unwrap());
    let mut pw = parquet_writer::get_parquet_writer(path, schema);
    
    let mut start = 0;
    let end = num_recs.unwrap_or(1000);
    let group_size = group_size.unwrap_or(60);
    let mut ng = 1;
    while start < end {
        let group_end = cmp::min(start+group_size, end);

        if ttypes::NESTED {
            // match type to call the right type of writer
            match &mut pw {
                ParquetWriter::FileWriter(ref mut writer) => write_parquet_row_group_nested(writer, start, group_end),
                ParquetWriter::S3Writer(ref mut writer) => write_parquet_row_group_nested(writer, start, group_end)
            }
        } else {
            match &mut pw {
                ParquetWriter::FileWriter(ref mut writer) => write_parquet_row_group(writer, start, group_end, selection),
                ParquetWriter::S3Writer(ref mut writer) => write_parquet_row_group(writer, start, group_end, selection)
            }
        }

        let last = group_end -1;
        println!("End of group {} in {:?} has value {} is account {} with amount {}", ng, now.elapsed(), last, ttypes::make_label(last), ttypes::find_amount(last));
        start += group_size;
        ng += 1;
    }

    match pw {
        ParquetWriter::FileWriter(writer) => writer.close().unwrap(),
        ParquetWriter::S3Writer(writer) => writer.close().unwrap()
    };

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
