use std::{ 
    cmp,
    io,
    time::Instant};
use parquet::{
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    file::writer::SerializedFileWriter,
    record::{Row,
        RowAccessor
    }
};

//use super::rowiterext::RowIterExt;
use super::rowwritebuffer::RowWriteBuffer;
use super::ttypes;
use super::parquet_writer::{self, ParquetWriter};
use super::REPORT_APPEND_STEP;


pub fn write_parquet(path: &str, extra_columns: usize, num_recs: Option<u64>, group_size: Option<u64>,
    selection: Option<fn(&u64) -> bool>) -> Result<(), io::Error> {

    let now = Instant::now();

    let schema = ttypes::get_test_schema(extra_columns.try_into().unwrap());

    let mut pw = parquet_writer::get_parquet_writer(path, schema.clone());

    // Next code should be the alternative if we have prepared rows.
    // would need some more refactoring to work with RowWriteBuffer
    //
    // let mut row_writer = RowWriteBuffer::new(path, schema, 10000).unwrap();

    // let mut row_processor = |row: Row| {
    //     if row.get_long(0).unwrap() % REPORT_APPEND_STEP == 0 {
    //         println!("Row with id={}, acc={} and amount={}.", row.get_long(0).unwrap(), row.get_string(1).unwrap(), row.get_int(2).unwrap());
    //     }
    //     row_writer.append_row(row);
    // };
    
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
//                        .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                        .write_batch_with_statistics(&values, None, None, None, None, None) //Some(distinct))
                        .expect("writing String column");
                },
//                _ => panic!("incorrect column number")
            }
            col_nr += 1;
            col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();

}

