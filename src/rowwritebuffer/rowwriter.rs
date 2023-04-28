
use std::{
    cmp::Ordering,
    fs,
    io,
    slice::Iter,
    sync::{Arc, mpsc::Receiver},
    time::{Instant, Duration}
};
use parquet::{
    basic::{Compression, ConvertedType, Type as PhysicalType},
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    errors::Result,
    file::{
        metadata::RowGroupMetaData,
        writer::{
            SerializedRowGroupWriter,
            SerializedColumnWriter}
    },
    record::{Row,
        RowAccessor
    },
    schema::types::Type
};
use s3_file::S3Writer;
use crate::parquet_writer::{self, ParquetWriter};

// use memory_stats::memory_stats;

// fn show_memory_usage(prefix: &str) {
//     if let Some(usage) = memory_stats() {
//         println!("{prefix}: Current physical memory usage: {:.1}Mb and virtual usage {:.1}", usage.physical_mem as f64 / 1_000_000 as f64, usage.virtual_mem as f64 /1_000_000 as f64);
//     } else {
//         println!("{prefix}: Couldn't get the current memory usage");
//     }
// }


enum RowGroupWriter<'a> {
    File(SerializedRowGroupWriter<'a, io::BufWriter<fs::File>>),
    S3(SerializedRowGroupWriter<'a, S3Writer>)
}

// to-check: weird bound suggested by compiler <'a: 'b, ....>
impl<'a> RowGroupWriter<'a> {
    fn next_column(&mut self) -> Option<SerializedColumnWriter<'_>> {
        match self {
            RowGroupWriter::File(rgw) => rgw.next_column().unwrap(),
            RowGroupWriter::S3(rgw) => rgw.next_column().unwrap()
        }
    }

//     // to-check: weird bound suggested by compiler <'a: 'b, ....>
// impl<'a> RowGroupWriter<'a> {
//     fn next_column<'b: 'a>(&'a mut self) -> Option<SerializedColumnWriter<'b>> {
//         match self {
//             RowGroupWriter::File(rgw) => rgw.next_column().unwrap(),
//             RowGroupWriter::S3(rgw) => rgw.next_column().unwrap()
//         }
//     }

    fn close(self) -> Result<Arc<RowGroupMetaData>>{
        match self {
            RowGroupWriter::File(rgw) => rgw.close(),
            RowGroupWriter::S3(rgw) => rgw.close()
        }
    }
}

pub struct RowWriter {
    schema: Arc<Type>,
    parquet_writer: ParquetWriter
}


impl RowWriter {

    /// create a row-writer and attach to the channel. The row-writer will be closed when the sender closes the channel.
    pub fn channel_writer(to_write: Receiver<Vec<Row>>, path: &str, schema: Arc<Type>) -> Result<()> {

        let mut row_writer = Self::create_writer(path, schema)?;

        let mut total_duration = Duration::new(0, 0);

        let mut idx = 0;
        for rows in to_write.iter() {
            let duration = row_writer.write_row_group(rows)?;
            total_duration += duration;
            idx += 1;
            println!("rowgroup {idx}: Total-write-duration={total_duration:?}");
        }

        match row_writer.parquet_writer {
            ParquetWriter::FileWriter(writer) => writer.close().unwrap(),
            ParquetWriter::S3Writer(writer) => {
                writer.close().unwrap()
            }
        };
    
        println!(" Total write duration {total_duration:?}");

        Ok(())
    }


    fn create_writer(path: &str, schema: Arc<Type>) ->  Result<RowWriter> {
        
        let schema_clone = Arc::clone(&schema);
        let parquet_writer = parquet_writer::get_parquet_writer(path, schema_clone);
        
        let row_writer = RowWriter {
            parquet_writer,
            schema
        };
        Ok(row_writer)
    }

    // fn close(self) -> Result<()> {
    //     let _filemetadata = self.parquet_writer.close()?;
    //     Ok(())
    // }



    fn write_row_group(&mut self, buffer: Vec<Row>) -> Result<Duration> {

        let timer = Instant::now();

        let mut row_group_writer =  match &mut self.parquet_writer {
            ParquetWriter::FileWriter(ref mut writer) => RowGroupWriter::File(writer.next_row_group().unwrap()),
            ParquetWriter::S3Writer(ref mut writer) => RowGroupWriter::S3(writer.next_row_group().unwrap())
        };

        for (idx, field) in self.schema.get_fields().iter().enumerate() {
    {
            if let Some(mut col_writer) = row_group_writer.next_column() {
                match field.get_basic_info().converted_type() {
                    // TODO: Add the Decimal type (and a few others)
                    ConvertedType::INT_64 => write_i64_column(buffer.iter(), idx, &mut col_writer)?,
                    ConvertedType::UINT_64 => write_u64_column(buffer.iter(), idx, &mut col_writer)?,
                    ConvertedType::INT_32 => write_i32_column(buffer.iter(), idx, &mut col_writer)?,
                    ConvertedType::UTF8 => write_utf8_column(buffer.iter(), idx, &mut col_writer)?,
                    ConvertedType::TIMESTAMP_MILLIS => write_ts_millis_column(buffer.iter(), idx, &mut col_writer)?,  // write the raw type
                    // some more types need to be implemented
                    ConvertedType::NONE => {
                        match field.get_physical_type() {
                            PhysicalType::INT64 => write_i64_column(buffer.iter(), idx, &mut col_writer)?,
                            PhysicalType::INT32 => write_i32_column(buffer.iter(), idx, &mut col_writer)?,
                            _ => {
                                panic!("Column {idx}: Unknown Pysical-type {:?}", field.get_physical_type());
                            }
                        }
                    },
                    // some more types need to be implemented
                    _ => panic!("Column {idx}: Unknown Converted-type {:?}", field.get_basic_info().converted_type())
                }
                // ensure the col_writer is closed, however, end of block possibly does close it automatic.
                col_writer.close()?; 
            } else {
                panic!("Could not find a column-writer for column {idx} containing {:#?}", field)
            }
        }
        }
        row_group_writer.close()?;

        let elapsed = timer.elapsed();

        println!("Flushing the row-group takes {:?}", elapsed);
        Ok(elapsed)
    }

}




// implementations of the columns-writers are implemented as private functions.


fn write_i64_column_aux<R>(rows: Iter<Row>, col_writer: &mut SerializedColumnWriter, row_acccessor: R) -> Result<()> where 
    R: FnMut(&Row) -> i64 {
    let column: Vec<i64> = rows
        .map( row_acccessor )
        .collect();
    // let the_min = column.iter().min().unwrap();
    // let the_max = column.iter().max().unwrap();

    // col_writer
    //     .typed::<Int64Type>()
    //     .write_batch_with_statistics(&column, None, None, Some(&the_min), Some(&the_max), None)?;
    let the_min = column.iter().min();
    let the_max = column.iter().max();

    col_writer
        .typed::<Int64Type>()
        .write_batch_with_statistics(&column, None, None, the_min, the_max, None)?;
    Ok(())
}


fn write_i64_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
    write_i64_column_aux(rows, col_writer, |row: &Row| row.get_long(idx).unwrap())
}


fn write_u64_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
    write_i64_column_aux(rows, col_writer, |row: &Row| (row.get_ulong(idx).unwrap() as i64))
}


fn write_ts_millis_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
    write_i64_column_aux(rows, col_writer, |row: &Row| (row.get_timestamp_millis(idx).unwrap() as i64))
}


fn write_i32_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
    let column: Vec<i32> = rows
        .map(|row| row.get_int(idx).unwrap() )
        .collect();
    let the_min = column.iter().min().unwrap();
    let the_max = column.iter().max().unwrap();

    col_writer
        .typed::<Int32Type>()
        .write_batch_with_statistics(&column, None, None, Some(&the_min), Some(&the_max), None)?;
    Ok(())
}



fn write_utf8_column(rows: Iter<Row>, idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
    let column: Vec<ByteArray> = rows
        .map(|row| row.get_string(idx).unwrap().as_str().into())
        .collect();
    let the_min = column.iter().reduce(|a, b| {
            match a.partial_cmp(b) {
                    Some(Ordering::Equal) => a,
                    Some(Ordering::Greater) => b,
                    Some(Ordering::Less) => a,
                    None => a
                }
        }).unwrap();
        let the_max = column.iter().reduce(|a, b| {
            match a.partial_cmp(b) {
                    Some(Ordering::Equal) => a,
                    Some(Ordering::Greater) => a,
                    Some(Ordering::Less) => b,
                    None => a
                }
        }).unwrap();

    col_writer
        .typed::<ByteArrayType>()
        .write_batch_with_statistics(&column, None, None, Some(the_min), Some(the_max), None)?;
    Ok(())
}
