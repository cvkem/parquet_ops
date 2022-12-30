
use std::{
    cmp::Ordering,
    fs,
    io::Write,
    mem,
    path::Path,
    slice::Iter,
    sync::{Arc, Mutex, mpsc::Receiver},
    thread,
    time::{self, Instant, Duration}, any::type_name
};
use parquet::{
    basic::{Compression, ConvertedType, Type as PhysicalType},
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    errors::Result,
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedColumnWriter, SerializedRowGroupWriter}
    },
    record::{Row,
        RowAccessor
    },
    schema::types::Type
};

// use memory_stats::memory_stats;

// fn show_memory_usage(prefix: &str) {
//     if let Some(usage) = memory_stats() {
//         println!("{prefix}: Current physical memory usage: {:.1}Mb and virtual usage {:.1}", usage.physical_mem as f64 / 1_000_000 as f64, usage.virtual_mem as f64 /1_000_000 as f64);
//     } else {
//         println!("{prefix}: Couldn't get the current memory usage");
//     }
// }


pub struct RowWriter {
    schema: Arc<Type>,
    row_writer: SerializedFileWriter::<fs::File>
}


impl RowWriter {

    pub fn channel_writer(to_write: Receiver<Vec<Row>>, path: &Path, schema: Arc<Type>) -> Result<()> {

        let mut row_writer = Self::create_writer(path, schema)?;

        let mut total_duration = Duration::new(0, 0);

        let mut idx = 0;
        for rows in to_write {
//            show_memory_usage("before");
            let duration = row_writer.write_row_group(rows)?;
//            show_memory_usage("after");
            total_duration += duration;
            idx += 1;
            println!("rowgroup {idx}: Total-write-duration={total_duration:?}");
        }
        println!("Input-channel has been termined. Now closing down!");

        row_writer.close()?;
        println!(" Total write duration {total_duration:?}");

        Ok(())
    }


    fn create_writer(path: &Path, schema: Arc<Type>) ->  Result<RowWriter> {
        let props = Arc::new(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build());
        let file = fs::File::create(&path).unwrap();
        let schema_clone = schema.clone();
        
        let row_writer = RowWriter {
            row_writer: SerializedFileWriter::<_>::new(file, schema_clone, props).unwrap(),
            schema: schema
        };
        Ok(row_writer)
    }

    fn close(self) -> Result<()> {
        let _filemetadata = self.row_writer.close()?;
        Ok(())
    }



    pub fn write_row_group(&mut self, buffer: Vec<Row>) -> Result<Duration> {

        let timer = Instant::now();

        let mut row_group_writer = self.row_writer.next_row_group().unwrap();

        for (idx, field) in self.schema.get_fields().iter().enumerate() {

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                match field.get_basic_info().converted_type() {
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
                col_writer.close(); 
            } else {
                panic!("Could not find a column-writer for column {idx} containing {:#?}", field)
            }
        }
        row_group_writer.close();

        let elapsed = timer.elapsed();

        println!("Flushing the row-group takes {:?}", elapsed);
        Ok(elapsed)
    }

}


// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}


// implementations of the columns-writers are implemented as private functions.


fn write_i64_column_aux<R>(rows: Iter<Row>, col_writer: &mut SerializedColumnWriter, row_acccessor: R) -> Result<()> where 
    R: FnMut(&Row) -> i64 {
    let column: Vec<i64> = rows
        .map( row_acccessor )
        .collect();
    let the_min = column.iter().min().unwrap();
    let the_max = column.iter().max().unwrap();

    col_writer
        .typed::<Int64Type>()
        .write_batch_with_statistics(&column, None, None, Some(&the_min), Some(&the_max), None)?;
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
