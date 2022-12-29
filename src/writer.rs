use std::{
    fs,
    io::Write,
    mem,
    path::Path,
    slice::Iter,
    sync::{Arc, Mutex},
    time::{Instant, Duration}, any::type_name
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
use super::ttypes;


enum FlushStatus {
    None, 
    Running, 
    Ready(Result<Duration>)
}


pub struct RowWriter<W: Write>{
    max_row_group: usize,
    row_writer: SerializedFileWriter<W>,
    buffer: Vec<Row>,
    schema: Arc<Type>,
    // internal
    duration: Duration,
    flush_status: Arc<Mutex<FlushStatus>>
}


impl<W: Write> RowWriter::<W> {
    pub fn new(path: &Path, schema: Arc<Type>, group_size: usize) -> Result<RowWriter<fs::File>> {
        let props = Arc::new(WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build());
        let file = fs::File::create(&path).unwrap();
        let schema_clone = schema.clone();

        let row_writer = RowWriter::<fs::File> {
            row_writer: SerializedFileWriter::<_>::new(file, schema, props).unwrap(),
            max_row_group: group_size,
            buffer: Vec::with_capacity(group_size),
            schema: schema_clone,
            duration: Duration::new(0, 0),
            flush_status: Arc::new(Mutex::new(FlushStatus::None))
        };
    
        Ok(row_writer)
    }

    pub fn remaining_space(&self) -> usize {
        self.max_row_group - self.buffer.len()
    }


    pub fn flush_aux(schema: Arc<Type>, buffer: Vec<Row>, mut row_group_writer: SerializedRowGroupWriter<W>) -> Result<Duration> {

        let timer = Instant::now();

        for (idx, field) in schema.get_fields().iter().enumerate() {

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


    pub fn flush(&mut self) -> Result<()> {
        let mut row_group_writer = self.row_writer.next_row_group().unwrap();
        let rows_to_write = mem::take(&mut self.buffer);
        
        match Self::flush_aux(self.schema.clone(), rows_to_write, row_group_writer) {
            Ok(duration) => self.duration += duration,
            Err(err) => return Err(err)
        }
        Ok(())
    }

    pub fn append_row(&mut self, row: Row) {
        self.buffer.push(row);

        if self.buffer.len() == self.max_row_group {
            self.flush().expect("Failed to flush buffer");
            self.buffer.clear();
        }
    }

    pub fn write_duration(&self) -> Duration {
        self.duration.clone()
    }

    // Close does consume the writer. 
    // Possibly does this work well when combined with a drop trait?
    pub fn close(mut self)  {
        if self.buffer.len() > 0 {
            if let Err(err) = self.flush() {
                panic!("auto-Flush on close failed with {err}");
            }
        }
        self.row_writer.close();
    }
}


// // failed to implement drop as it requires and owned value
// impl<W> Drop for RowWriter<W> where 
//     W: Write {
//     fn drop(&mut self) {
//         self.close();
//     }
// }

// implementations of the columns-writers are implemented as private functions.

// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

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



// fn write_i64_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
//     let first_row = rows.clone().next();

//     println!("column {idx} has values {:?} of type {}", &first_row, type_of(&first_row));
//     println!(" Full row: {:#?}", first_row);

//     let column: Vec<i64> = rows
//         .map(|row| row.get_long(idx).unwrap() )
//         .collect();
//     let the_min = column.iter().min().unwrap();
//     let the_max = column.iter().max().unwrap();

//     col_writer
//         .typed::<Int64Type>()
//         .write_batch_with_statistics(&column, None, None, Some(&the_min), Some(&the_max), None)?;
//     Ok(())
// }

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


use std::cmp::Ordering;

fn write_utf8_column(rows: Iter<Row>, idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
    let column: Vec<ByteArray> = rows
        .map(|row| row.get_string(idx).unwrap().as_str().into())
        .collect();
// //        let the_min = column.iter().min().unwrap();
// //        let the_max = column.iter().max().unwrap();

//     col_writer
//         .typed::<ByteArrayType>()
//         .write_batch_with_statistics(&column, None, None, Some(&(column[0])), column.last(), None)?;
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



#[cfg(test)]
pub mod tests {

    use std::{
        fs::File,
        path::Path,
        sync::Arc};
    use parquet::{
        basic::Compression,
        data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
        file::{
            properties::WriterProperties,
            writer::{
                SerializedFileWriter,
                SerializedRowGroupWriter},
            reader::{
                SerializedFileReader,
                FileReader}
        },
        record::{Row, Field},
//            api::{Field, make_row}},
        schema::{parser::parse_message_type,
            types::Type}
    };
    use parquet_derive::ParquetRecordWriter;
    use crate::RowWriter;

    //#[derive(Debug)]
    // I expect some kind of feature of arrow-parquet is needed to make this work.
    #[derive(ParquetRecordWriter)]
    struct TestAccount<'a> {
        pub id: i64,
        pub account: &'a str
    }


    // this is not the right test as I switch to example code
    #[test]
    fn test_write_parquet() {
        const MESSAGE_TYPE: &str = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED BINARY account (UTF8);
        ";
        // let input_tuples = vec![(1_i64, "Hello"), (2_i64, "World")];

        // let tuple_to_row = |(id, account)|  vec![("id", Field.Long(id)), ("account", Field.Str(account))]; 
        // let input_rows = make_row(input_tuples.into_iter().map(tuple_to_row).collect()); 
        let test_account_data = vec![
            TestAccount {
                id: 1,
                account: "Hello"
            },
            TestAccount {
                id: 2,
                account: "World"
            }
        ];
        println!("Original data: {:#?}", test_account_data);

        let path = Path::new("/tmp/test_write_parquet.parquet");
        let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
        let generated_schema = test_account_data.as_slice().schema().unwrap();

        let props = Arc::new(WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build());
//        let row_writer = RowWriter::new(path, schema, 10).unwrap();
        let file = fs::File::create(&path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, generated_schema, props).unwrap();


        println!(" row_data= {:#?}", generated_schema);

        // for row in row_data {
        //     row_writer.append_row(row);
        // }
        let mut row_group = row_writer.next_row_group().unwrap();
        test_account_data.as_slice().write_to_row_group(&mut row_group).unwrap();
        row_group.close().unwrap();


        row_writer.close().unwrap();

    }

}