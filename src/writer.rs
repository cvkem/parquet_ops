use std::{
    fs,
    io::{self, Write},
    path::Path,
    slice::Iter,
    sync::Arc
};
use parquet::{
    basic::{Compression, ConvertedType, Type as PhysicalType},
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedColumnWriter},
        reader::SerializedFileReader
    },
    record::{Row,
        RowAccessor
    },
    schema::{parser::parse_message_type,
        types::Type}, errors::ParquetError
};
use super::ttypes;



pub struct RowWriter<W: Write>{
    max_row_group: usize,
    row_writer: SerializedFileWriter<W>,
    buffer: Vec<Row>,
    schema: Arc<Type>

}

fn write_i64_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<(), ParquetError> {
    let column: Vec<i64> = rows
        .map(|row| row.get_long(idx).unwrap() )
        .collect();
    let the_min = column.iter().min().unwrap();
    let the_max = column.iter().max().unwrap();

    col_writer
        .typed::<Int64Type>()
        .write_batch_with_statistics(&column, None, None, Some(&the_min), Some(&the_max), None)?;
    Ok(())
}



fn write_utf8_column(rows: Iter<Row>, idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<(), ParquetError> {
    let column: Vec<ByteArray> = rows
        .map(|row| row.get_string(idx).unwrap().as_str().into())
        .collect();
//        let the_min = column.iter().min().unwrap();
//        let the_max = column.iter().max().unwrap();

    col_writer
        .typed::<ByteArrayType>()
        .write_batch_with_statistics(&column, None, None, Some(&(column[0])), column.last(), None)?;
    Ok(())
}


impl<W: Write> RowWriter::<W> {
    pub fn new(path: &Path, schema: Arc<Type>, num_recs: u64, group_size: usize) -> Result<RowWriter<fs::File>, io::Error> {
//        let message_type = if NESTED {ttypes::LONG_NESTED_MESSAGE_TYPE} else {ttypes::MESSAGE_TYPE};
//        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build());
        let file = fs::File::create(&path).unwrap();
        let schema_clone = schema.clone();

        let row_writer = RowWriter::<fs::File> {
            row_writer: SerializedFileWriter::<_>::new(file, schema, props).unwrap(),
            max_row_group: group_size,
            buffer: Vec::with_capacity(group_size),
            schema: schema_clone
        };
    
        Ok(row_writer)
    }

    pub fn remaining_space(&self) -> usize {
        self.max_row_group - self.buffer.len()
    }


    pub fn flush(&mut self) -> Result<(), ParquetError> {
        let mut row_group_writer = self.row_writer.next_row_group().unwrap();

        let mut fields = self.schema.get_fields().iter().enumerate();

        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            let (idx, field) = fields.next().unwrap();

            match field.get_basic_info().converted_type() {
                ConvertedType::INT_64 => write_i64_column(self.buffer.iter(), idx, &mut col_writer)?,
                ConvertedType::UTF8 => write_utf8_column(self.buffer.iter(), idx, &mut col_writer)?,
                ConvertedType::TIMESTAMP_MILLIS => {

                },
                // some more types need to be implemented
                ConvertedType::NONE => {
                    match field.get_physical_type() {
                        PhysicalType::INT64 => write_i64_column(self.buffer.iter(), idx, &mut col_writer)?,
                        _ => {
                            panic!("Column {idx}: Unknown Pysical-type {:?}", field.get_physical_type());
                        }
                    }
                },
                // some more types need to be implemented
                _ => panic!("Column {idx}: Unknown Converted-type {:?}", field.get_basic_info().converted_type())
            }

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

}