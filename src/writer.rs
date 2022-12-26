use std::{
    fs,
    io::Write,
    path::Path,
    slice::Iter,
    sync::Arc
};
use parquet::{
    basic::{Compression, ConvertedType, Type as PhysicalType},
    data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
    errors::Result,
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedColumnWriter}
    },
    record::{Row,
        RowAccessor
    },
    schema::types::Type
};
use super::ttypes;





pub struct RowWriter<W: Write>{
    max_row_group: usize,
    row_writer: SerializedFileWriter<W>,
    buffer: Vec<Row>,
    schema: Arc<Type>

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
            schema: schema_clone
        };
    
        Ok(row_writer)
    }

    pub fn remaining_space(&self) -> usize {
        self.max_row_group - self.buffer.len()
    }


    pub fn flush(&mut self) -> Result<()> {
        let mut row_group_writer = self.row_writer.next_row_group().unwrap();

        for (idx, field) in self.schema.get_fields().iter().enumerate() {

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
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
            } else {
                panic!("Could not find a column-writer for column {idx} containing {:#?}", field)
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


    // Close does not consume the writer. 
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


fn write_i64_column(rows: Iter<Row>,  idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
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



fn write_utf8_column(rows: Iter<Row>, idx: usize, col_writer: &mut SerializedColumnWriter) -> Result<()> {
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



// #[cfg(test)]
// pub mod tests {

//     use std::{
//         path::Path,
//         sync::Arc};
//     use parquet::{
//         basic::Compression,
//         data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
//         file::{
//             properties::WriterProperties,
//             writer::SerializedFileWriter,
//             reader::SerializedFileReader,
//             reader::FileReader
//         },
//         record::{Row, 
//             api::{Field, make_row}},
//         schema::{parser::parse_message_type,
//             types::Type}
//     };
//     use crate::RowWriter;


//     #[test]
//     fn test_write_parquet() {
//         const MESSAGE_TYPE: &str = "
//         message schema {
//             REQUIRED INT64 id;
//             REQUIRED BINARY account (UTF8);
//         ";
//         let input_tuples = vec![(1_i64, "Hello"), (2_i64, "World")];

//         let tuple_to_row = |(id, account)|  vec![("id", Field.Long(id)), ("account", Field.Str(account))]; 
//         let input_rows = make_row(input_tuples.into_iter().map(tuple_to_row).collect()); 


//         let path = Path::new("/tmp/test_write_parquet.parquet");
//         let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());

//         let row_writer = RowWriter::new(path, schema, 10).unwrap();

//         for row in input_rows {
//             row_writer.append_row(row);
//         }
//         row_writer.close();

//     }

// }