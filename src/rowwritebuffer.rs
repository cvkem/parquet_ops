use std::{
    fs,
    io::Write,
    mem,
    path::Path,
    sync::{Arc, mpsc::{self, SyncSender}},
    thread,
};
use parquet::{
    errors::Result,
    record::Row,
    schema::types::Type
};
use crate::rowwriter;

pub struct RowWriteBuffer {
    max_row_group: usize,
    buffer: Vec<Row>,
    schema: Arc<Type>,
    write_sink: SyncSender<Vec<Row>>,
    writer_handle: thread::JoinHandle<()>
}


impl RowWriteBuffer {
    
    pub fn new(path: &Path, schema: Arc<Type>, group_size: usize) -> Result<RowWriteBuffer> {
        let (write_sink, rec_buffer) = mpsc::sync_channel(2);

        let schema_clone = schema.clone();
        let path_clone = path.to_owned();

        let writer_handle = thread::spawn(move || {
            match rowwriter::RowWriter::channel_writer(rec_buffer, &path_clone, schema_clone) {
                Ok(()) => println!("File {path_clone:?} written"),
                Err(err) => println!("Writing file failed with errors {:?}", err)
            }
        });

        let row_writer = RowWriteBuffer {
            max_row_group: group_size,
            buffer: Vec::with_capacity(group_size),
            schema: schema.clone(),
            write_sink,
            writer_handle
        };
    
        Ok(row_writer)
    }

    pub fn remaining_space(&self) -> usize {
        self.max_row_group - self.buffer.len()
    }


    pub fn flush(&mut self) -> Result<()> {
        let rows_to_write = mem::take(&mut self.buffer);

        self.write_sink.send(rows_to_write).unwrap(); // can not use ?  should use match to propagate error.

        Ok(())
    }

    pub fn append_row(&mut self, row: Row) {
        self.buffer.push(row);

        if self.buffer.len() == self.max_row_group {
            self.flush().expect("Failed to flush buffer");
            self.buffer.clear();
        }
    }

    // pub fn write_duration(&self) -> Duration {
    //     self.duration.clone()
    // }

    // Close does consume the writer. 
    // Possibly does this work well when combined with a drop trait?
    pub fn close(mut self)  {
        if self.buffer.len() > 0 {
            if let Err(err) = self.flush() {
                panic!("auto-Flush on close failed with {err}");
            }
        }

        println!("Closing the sending end of the channel.");
        // closing channel will close the writer
        drop(self.write_sink);

        // wait for writer to be ready
        self.writer_handle.join();

    }
}


// // failed to implement drop as it requires and owned value
// impl<W> Drop for RowWriter<W> where 
//     W: Write {
//     fn drop(&mut self) {
//         self.close();
//     }
// }




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
    use crate::RowWriteBuffer;

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