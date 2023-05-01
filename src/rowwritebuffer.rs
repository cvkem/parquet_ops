use async_bridge;
use parquet::{
    errors::{ParquetError, Result},
    record::{Field, Row},
    schema::types::Type,
};
use std::{
    error::Error,
    fs,
    io::{BufWriter, Write},
    mem,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
};

mod rowwriter;

const CHANNEL_SIZE: usize = 2;

pub struct RowWriteBuffer {
    max_row_group: usize,
    buffer: Vec<Row>,
    write_sink: Option<SyncSender<Vec<Row>>>,
    writer_handle: Option<tokio::task::JoinHandle<()>>, // thread::JoinHandle<()>
}

impl RowWriteBuffer {
    pub fn new(path: &str, schema: Arc<Type>, group_size: usize) -> Result<RowWriteBuffer> {
        let (write_sink, rec_buffer) = mpsc::sync_channel(CHANNEL_SIZE);

        let path_clone = path.to_owned();

        let writer_handle = async_bridge::spawn_async(async move {
            //} || {

            // here a channel-writer is started and will run until the rec_buffer is closed by all senders (typically one sender)
            // each vector of rows received over the channel via the rec_buffer is writen as a separate row_group.
            match rowwriter::RowWriter::channel_writer(rec_buffer, &path_clone, schema) {
                Ok(()) => (),
                Err(err) => println!("Writing file '{path_clone:?}'failed with errors {:?}", err),
            }
        });

        let row_writer = RowWriteBuffer {
            max_row_group: group_size,
            buffer: Vec::with_capacity(group_size),
            write_sink: Some(write_sink),
            writer_handle: Some(writer_handle),
        };

        Ok(row_writer)
    }

    pub fn remaining_space(&self) -> usize {
        self.max_row_group - self.buffer.len()
    }

    pub fn flush(&mut self) -> Result<()> {
        let rows_to_write = mem::take(&mut self.buffer);

        match self
            .write_sink
            .as_ref()
            .expect("Write_sink should still exist (but None)")
            .send(rows_to_write)
        {
            Ok(()) => Ok(()),
            Err(err) => {
                println!("ERROR during flush (sending to write_sink):");
                println!("   Source: {:?}", err.source());

                Err(ParquetError::General(format!(
                    "Error during flush: {err:#?}"
                )))
            }
        }
    }

    pub fn append_row(&mut self, row: Row) {
        self.buffer.push(row);

        if self.buffer.len() == self.max_row_group {
            self.flush().expect("Failed to flush buffer");
            self.buffer.clear(); // flush already replaces with an empty buffer, so this is not needed.
        }
    }

    // write a complete row_group to the write-sink. Assumes the current buffer is empty (no pushed rows)
    pub fn append_row_group(&mut self, rowgroup_data: Vec<Row>) {
        if rowgroup_data.len() == 0 {
            println!("Received an empty row-group. Ignoring it");
            return;
        }
        let old_buffer = mem::replace(&mut self.buffer, rowgroup_data);

        if old_buffer.len() > 0 {
            panic!(
                "The buffer already contains {} rows. Should be empty when adding a row-group",
                old_buffer.len()
            );
        };

        self.flush().expect("Failed to flush buffer");
        //self.buffer.clear();  // flush already replaces with an empty buffer, so this is not needed.
    }

    // pub fn write_duration(&self) -> Duration {
    //     self.duration.clone()
    // }

    // Close does consume the writer.
    // Possibly does this work well when combined with a drop trait?
    pub fn close(&mut self) {
        if self.buffer.len() > 0 {
            if let Err(err) = self.flush() {
                panic!("auto-Flush on close failed with {err}");
            }
        }

        println!("Closing the sending end of the channel.");
        // closing channel will close the writer
        drop(
            self.write_sink
                .take()
                .expect("Write_sink should still exist (but None)"),
        );

        // wait for writer to be ready
        //        self.writer_handle.join().unwrap();
        async_bridge::run_async(async {
            let wh = self.writer_handle.take().unwrap();
            match wh.await {
                Err(err) => eprintln!("Error while closing merged file: {err:?}"),
                Ok(_) => (),
            }
        })
    }
}

impl Drop for RowWriteBuffer {
    fn drop(&mut self) {
        match &self.write_sink {
            Some(ws) => println!("Write-sink exists"),
            None => println!("No write-sink (None)"),
        };
        match &self.writer_handle {
            Some(wh) => println!("Writer_handle exists"),
            None => println!("No writer_handle (None)"),
        };
    }
}

/// Create a writer based on a string that implements the std::io::Write interface.
/// If string is prefixed by 'mem:' this will be an in memory buffer, if is is prefixed by 's3:' it will be a s3-object. Otherswise it will be a path on the local file system.
fn create_writer(path: &str) -> Box<dyn Write> {
    let writer: Box<dyn Write> = match path.split(':').next().unwrap() {
        prefix if prefix.len() == path.len() => {
            let file = fs::OpenOptions::new()
                //                    .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap();
            Box::new(BufWriter::new(file))
        }
        "mem" => Box::new(Vec::new()),
        //        "s3" => println!("{s}: S3"),
        prefix => panic!("Unknown prefix '{prefix}' on file {path}"),
    };
    writer
}

/// Creates a frow from a series of tuples. This function is based on parquet::record::api::make_row, which is a private function.
/// A transmute is used to be able to create the rows here. This is a safe step as both parquet::record::Row and RowImitation have the same
/// definition, both are compiled with the same compiler, and a struct with only 1 field allows for only a single logical layout.
pub fn create_row(fields: Vec<(String, Field)>) -> Row {
    pub struct RowImitation {
        fields: Vec<(String, Field)>,
    }
    let row_contents = RowImitation { fields };
    unsafe { mem::transmute(row_contents) }
}

#[cfg(test)]
pub mod tests {

    use crate::rowiterext;
    use crate::rowwritebuffer;
    use parquet::{
        // basic::Compression,
        // data_type::{Int32Type, Int64Type, ByteArrayType, ByteArray},
        // file::{
        //    properties::WriterProperties,
        //     writer::{
        //         SerializedFileWriter,
        //         SerializedRowGroupWriter},
        //     reader::{
        //         SerializedFileReader,
        //         FileReader}
        // },
        record::{Field, Row, RowAccessor},
        schema::parser::parse_message_type,
        //    types::Type}
    };
    use std::sync::Arc;

    // this is not the right test as I switch to example code
    #[test]
    fn test_write_parquet() {
        const MESSAGE_TYPE: &str = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED BINARY account (UTF8);
        ";
        let input_tuples = vec![
            (1_i64, "Hello".to_owned()),
            (2_i64, "World".to_owned()),
            (3_i64, "This is a test!".to_owned()),
        ];

        let tuple_to_row = |(id, account)| {
            rowwritebuffer::create_row(vec![
                ("id".to_owned(), Field::Long(id)),
                ("account".to_owned(), Field::Str(account)),
            ])
        };
        let input_rows: Vec<Row> = input_tuples.clone().into_iter().map(tuple_to_row).collect();

        let path = "/tmp/test_write_parquet.parquet";
        //        let path = "test_write_parquet.parquet";
        let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());

        let mut row_writer = rowwritebuffer::RowWriteBuffer::new(path, schema, 10_000).unwrap();

        for row in input_rows.into_iter() {
            row_writer.append_row(row);
        }

        row_writer.close();

        println!("Now open the file {path} and read it again");
        let result = rowiterext::read_rows(path, Some(10), MESSAGE_TYPE);

        println!("Result of read: {}", result[0]);
        let output_tuples: Vec<(i64, String)> = result
            .iter()
            .map(|row| {
                (
                    row.get_long(0).unwrap(),
                    row.get_string(1).unwrap().to_owned(),
                )
            })
            .collect();
        assert_eq!(input_tuples, output_tuples)
    }
}
