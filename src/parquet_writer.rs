

use std::{ 
    fs,
    io,
    path::Path, 
    sync::Arc};
use parquet::{
    basic::Compression,
    file::{
        properties::WriterProperties,
        writer::SerializedFileWriter,
    },
    schema::types::Type
};
use s3_file::S3Writer;


pub enum ParquetWriter {
    FileWriter(SerializedFileWriter<io::BufWriter<fs::File>>),
    S3Writer(SerializedFileWriter<S3Writer>)
}

/// Parse the string and return a ParquetWriter with the corresponding type.
pub fn get_parquet_writer(path: &str, schema: Arc<Type>) -> ParquetWriter {
    // TODO: at this location we are still tightly lined to the test-types (ttypes)
    let props = Arc::new(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build());

    let parts: Vec<&str> = path.split(":").collect();
    match parts.len() {
        1 => { 
            let path = Path::new(path);
            let file = fs::OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(path)
                                .unwrap();
            let buf_file = io::BufWriter::new(file);
            let writer = SerializedFileWriter::new(buf_file, schema, props).unwrap();
            ParquetWriter::FileWriter(writer)
        },
        3 => {
            assert_eq!(parts[0], "s3");
            let block_size = 10_000_000;
            let bucket_name = parts[1].to_string();
            let object_name = parts[2].to_owned();
        
            let file = s3_file::S3Writer::new(bucket_name, object_name, block_size);
            
            let writer = SerializedFileWriter::new(file, schema, props).unwrap();
            ParquetWriter::S3Writer(writer)
        
        },
        _  => panic!("File-path should have no colon (:) or S3-path should have format \"s3:<bucket>:<object_name>\".")
    }
}
