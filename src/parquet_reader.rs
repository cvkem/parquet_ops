use parquet::file::{
    metadata::ParquetMetaData,
    reader::{FileReader, SerializedFileReader},
};
use s3_file::S3Reader;
use std::fs::File;

pub enum ParquetReaderEnum {
    File(SerializedFileReader<File>),
    S3(SerializedFileReader<S3Reader>),
}

impl ParquetReaderEnum {
    pub fn metadata(&self) -> ParquetMetaData {
        match self {
            Self::File(reader) => reader.metadata(),
            Self::S3(reader) => reader.metadata(),
        }
        .clone()
    }

    pub fn num_rows(&self) -> i64 {
        self.metadata().file_metadata().num_rows()
    }
}

/// create an iterator over the data of a Parquet-file.
/// If string is prefixed by 'mem:' this will be an in memory buffer, if is is prefixed by 's3:' it will be a s3-object. Otherswise it will be a path on the local file system.
pub fn get_parquet_reader<'a>(path: &'a str) -> ParquetReaderEnum {
    // we differentiate at this level for the different types of inputs as lower levels can not handle this more generic
    // as the Associated types are in the way on ChunkReader, and also on SerializedFileReader as it wants to see the generic.
    // handling it at this level introduces some source-code-duplication, but that is manageable.
    match path.split(':').next().unwrap() {
        prefix if path.len() == prefix.len() => {
                let reader = SerializedFileReader::try_from(path.to_owned()).unwrap();
                ParquetReaderEnum::File(reader)
            }
        "mem" =>panic!("prefix 'mem:'can best be handled via temp-files, or all data should be incoded in the path-string"),
        "s3" => {
            let parts: Vec<&str> = path.split(":").collect();
            assert_eq!(parts.len(), 3, "Path should have format \"s3:<bucket>:<object_name>\".");
            let bucket_name = parts[1].to_string();
            let object_name = parts[2].to_owned();
            let chunk_reader = S3Reader::new(bucket_name, object_name, 10_000*1024);

            let reader = SerializedFileReader::new(chunk_reader).unwrap();
            ParquetReaderEnum::S3(reader)        
        }
        prefix => panic!("get_parquet_iter not implemented for prefix {prefix} of path {path}")
        }
}
