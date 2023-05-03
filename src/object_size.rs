// use parquet::file::{
//     metadata::ParquetMetaData,
//     reader::{FileReader, SerializedFileReader},
// };
use s3_file::S3Reader;
use std::fs;
use async_bridge;



/// Create an iterator over the data of a Parquet-file or Parquet S3 object 
/// If string is prefixed by 'mem:' this will be an in memory buffer, if is is prefixed by 's3:' it will be a s3-object. Otherswise it will be a path on the local file system.
pub fn get_object_size(path: &str) -> u64 {
    match path.split(':').next().unwrap() {
        prefix if path.len() == prefix.len() => fs::metadata(path).expect(&format!("Failed to open file {}", path)).len(),
        "mem" =>panic!("prefix 'mem:'can best be handled via temp-files, or all data should be incoded in the path-string"),
        "s3" => {
            // TODO: this is an inefficient solution as S3_reader also allocates a cache. However, this is the easy solution for now.
            // However,the round-trip so AWS over https probably takes more time (but temporary memory allocation might be issue when low on memory)
            let parts: Vec<&str> = path.split(":").collect();
            assert_eq!(parts.len(), 3, "Path should have format \"s3:<bucket>:<object_name>\".");
            let bucket_name = parts[1].to_string();
            let object_name = parts[2].to_owned();
            let reader = S3Reader::new(bucket_name, object_name, 10_000*1024);
            async_bridge::run_async(reader.get_length()).unwrap()
        }
        prefix => panic!("get_parquet_iter not implemented for prefix {prefix} of path {path}")
        }
}



