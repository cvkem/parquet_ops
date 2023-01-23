// This module is used to share filenames between multiple examples, such that these are aligned.

pub const PATH_1: &str = "./sample_even.parquet";
pub const PATH_2: &str = "./sample_odd.parquet";
pub const MERGED: &str = "./merged.parquet";

pub const PATH_1_S3: &str = "s3:parquet-exp:sample_even.parquet";
pub const PATH_2_S3: &str = "s3:parquet-exp:sample_odd.parquet";
pub const MERGED_S3: &str = "s3:parquet-exp:merged.parquet";



pub fn main() {
    panic!("This is shared code for all examples, and is not intended to run as an independent example.")
}
