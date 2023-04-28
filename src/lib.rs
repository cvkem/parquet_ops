
pub use self::{
    rowiterext::{read_rows, read_rows_stepped, read_row_sample, get_parquet_iter},
    rowiterext::ttest::read_parquet_rowiter,
    merge::{merge_parquet, merge_parquet_fake},
    //legacy_writer::write_parquet,
    testdata_writer::write_parquet,
    metadata::{get_parquet_metadata, show_parquet_metadata},
    sort::{sort, sort_multistage},
    ttypes::{MESSAGE_TYPE, ACCOUNT_ONLY_TYPE, ID_ONLY_TYPE},
    rowwritebuffer::RowWriteBuffer
};

mod parquet_reader;
mod parquet_writer;
mod rowiterext;
mod rowwritebuffer;
mod merge;
mod metadata;
// test stuff
mod ttypes;
//mod test_writer;
mod sort;

// TODO: to be dropped. Still in place for nested types??
//mod legacy_writer;
mod testdata_writer;

const REPORT_APPEND_STEP: i64 = 100; //10_000  // 1 in REPORT_APPEND_STEP rows is reported on the console.

