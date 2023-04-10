use std::{ 
    any::type_name,
    fs,
    path::Path, 
};
use parquet::{
    file::{
        reader::SerializedFileReader,
        reader::FileReader, 
        metadata::ParquetMetaData
    },
    schema::types::Type, basic::ConvertedType
};

use crate::parquet_reader::get_parquet_reader;


// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}



const SHOW_FIRST_GROUP_ONLY: bool = true;

fn print_schema(schema: &Type) {
    for (idx, fld) in schema.get_fields().iter().enumerate() {
        let basic_info = fld.get_basic_info();
        let nme = basic_info.name();
        let conversion = {
            let conv_type = basic_info.converted_type();
            if conv_type != ConvertedType::NONE {
                format!("->{}", conv_type)
            } else {
                "".to_owned()
            }
        };
        let phys_type = fld.get_physical_type();
        println!("\t\tidx={} {}: {}{}\n\t\t\tbasic_type={:?}", &idx, &nme, &phys_type, &conversion, basic_info);
    }
}


/// Return a clone of the metadata of a reader at a 
pub fn get_parquet_metadata(path: &str) -> ParquetMetaData {
    // meta-data is the full set of meta-data, which falls apart in:
    //  *  file metadata, which includes:
    //         - the schema
    //         - the total number of rows
    //  *  metadata for each of the row-groups.
    get_parquet_reader(path).metadata()
}



/// Show the metadata on the console.
pub fn show_parquet_metadata(parquet_metadata: &ParquetMetaData) {
    let file_metadata = parquet_metadata.file_metadata();

    println!("\tversion = {}", file_metadata.version());
    println!("\tnum_rows = {}", file_metadata.num_rows());
    println!("\tcreated_by = {:?}", file_metadata.created_by());
    println!("\tkey_value_metadata = {:?}", file_metadata.key_value_metadata());
    println!("\tcolumn = {:?}", file_metadata.column_orders());
    println!("\tschema_descr: (only contains rootschema with name {} and nested the full schema ", file_metadata.schema_descr().root_schema().name());
    print_schema(file_metadata.schema());

    println!("\nNow showing the RowGroups");

    for (idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
        println!("  rowgroup: {} has meta {:#?}", idx, rg);
        if SHOW_FIRST_GROUP_ONLY {
            println!("\nFile contains {} row_groups in total, but only first shown.", parquet_metadata.row_groups().len());
            break;
        }
    }

    // let fields = parquet_metadata.file_metadata().schema().get_fields();
}
