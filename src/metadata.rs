use parquet::{
    basic::ConvertedType,
    file::metadata::ParquetMetaData,
    schema::types::{BasicTypeInfo, Type},
};
use std::{any::type_name, sync::Arc};

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
        println!(
            "\t\tidx={} {}: {}{}\n\t\t\tbasic_type={:?}",
            &idx, &nme, &phys_type, &conversion, basic_info
        );
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
    println!(
        "\tkey_value_metadata = {:?}",
        file_metadata.key_value_metadata()
    );
    println!("\tcolumn = {:?}", file_metadata.column_orders());
    println!(
        "\tschema_descr: (only contains rootschema with name {} and nested the full schema ",
        file_metadata.schema_descr().root_schema().name()
    );
    print_schema(file_metadata.schema());

    println!("\nNow showing the RowGroups");

    for (idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
        println!("  rowgroup: {} has meta {:#?}", idx, rg);
        if SHOW_FIRST_GROUP_ONLY {
            println!(
                "\nFile contains {} row_groups in total, but only first shown.",
                parquet_metadata.row_groups().len()
            );
            break;
        }
    }

    // let fields = parquet_metadata.file_metadata().schema().get_fields();
}

type FFAResType = (usize, Vec<(usize, Arc<Type>)>);
type FFResType = (usize, Arc<Type>);

// Auxiliary function to cleanly handle nested structures.
fn find_field_aux(group: Arc<Type>, field_name: &str, state: FFAResType) -> FFAResType {
    group
        .get_fields()
        .iter()
        .fold(state, |(idx, mut res), tpe| {
            match **tpe {
                Type::GroupType { .. } => {
                    // processing to go one level deeper
                    find_field_aux(Arc::clone(tpe), field_name, (idx, res))
                }
                Type::PrimitiveType { .. } => {
                    if field_name == tpe.get_basic_info().name() {
                        res.push((idx, Arc::clone(tpe)));
                    };
                    (idx + 1, res)
                }
            }
        })
}

// Find a field in the schema based on the names. For nested names we do not parse the full path, but only the name of the leaf.
// In case the field_name occurs multiple times, for example in nested fields or when the field_name does not exist the function panics.
pub fn find_field(schema: Arc<Type>, field_name: &str) -> FFResType {
    let (_, mut results) = find_field_aux(schema, field_name, (0, Vec::new()));

    match results.len() {
        0 => panic!("Failed to find a field with name: '{field_name}'"),
        1 => return results.pop().unwrap(),
        num => panic!("Obtained multiple results {num} for name: '{field_name}'."),
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::find_field;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    #[test]
    fn test_find_field() {
        let msg_type = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED BINARY account (UTF8);
            REQUIRED INT32 amount;
            REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
            REQUIRED BINARY extra_00 (UTF8);} 
        ";
        let schema = Arc::new(parse_message_type(msg_type).unwrap());

        let (idx, tpe) = find_field(schema.clone(), "account");

        println!("the selected type = {tpe:?}");
        assert_eq!(idx, 1);

        let (idx, tpe) = find_field(schema, "datetime");

        println!("the selected type = {tpe:?}");
        assert_eq!(idx, 3);
    }

    #[test]
    fn test_find_field_nested() {
        let msg_type = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED GROUP nested_rec {
                REQUIRED BINARY account (UTF8);
                REQUIRED INT32 amount;
            }
            REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
            REQUIRED BINARY extra_00 (UTF8);} 
        ";
        let schema = Arc::new(parse_message_type(msg_type).unwrap());

        let (idx, tpe) = find_field(Arc::clone(&schema), "account");

        println!("the selected type = {tpe:?}");
        assert_eq!(idx, 1);

        let (idx, tpe) = find_field(schema, "datetime");

        println!("the selected type = {tpe:?}");
        assert_eq!(idx, 3);
    }
}
