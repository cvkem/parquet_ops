use crate::find_field;
use parquet::{
    basic::Type as PhysType,
    record::{Row, RowAccessor},
    schema::types::Type,
};
use std::{cmp::Ordering, sync::Arc};

pub trait sort_multistage_typed {
    fn get_partition_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering>;
    fn get_record_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering>;
    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool>;
    fn get_partition_message_schema(&self) -> String;
}

pub struct ParquetKey {
    name: String,
    sort_col: usize,
    phys_type: PhysType,
}

impl ParquetKey {
    pub fn new(name: String, schema: Arc<Type>) -> Self {
        let (sort_col, tpe) = find_field(schema, &name);
        let phys_type = tpe.get_physical_type();

        Self {
            name,
            sort_col,
            phys_type,
        }
    }
}

impl sort_multistage_typed for ParquetKey {
    fn get_partition_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering> {
        match self.phys_type {
            PhysType::INT64 => Box::new(|left: &Row, right: &Row| {
                left.get_long(0).unwrap().cmp(&right.get_long(0).unwrap())
            }),
            PhysType::INT32 => Box::new(|left: &Row, right: &Row| {
                left.get_int(0).unwrap().cmp(&right.get_int(0).unwrap())
            }),
            other => panic!("columns of type '{other}' are not supported (yet)!"),
        }
    }

    fn get_record_compare_fn(&self) -> Box<dyn Fn(&Row, &Row) -> Ordering> {
        let col = self.sort_col;
        match self.phys_type {
            PhysType::INT64 => Box::new(move |left: &Row, right: &Row| {
                left.get_long(col)
                    .unwrap()
                    .cmp(&right.get_long(col).unwrap())
            }),
            PhysType::INT32 => Box::new(move |left: &Row, right: &Row| {
                left.get_int(col).unwrap().cmp(&right.get_int(col).unwrap())
            }),
            other => panic!("columns of type '{other}' are not supported (yet)!"),
        }
    }

    fn get_partition_filter_fn(&self, partition_row: &Row) -> Box<dyn Fn(&Row) -> bool> {
        match self.phys_type {
            PhysType::INT64 => {
                let col = self.sort_col;
                let upper_bound = partition_row.get_long(col).unwrap();
                Box::new(move |row: &Row| row.get_long(col).unwrap() <= upper_bound)
            }
            PhysType::INT32 => {
                let col = self.sort_col;
                let upper_bound = partition_row.get_int(col).unwrap();
                Box::new(move |row: &Row| row.get_int(col).unwrap() <= upper_bound)
            }
            other => panic!("columns of type '{other}' are not supported (yet)!"),
        }
    }

    fn get_partition_message_schema(&self) -> String {
        let type_label = match self.phys_type {
            PhysType::INT64 => "INT64",
            PhysType::INT32 => "INT32",
            other => panic!("columns of type '{other}' are not supported (yet)!"),
        };

        format!(
            "
        message schema {{
          REQUIRED {type_label} {};
        }}",
            self.name
        )
    }
}
