use std::collections::HashSet;

use chrono::NaiveDate;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Mutex;

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct Demand {
    pub part_number: String,
    pub due_date: NaiveDate,
    pub sourcefile: String,
    pub demand_qty: Decimal,
    pub job_num: String,
    pub asm: i32,
    pub mtl: i32,
    pub order: i32,
    pub order_line: i32,
    pub order_rel: i32,
    pub pegged_demand: Decimal,
    pub supply: Vec<Supply>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct Supply {
    pub due_date: NaiveDate,
    pub sourcefile: String,
    pub pegged_qty: Decimal,
    pub job_num: String,
    pub asm: i32,
    pub mtl: i32,
    pub po_num: Option<i32>,
    pub po_line: Option<i32>,
    pub po_rel: Option<i32>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct PartDtl {
    pub part_number: String,
    pub requirement: bool,
    pub direct: bool,
    pub due_date: NaiveDate,
    pub sourcefile: String,
    pub qty: Decimal,
    pub job_num: Option<String>,
    pub asm: Option<i32>,
    pub mtl: Option<i32>,
    pub po_num: Option<i32>,
    pub po_line: Option<i32>,
    pub po_rel: Option<i32>,
    pub order: Option<i32>,
    pub order_line: Option<i32>,
    pub order_rel: Option<i32>,
    pub supply: Vec<PartDtl>,
    pub bom: Vec<PartDtl>,
}

impl PartDtl {
    pub fn new_on_hand(part_number: &str, qty: Decimal) -> PartDtl {
        PartDtl {
            requirement: false,
            part_number: part_number.to_string(),
            due_date: NaiveDate::from_ymd_opt(1999, 1, 1).unwrap(),
            sourcefile: "OH".to_owned(),
            qty,
            job_num: None,
            asm: None,
            mtl: None,
            order: None,
            order_line: None,
            order_rel: None,
            direct: false,
            po_num: None,
            po_line: None,
            po_rel: None,
            supply: vec![],
            bom: vec![],
        }
    }
}

pub struct PartDtlCollection(pub Vec<PartDtl>);

impl PartDtlCollection {
    pub fn get_unique_part_numbers(&self) -> Vec<String> {
        let mut part_numbers: Vec<String> = Vec::new();
        for part_dtl_row in &self.0 {
            let mut does_exist = false;
            let partnum = &part_dtl_row.part_number;
            for part_number in &part_numbers {
                if partnum == part_number {
                    does_exist = true
                }
            }
            if does_exist == false {
                part_numbers.push(partnum.to_string())
            }
        }
        return part_numbers;
    }

    pub fn get_unique_part_numbers_par(&self) -> Vec<String> {
        let seen = Mutex::new(HashSet::new());

        let unique_part_numbers: Vec<String> = self
            .0
            .par_iter()
            .filter_map(|part| {
                let mut seen = seen.lock().unwrap();
                if seen.insert(&part.part_number) {
                    Some(part.part_number.clone())
                } else {
                    None
                }
            })
            .collect();

        unique_part_numbers
    }
}
