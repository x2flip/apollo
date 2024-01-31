use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Serialize;

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
    pub requirement: bool,
    pub direct: bool,
    pub part_number: String,
    pub due_date: NaiveDate,
    pub sourcefile: String,
    pub qty: Decimal,
    pub job_num: String,
    pub asm: Option<i32>,
    pub mtl: Option<i32>,
    pub po_num: Option<i32>,
    pub po_line: Option<i32>,
    pub po_rel: Option<i32>,
    pub order: Option<i32>,
    pub order_line: Option<i32>,
    pub order_rel: Option<i32>,
    pub supply: Vec<PartDtl>,
}
