use serde::Serialize;

use crate::parttimephase::Demand;

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct OrderRelease {
    pub order: Option<i32>,
    pub line: Option<i32>,
    pub release: Option<i32>,
    pub part_number: String,
    pub demand: Vec<Demand>,
}
