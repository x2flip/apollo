// This function tries to create a date and panics if it's invalid
use chrono::NaiveDate;
pub fn create_date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("Invalid date")
}
