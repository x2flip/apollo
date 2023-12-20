use std::ops::IndexMut;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Serialize;
use tiberius::{Config, EncryptionLevel};

extern crate dotenv;
use dotenv::dotenv;
use std::env;

pub fn get_sql_config() -> Config {
    // Load the environmental variables from .env file
    dotenv().ok();

    let db_host = env::var("SQL_HOST").expect("SQL_HOST environment variable must be set");
    let db_port = env::var("SQL_PORT")
        .expect("SQL_PORT environment variable must be set")
        .parse::<u16>()
        .expect("Could not parse SQL_PORT. Please provide a valid, non-negative integer between 0 and 65535.");
    let db_database = env::var("SQL_DB").expect("SQL_DB environment variable must be set");
    let db_username = env::var("SQL_USER").expect("SQL_USER environment variable must be set");
    let db_password = env::var("SQL_PASS").expect("SQL_PASS environment variable must be set");
    //let testpass = "".to_string();

    let mut config = Config::new();
    config.encryption(EncryptionLevel::Off);
    config.authentication(tiberius::AuthMethod::sql_server(db_username, db_password));
    config.trust_cert();
    config.host(db_host);
    config.port(db_port);
    config.database(db_database);
    return config;
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct SQLReturnRow {
    pub id: u32,
    pub requirement: bool,
    pub part_num: String,
    pub due_date: NaiveDate,
    pub sourcefile: String,
    pub qty: Decimal,
    pub net_qty: Decimal,
    pub job_num: String,
    pub asm: i32,
    pub mtl: i32,
    pub order: i32,
    pub order_line: i32,
    pub order_rel: i32,
    pub direct: bool,
}

impl SQLReturnRow {
    pub fn new_on_hand(part_num: &str, qty: Decimal) -> SQLReturnRow {
        SQLReturnRow {
            id: 0,
            requirement: false,
            part_num: part_num.to_string(),
            due_date: NaiveDate::from_ymd(1999, 1, 1),
            sourcefile: "OH".to_owned(),
            qty,
            net_qty: qty,
            job_num: "".to_owned(),
            asm: 0,
            mtl: 0,
            order: 0,
            order_line: 0,
            order_rel: 0,
            direct: false,
        }
    }
}
