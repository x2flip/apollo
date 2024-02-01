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
    pub po_num: Option<i32>,
    pub po_line: Option<i32>,
    pub po_rel: Option<i32>,
    pub direct: bool,
}

impl SQLReturnRow {
    pub fn new_on_hand(part_num: &str, qty: Decimal) -> SQLReturnRow {
        SQLReturnRow {
            id: 0,
            requirement: false,
            part_num: part_num.to_string(),
            due_date: NaiveDate::from_ymd_opt(1999, 1, 1).unwrap(),
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
            po_num: None,
            po_line: None,
            po_rel: None,
        }
    }
}

pub fn define_query_string(part_numbers: Option<Vec<String>>) -> String {

    // Initialize the query
    let mut new_query = 
        "
            SELECT
                PD.RequirementFlag,
                PD.PartNum,
                PD.SourceFile,
                PD.Type,
                PD.DueDate,
                PD.Quantity,
                PD.JobNum,
                PD.AssemblySeq,
                PD.JobSeq,
                PD.OrderNum,
                PD.OrderLine,
                PD.OrderRelNum,
                PD.PONum,
                PD.POLine,
                PD.PORelNum,
                PD.StockTrans
            FROM 
                Erp.PartDtl as PD
            LEFT OUTER JOIN Erp.Part as PART on 
                PART.Company = PD.Company
                and PART.PartNum = PD.PartNum
                and (not PART.ProdCode = 'ETO' and not PART.ProdCode = 'RMA' and not PART.ProdCode = 'SAMPLE' and not PART.ProdCode = 'TOOL')
            WHERE 
                PD.Type <> 'Sub'
                and PD.Plant = 'MfgSys'
                and PD.Company = 'AE'
                ".to_string();

    // If a vector of part numbers is passed, then we will want to filter on 
    // those in the query
    //
    match part_numbers {
        Some(parts) => {
            let mut filter_text = "and PD.PartNum IN (".to_string();

            parts.iter().enumerate().for_each(|(i, _)| {
                let next = parts.get(i + 1);
                match next {
                    Some(_) => new_query.push_str(&format!("@P{}, ", i + 1)),
                    None => new_query.push_str(&format!("@P{}", i + 1))
                }
            });

            filter_text.push(')');

            filter_text
        },
        None => "".to_string()
    };


    //
    // Add the final Order By details
    new_query.push_str("
            ORDER BY 
                PD.PartNum,
                PD.DueDate,
                PD.RequirementFlag
        ");

    new_query

}
