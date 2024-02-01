use std::{collections::HashMap, time::Instant};

use crate::{
    parttimephase::PartDtl,
    sql::{define_query_string, get_sql_config},
    transformtozero::transform_zero_to_none,
};
use async_std::net::TcpStream;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use tiberius::{Client, Query, Row};

pub async fn get_new_time_phase_details() -> Result<Vec<PartDtl>, anyhow::Error> {
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    println!("Connecting to client");
    let mut client = Client::connect(config, tcp).await?;
    println!("Connected to the client!");

    // Getting on hand. Removing because it's not being used on this
    // let _on_hand = get_parts_on_hand().await.unwrap();

    // Construct Query
    println!("Defining query string");
    let query = define_query_string(None);
    println!("Query string has been defined!");

    // let qry_start = Instant::now();

    println!("Creating query object with the given string");
    let new_query = Query::new(query);
    println!("Query Object has been created!");

    // Stream Query
    println!("Getting Query Stream...");
    let stream = new_query.query(&mut client).await?;
    println!("Query String retreived.");

    // Consume stream
    println!("Retreiving result set into Row objects");
    let rows = stream.into_first_result().await?;
    println!("Row objects are now retreived");

    // Transform rows into new data type
    println!("Transforming the rows into new PartDtl struct");
    let transform_timer = Instant::now();
    let result: Vec<PartDtl> = transform_rows_to_partdtl(rows);
    println!("Transformation is complete!");

    // Close Client Connection
    println!("Closing client connection");
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    println!("Client connection closed!");

    let multi_results = Arc::new(Mutex::new(HashMap::new()));

    let unique_part_numbers = get_unique_part_numbers(&result);

    //Peg unique part numbers
    unique_part_numbers.par_iter().for_each(|item| {
        let multi_peg_data = multi_peg_part_dtl(&result, &on_hand, &item);
        let mut multi_results = multi_results.lock().unwrap();
        multi_results.insert(item.to_owned(), multi_peg_data);
    });

    println!("Returning result");
    Ok(result)
}

fn transform_rows_to_partdtl(rows: Vec<Row>) -> Vec<PartDtl> {
    let mut net_qty = dec!(0.0);
    let mut id = 0;
    let mut result: Vec<PartDtl> = vec![];

    rows.iter().for_each(|val| {
        let requirement = val
            .get::<bool, _>("RequirementFlag")
            .unwrap_or(false.to_owned())
            .to_owned();
        let direct = val
            .get::<bool, _>("StockTrans")
            .unwrap_or(false.to_owned())
            .to_owned();
        let sourcefile = val
            .get::<&str, &str>("SourceFile")
            .unwrap_or("ER")
            .to_owned();
        let part_number = val
            .get::<&str, &str>("PartNum")
            .unwrap_or("ERROR")
            .to_owned();
        let qty = val
            .get::<Decimal, _>("Quantity")
            .unwrap_or(dec!(0.0))
            .to_owned();
        let due_date = val
            .get::<NaiveDate, _>("DueDate")
            .unwrap_or(NaiveDate::from_ymd_opt(1999, 1, 1).unwrap())
            .to_owned();
        let job_num = val.get::<&str, &str>("JobNum").unwrap().to_owned();
        let asm = val.get::<i32, _>("AssemblySeq");
        let mtl = val.get::<i32, _>("JobSeq");
        let order = val.get::<i32, _>("OrderNum");
        let order_line = val.get::<i32, _>("OrderLine");
        let order_rel = val.get::<i32, _>("OrderRelNum");
        let po_num = transform_zero_to_none(val.get::<i32, _>("PONum").to_owned());
        let po_line = transform_zero_to_none(val.get::<i32, _>("POLine").to_owned());
        let po_rel = transform_zero_to_none(val.get::<i32, _>("PORelNum").to_owned());

        if requirement {
            net_qty = net_qty.saturating_sub(qty);
        } else {
            net_qty = net_qty.saturating_add(qty);
        }

        result.push(PartDtl {
            requirement,
            part_number,
            direct,
            due_date,
            sourcefile,
            qty,
            job_num,
            asm,
            mtl,
            po_num,
            po_line,
            po_rel,
            order,
            order_line,
            order_rel,
            supply: vec![],
        });

        id += 1;
    });

    result
}

fn get_unique_part_numbers(part_dtls: &Vec<PartDtl>) -> Vec<String> {
    let mut part_numbers: Vec<String> = Vec::new();
    for part_dtl_row in part_dtls {
        let mut does_exist = false;
        let partnum = &part_dtl_row.part_num;
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
