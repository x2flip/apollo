use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    onhand::get_parts_on_hand,
    parttimephase::{PartDtl, PartDtlCollection},
    peg_part_dtl::multi_peg_part_dtl,
    sql::{define_query_string, get_sql_config},
    transformtozero::transform_zero_to_none,
};
use async_std::net::TcpStream;
use chrono::NaiveDate;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::io::{Error, ErrorKind};
use tiberius::{Client, Query, Row};

async fn get_db_client() -> Result<Client<TcpStream>, anyhow::Error> {
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let client = Client::connect(config, tcp).await?;

    Ok(client)
}

async fn get_partdtl_rows() -> Result<Vec<Row>, anyhow::Error> {
    // Connect to server
    let mut client = get_db_client().await?;

    // Getting on hand. Removing because it's not being used on this
    // let _on_hand = get_parts_on_hand().await.unwrap();

    // Construct Query
    let query = define_query_string(None);

    let new_query = Query::new(query);

    // Stream Query
    let stream = new_query.query(&mut client).await?;

    // Consume stream
    let rows = stream.into_first_result().await?;

    // Close Client Connection
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    Ok(rows)
}

pub async fn get_new_time_phase_details(
) -> Result<Arc<Mutex<HashMap<String, Vec<PartDtl>>>>, anyhow::Error> {
    let rows = get_partdtl_rows().await?;

    // Transform rows into new data type
    // let result: PartDtlCollection = PartDtlCollection(transform_rows_to_partdtl(rows));
    let result: PartDtlCollection = PartDtlCollection(transform_rows_to_partdtl_par(rows));

    let unique_part_numbers_par_timer_start = Instant::now();
    let unique_part_numbers = result.get_unique_part_numbers_par();
    let unique_part_numbers_par_timer_elapsed = unique_part_numbers_par_timer_start.elapsed();
    println!(
        "Getting unique part numbers in parallel took: {:?}",
        unique_part_numbers_par_timer_elapsed
    );

    let on_hand = get_parts_on_hand().await.unwrap();

    //Peg unique part numbers
    let multi_results = Arc::new(Mutex::new(HashMap::new()));
    unique_part_numbers.par_iter().for_each(|item| {
        let multi_peg_data = multi_peg_part_dtl(&result.0, &on_hand, &item);
        let mut multi_results = multi_results.lock().unwrap();
        multi_results.insert(item.to_owned(), multi_peg_data);
    });

    println!("Returning result");
    Ok(multi_results)
}

fn transform_rows_to_partdtl(rows: Vec<Row>) -> Vec<PartDtl> {
    let transform_timer_start = Instant::now();
    let mut net_qty = dec!(0.0);
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
        let job_num = val.get::<&str, &str>("JobNum").map(|s| s.to_owned());
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
            bom: vec![],
        });
    });
    let transform_elapsed = transform_timer_start.elapsed();
    println!("Transformation took: {:#?}", transform_elapsed);

    result
}

fn transform_rows_to_partdtl_par(rows: Vec<Row>) -> Vec<PartDtl> {
    let transform_timer_start = Instant::now();

    // Parallel transformation of rows to PartDtl
    let result: Vec<PartDtl> = rows
        .par_iter()
        .map(|val| {
            let requirement = val.get::<bool, _>("RequirementFlag").unwrap_or(false);
            let direct = val.get::<bool, _>("StockTrans").unwrap_or(false);
            let sourcefile = val
                .get::<&str, &str>("SourceFile")
                .unwrap_or("ER")
                .to_string();
            let part_number = val
                .get::<&str, &str>("PartNum")
                .unwrap_or("ERROR")
                .to_string();
            let qty = val.get::<Decimal, _>("Quantity").unwrap_or(dec!(0.0));
            let due_date = val
                .get::<NaiveDate, _>("DueDate")
                .unwrap_or(NaiveDate::from_ymd(1999, 1, 1));
            let job_num = val.get::<&str, &str>("JobNum").map(|s| s.to_string());
            let asm = val.get::<i32, _>("AssemblySeq");
            let mtl = val.get::<i32, _>("JobSeq");
            let order = val.get::<i32, _>("OrderNum");
            let order_line = val.get::<i32, _>("OrderLine");
            let order_rel = val.get::<i32, _>("OrderRelNum");
            let po_num = transform_zero_to_none(val.get::<i32, _>("PONum"));
            let po_line = transform_zero_to_none(val.get::<i32, _>("POLine"));
            let po_rel = transform_zero_to_none(val.get::<i32, _>("PORelNum"));

            // Removed net_qty calculation from here as it's not straightforwardly parallelizable

            PartDtl {
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
                bom: vec![],
            }
        })
        .collect();

    let transform_elapsed = transform_timer_start.elapsed();
    println!("Transformation took: {:#?}", transform_elapsed);

    // Sequentially calculate net_qty if necessary, considering redesigning its calculation for parallel execution

    result
}
