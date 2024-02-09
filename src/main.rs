extern crate chrono;

mod baq;
mod directlinks;
mod jobmtl;
mod onhand;
mod parttimephase;
mod sql;
mod getdata;
mod transformtozero;
mod peg;
mod orderrelease;
mod backlog;
mod peg_part_dtl;

use actix_cors::Cors;
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use async_std::net::TcpStream;
use chrono::NaiveDate;
use onhand::OnHand;
use parttimephase::{Demand, Supply, PartDtl};
use rayon::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::vec::Vec;
use tiberius::{Client, Query};

use crate::backlog::get_backlog_result;
use crate::directlinks::get_make_direct_jobs;
use crate::getdata::get_new_time_phase_details;
use crate::jobmtl::{get_job_bom, get_job_boms, get_all_job_boms};
use crate::onhand::get_parts_on_hand;
use crate::sql::{get_sql_config, SQLReturnRow};
use crate::transformtozero::transform_zero_to_none;

impl Responder for SQLReturnRow {
    type Body = BoxBody;

    fn respond_to(self, _req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();

        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            // .service(index)
            .service(job)
            .service(jobs)
            .service(get_order)
            .service(all)
            .service(all_new)
            .service(get_backlog)
    })
    .bind(("0.0.0.0", 8081))?
    .run()
    .await
}

#[get("/all/all")]
async fn all() -> impl Responder {
    let data = get_all_time_phase_data().await.unwrap();

    let res = Arc::try_unwrap(data).expect("").into_inner().expect("");

    let response = HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&res).unwrap());

    // Get the data
    // Filter the data by job_num
    //      In order to do this, we need to get the entire BOM for the job. Need JobMtl table
    //      Then filter entire list of data for each part on the BOM. Only return the result sets
    //      where the Demand is for the related job

    response
}

#[get("/all/new")]
async fn all_new() -> impl Responder {
    let data = get_new_time_phase_details().await.unwrap();


    // let response = HttpResponse::Ok()
    //     .content_type("application/json")
    //     .body(serde_json::to_string(&data).unwrap());
    //
    let hashmap_lock = data.lock().unwrap();
    let hashmap = hashmap_lock.clone();
    drop(hashmap_lock);

    let single_part = hashmap.get("853-305102-005(B)").unwrap();

    let response = match serde_json::to_string(&single_part) {
        Ok(json_string) => HttpResponse::Ok().content_type("application/json").body(json_string),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error serializing data: {}", e)),
    };

    // Get the data
    // Filter the data by job_num
    //      In order to do this, we need to get the entire BOM for the job. Need JobMtl table
    //      Then filter entire list of data for each part on the BOM. Only return the result sets
    //      where the Demand is for the related job

    response
}

#[get("/{part}")]
async fn index(path: web::Path<String>) -> impl Responder {
    let part_num: String = path.into_inner();
    let data = get_time_phase_data(None).await.unwrap();

    let response = match Arc::try_unwrap(data)
        .expect("Lock still has multiple owners")
        .into_inner()
        .expect("Mutex cannot be locked")
        .get(&part_num)
    {
        Some(res) => HttpResponse::Ok()
            .content_type("application/json")
            .body(serde_json::to_string(&res).unwrap()),
        None => HttpResponse::UnavailableForLegalReasons().finish(),
    };

    // Get the data
    // Filter the data by job_num
    //      In order to do this, we need to get the entire BOM for the job. Need JobMtl table
    //      Then filter entire list of data for each part on the BOM. Only return the result sets
    //      where the Demand is for the related job

    response
}

#[get("order/{orderlinerel}")]
async fn get_order(path: web::Path<String>) -> impl Responder {
    let orderlinerel: String = path.into_inner();
    let mut spl = orderlinerel.split("-");
    let order_num = spl.next();
    let _order_line = spl.next();
    let _order_rel = spl.next();

    let _time_phase_data = get_time_phase_data(None).await.unwrap();

    let response = match serde_json::to_string(&order_num) {
        Ok(res) => HttpResponse::Ok()
            .content_type("application/json")
            .body(res),
        Err(_) => HttpResponse::UnavailableForLegalReasons().finish(),
    };

    response
}


#[get("jobs/{job_numbers}")]
async fn jobs(path: web::Path<String>) -> impl Responder {
    let url_str: String = path.into_inner();
    let job_numbers = url_str.split("&").collect::<Vec<&str>>();

    let mut job_bom = get_job_boms(&job_numbers).await.unwrap();
    let mut seen = HashSet::new();

    // Get the unique part numbers
    let unique: Vec<_> = job_bom
        .iter()
        .filter(|item| seen.insert(item.part_num.clone()))
        .map(|item| item.part_num.clone())
        .collect();
    //let part_numbers = job_bom.iter().map(|item| item.part_num.to_owned()).collect::<Vec<String>>();
    
    //Start the pegging process for the unique part numbers
    let peg_process_start = Instant::now();
    println!("Starting Pegging");
    let time_phase_data = get_time_phase_data(Some(unique)).await.unwrap();
    println!("Data retrieved: {:#?}", time_phase_data);
    let peg_process_dur = peg_process_start.elapsed();
    println!("Pegging took: {:#?}", peg_process_dur);

    let new_time_phase_data = Arc::try_unwrap(time_phase_data)
        .expect("Lock still has multiple owners")
        .into_inner()
        .expect("Mutex cannot be unlocked");

    for job_num in job_numbers.iter() {

    for job_mtl in &mut job_bom {
        if job_mtl.direct {
            println!("This line is make or purchase direct!");
            let mut dmd = Demand {
                part_number: job_mtl.part_num.to_owned(),
                job_num: job_mtl.job_num.to_owned(),
                asm: job_mtl.asm,
                mtl: job_mtl.mtl,
                demand_qty: job_mtl.req_qty,
                pegged_demand: job_mtl.req_qty,
                order: 0,
                order_line: 0,
                order_rel: 0,
                due_date: job_mtl.req_date,
                sourcefile: "JM".to_owned(),
                supply: vec![],
            };
            let make_direct_jobs = get_make_direct_jobs(&job_mtl.job_num, job_mtl.asm, job_mtl.mtl)
                .await
                .unwrap();

            for job_prod in make_direct_jobs {
                dmd.supply.push(Supply {
                    due_date: job_prod.due_date,
                    sourcefile: "JH".to_owned(),
                    pegged_qty: job_prod.prod_qty,
                    job_num: job_prod.job_num,
                    asm: 0,
                    mtl: 0,
                    po_num: None,
                    po_rel: None,
                    po_line: None,
                })
            }

            job_mtl.demand.push(dmd);
        } else {
            //let pegged_demand = time_phase_data.get(&job_mtl.part_num).unwrap();
            let pegged_demand = new_time_phase_data.get(&job_mtl.part_num).unwrap();

            for demand_row in pegged_demand {
                if &&demand_row.job_num == job_num {
                    job_mtl.demand.push(demand_row.clone())
                }
            }
        }
    }
    }

    let response = match serde_json::to_string(&job_bom) {
        Ok(res) => HttpResponse::Ok()
            .content_type("application/json")
            .body(res),
        Err(_) => HttpResponse::UnavailableForLegalReasons().finish(),
    };

    // Get the data
    // Filter the data by job_num
    //      In order to do this, we need to get the entire BOM for the job. Need JobMtl table
    //      Then filter entire list of data for each part on the BOM. Only return the result sets
    //      where the Demand is for the related job

    response
}

#[get("/backlog")]
async fn get_backlog() -> impl Responder {
    println!("Testing backlog path");

    // Get the backlog of sales order releases
    let mut backlog = get_backlog_result().await.unwrap();
    println!("Backlog length: {:#?}", backlog.len());

    // Then get all of the job get all of the job boms 
    // This is a vec for now, but it really should be a HashMap
    let job_bom = get_all_job_boms().await.unwrap();
    
    // Get all of the pegging data by part number
    let time_phase_data = get_all_time_phase_data().await.unwrap();

    let new_time_phase_data = Arc::try_unwrap(time_phase_data)
        .expect("Lock still has multiple owners")
        .into_inner()
        .expect("Mutex cannot be unlocked");

    // Peg sales all sales orders in the backlog 
    backlog.iter_mut().for_each(|row| {
        let part_number = row.part_number.clone();
        let demand = new_time_phase_data.get(&part_number);
        match demand {
            Some(dmd) => {
                let filtered_demand: Vec<&Demand> = dmd
                    .iter()
                    .filter(|demand| 
                        demand.order == row.order.unwrap() 
                        && demand.order_line == row.line.unwrap() 
                        && demand.order_rel == row.release.unwrap()
                    )
                    .collect();

                filtered_demand.iter().for_each(|d| {
                    let new_dmd = d.to_owned().to_owned();
                row.demand.push(new_dmd);
                })
            },
            None => return
        };
    });



    let response = match serde_json::to_string(&new_time_phase_data) {
        Ok(res) => HttpResponse::Ok()
            .content_type("application/json")
            .body(res),
        Err(_) => HttpResponse::UnavailableForLegalReasons().finish(),
    };

    // Get the data
    // Filter the data by job_num
    //      In order to do this, we need to get the entire BOM for the job. Need JobMtl table
    //      Then filter entire list of data for each part on the BOM. Only return the result sets
    //      where the Demand is for the related job

    response
}

#[get("job/{job_num}")]
async fn job(path: web::Path<String>) -> impl Responder {
    let job_num: String = path.into_inner();

    let mut job_bom = get_job_bom(&job_num).await.unwrap();

    let mut is_everything_issued = true;
    job_bom.iter().for_each(|mtl| {
        if mtl.issued_qty < mtl.req_qty {
            is_everything_issued = false;
        }
    });

    if is_everything_issued {
        println!("Everything is issued complete!");
        return HttpResponse::Ok()
            .content_type("application/json")
            .body(serde_json::to_string(&job_bom).unwrap());
    };




    let part_numbers = job_bom.iter().map(|item| item.part_num.to_owned()).collect::<Vec<String>>();


    let time_phase_data = get_time_phase_data(Some(part_numbers)).await.unwrap();

    let new_time_phase_data = Arc::try_unwrap(time_phase_data)
        .expect("Lock still has multiple owners")
        .into_inner()
        .expect("Mutex cannot be unlocked");

    for job_mtl in &mut job_bom {
        if job_mtl.issued_qty >= job_mtl.req_qty {
            println!("Material is issued complete");
            continue;
        } else if job_mtl.direct {
            println!("This line is make or purchase direct!");
            let mut dmd = Demand {
                part_number: job_mtl.part_num.to_owned(),
                job_num: job_mtl.job_num.to_owned(),
                asm: job_mtl.asm,
                mtl: job_mtl.mtl,
                demand_qty: job_mtl.req_qty,
                pegged_demand: job_mtl.req_qty,
                order: 0,
                order_line: 0,
                order_rel: 0,
                due_date: job_mtl.req_date,
                sourcefile: "JM".to_owned(),
                supply: vec![],
            };
            let make_direct_jobs = get_make_direct_jobs(&job_mtl.job_num, job_mtl.asm, job_mtl.mtl)
                .await
                .unwrap();

            for job_prod in make_direct_jobs {
                dmd.supply.push(Supply {
                    due_date: job_prod.due_date,
                    sourcefile: "JH".to_owned(),
                    pegged_qty: job_prod.prod_qty,
                    job_num: job_prod.job_num,
                    asm: 0,
                    mtl: 0,
                    po_line: None,
                    po_rel: None,
                    po_num: None,
                })
            }

            job_mtl.demand.push(dmd);
        } else {
            //let pegged_demand = time_phase_data.get(&job_mtl.part_num).unwrap();
            let pegged_demand = new_time_phase_data.get(&job_mtl.part_num).unwrap();

            for demand_row in pegged_demand {
                if demand_row.job_num == job_num {
                    job_mtl.demand.push(demand_row.clone())
                }
            }
        }
    }

    let response = match serde_json::to_string(&job_bom) {
        Ok(res) => HttpResponse::Ok()
            .content_type("application/json")
            .body(res),
        Err(_) => HttpResponse::UnavailableForLegalReasons().finish(),
    };

    // Get the data
    // Filter the data by job_num
    //      In order to do this, we need to get the entire BOM for the job. Need JobMtl table
    //      Then filter entire list of data for each part on the BOM. Only return the result sets
    //      where the Demand is for the related job

    response
}

pub async fn get_all_time_phase_data() -> Result<Arc<Mutex<HashMap<String, Vec<Demand>>>>, anyhow::Error>
{
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let mut client = Client::connect(config, tcp).await?;
    let on_hand = get_parts_on_hand().await.unwrap();

    // Construct Query
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
                and PD.Company = 'AE'".to_string();

    new_query.push_str("
            ORDER BY 
                PD.PartNum,
                PD.DueDate,
                PD.RequirementFlag
        ");

    let mut result: Vec<SQLReturnRow> = vec![];

    let qry_start = Instant::now();
    let mut new_query = Query::new(new_query);

    // Stream Query
    let stream = new_query.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;
    let qry_dur = qry_start.elapsed();
    println!("Query took: {:#?}", qry_dur);

    let mut net_qty = dec!(0.0);
    let mut id = 0;

    let tf_start = Instant::now();
    row.iter().for_each(|val| {
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
        let part_num = val
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
        let asm = val.get::<i32, _>("AssemblySeq").unwrap().to_owned();
        let mtl = val.get::<i32, _>("JobSeq").unwrap().to_owned();
        let order = val.get::<i32, _>("OrderNum").unwrap().to_owned();
        let order_line = val.get::<i32, _>("OrderLine").unwrap().to_owned();
        let order_rel = val.get::<i32, _>("OrderRelNum").unwrap().to_owned();
        let po_num = transform_zero_to_none(val.get::<i32, _>("PONum").to_owned());
        let po_line = transform_zero_to_none(val.get::<i32, _>("POLine").to_owned());
        let po_rel = transform_zero_to_none(val.get::<i32, _>("PORelNum").to_owned());

        if requirement {
            net_qty = net_qty.saturating_sub(qty);
        } else {
            net_qty = net_qty.saturating_add(qty);
        }

        result.push(SQLReturnRow {
            id,
            part_num,
            job_num,
            asm,
            mtl,
            requirement,
            due_date,
            sourcefile,
            qty,
            net_qty,
            order,
            order_line,
            order_rel,
            po_num,
            po_line,
            po_rel,
            direct: !direct,
        });

        id += 1;
    });
    let tf_dur = tf_start.elapsed();
    println!("Transform Took: {:#?}", tf_dur);

    // Close Client Connection
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    println!("Getting unique parts");
    let unq_start = Instant::now();
    //let mut new_result = HashMap::new();
    let multi_results = Arc::new(Mutex::new(HashMap::new()));

    let unique_part_numbers = get_unique_part_numbers(&result);
    let unq_dur = unq_start.elapsed();
    println!("Getting Unique Parts took: {:#?}", unq_dur);

    // Peg unique part numbers
    unique_part_numbers.par_iter().for_each(|item| {
        let multi_peg_data = multi_peg_part_dtl(&result, &on_hand, &item);
        let mut multi_results = multi_results.lock().unwrap();
        multi_results.insert(item.to_owned(), multi_peg_data);

    });

    Ok(multi_results)
}


pub async fn get_time_phase_data(part_numbers: Option<Vec<String>>) -> Result<Arc<Mutex<HashMap<String, Vec<Demand>>>>, anyhow::Error>
{
    let parts = part_numbers.unwrap();
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let mut client = Client::connect(config, tcp).await?;
    let on_hand = get_parts_on_hand().await.unwrap();

    // Construct Query

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
                and PD.PartNum IN (".to_string();

    parts.iter().enumerate().for_each(|(i, _)| {
        let next = parts.get(i + 1);
        match next {
            Some(_) => new_query.push_str(&format!("@P{}, ", i + 1)),
            None => new_query.push_str(&format!("@P{}", i + 1))
        }
    });

    new_query.push(')');
    new_query.push_str("
            ORDER BY 
                PD.PartNum,
                PD.DueDate,
                PD.RequirementFlag
        ");

    let mut result: Vec<SQLReturnRow> = vec![];

    let qry_start = Instant::now();
    let mut new_query = Query::new(new_query);
    parts.iter().for_each(|part| {
        new_query.bind(part.to_owned());
    });

    // Stream Query
    let stream = new_query.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;
    let qry_dur = qry_start.elapsed();
    println!("Query took: {:#?}", qry_dur);

    let mut net_qty = dec!(0.0);
    let mut id = 0;

    let tf_start = Instant::now();
    row.iter().for_each(|val| {
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
        let part_num = val
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
        let asm = val.get::<i32, _>("AssemblySeq").unwrap().to_owned();
        let mtl = val.get::<i32, _>("JobSeq").unwrap().to_owned();
        let order = val.get::<i32, _>("OrderNum").unwrap().to_owned();
        let order_line = val.get::<i32, _>("OrderLine").unwrap().to_owned();
        let order_rel = val.get::<i32, _>("OrderRelNum").unwrap().to_owned();
        let po_num = transform_zero_to_none(val.get::<i32, _>("PONum").to_owned());
        let po_line = transform_zero_to_none(val.get::<i32, _>("POLine").to_owned());
        let po_rel = transform_zero_to_none(val.get::<i32, _>("PORelNum").to_owned());

        if requirement {
            net_qty = net_qty.saturating_sub(qty);
        } else {
            net_qty = net_qty.saturating_add(qty);
        }

        result.push(SQLReturnRow {
            id,
            part_num,
            job_num,
            asm,
            mtl,
            requirement,
            due_date,
            sourcefile,
            qty,
            net_qty,
            order,
            order_line,
            order_rel,
            po_num,
            po_line,
            po_rel,
            direct: !direct,
        });

        id += 1;
    });
    let tf_dur = tf_start.elapsed();
    println!("Transform Took: {:#?}", tf_dur);

    // Close Client Connection
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    println!("Getting unique parts");
    let unq_start = Instant::now();
    //let mut new_result = HashMap::new();
    let multi_results = Arc::new(Mutex::new(HashMap::new()));

    let unique_part_numbers = get_unique_part_numbers(&result);
    let unq_dur = unq_start.elapsed();
    println!("Getting Unique Parts took: {:#?}", unq_dur);

    // Peg unique part numbers
    unique_part_numbers.par_iter().for_each(|item| {
        let multi_peg_data = multi_peg_part_dtl(&result, &on_hand, &item);
        let mut multi_results = multi_results.lock().unwrap();
        multi_results.insert(item.to_owned(), multi_peg_data);

    });

    Ok(multi_results)
}

fn get_unique_part_numbers(part_dtls: &Vec<SQLReturnRow>) -> Vec<String> {
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

fn _peg_part_dtl(part_dtl: Vec<&SQLReturnRow>, on_hand: &Vec<OnHand>) -> Vec<Demand> {
    let mut intermediate_pegging: Vec<Demand> = Vec::new();

    let mut remaining_supplies: Vec<SQLReturnRow> = vec![];

    // Add remaining supplies from on hand quantity
    on_hand.iter().for_each(|row| {
        let new_oh = SQLReturnRow::new_on_hand(&row.part_num, row.qty);
        remaining_supplies.push(new_oh);
    });

    let part_dtl_supplies: Vec<SQLReturnRow> = part_dtl
        .clone()
        .into_iter()
        .filter(|a| a.requirement == false)
        .cloned()
        .collect();

    for row in part_dtl_supplies {
        remaining_supplies.push(row)
    }

    let mut sorted_demands: Vec<&SQLReturnRow> = part_dtl
        .clone()
        .into_iter()
        .filter(|a| a.requirement == true)
        .collect();

    sorted_demands.sort_by(|a, b| a.due_date.cmp(&b.due_date));

    for demand in sorted_demands.iter() {
        let mut pegged_demand = Demand {
            part_number: demand.part_num.to_owned(),
            due_date: demand.due_date,
            sourcefile: demand.sourcefile.to_owned(),
            demand_qty: demand.qty,
            job_num: demand.job_num.to_owned(),
            asm: demand.asm,
            mtl: demand.mtl,
            order: demand.order,
            order_line: demand.order_line,
            order_rel: demand.order_rel,
            supply: vec![],
            pegged_demand: dec!(0.0),
        };

        let mut demand_quantity_remaining = pegged_demand.demand_qty;

        // While the remaining demand quantity is greater than zero and there is still open supply
        while demand_quantity_remaining > dec!(0.0) && !remaining_supplies.is_empty() {
            // Calculate the quantity to be used. This should be equal to either the remaining
            // demand quantity if it is min, or the remaining supply quantity if it is min
            let supply_used_quantity =
                Decimal::min(remaining_supplies[0].qty, demand_quantity_remaining);

            pegged_demand.pegged_demand += supply_used_quantity;

            pegged_demand.supply.push(Supply {
                due_date: remaining_supplies[0].due_date,
                job_num: remaining_supplies[0].job_num.clone(),
                sourcefile: remaining_supplies[0].sourcefile.clone(),
                asm: remaining_supplies[0].asm,
                mtl: remaining_supplies[0].mtl,
                pegged_qty: supply_used_quantity,
                po_num: remaining_supplies[0].po_num,
                po_line: remaining_supplies[0].po_line,
                po_rel: remaining_supplies[0].po_rel,
            });

            // Subtract any used quantity from the supply
            demand_quantity_remaining -= supply_used_quantity;

            if remaining_supplies[0].qty > supply_used_quantity {
                let new_qty = remaining_supplies[0]
                    .qty
                    .checked_sub(supply_used_quantity)
                    .unwrap();

                remaining_supplies[0].qty = new_qty;
            } else {
                remaining_supplies.remove(0);
            }
        }

        intermediate_pegging.push(pegged_demand);
    }

    let result: Vec<Demand> = intermediate_pegging.into_iter().collect();
    result
}

fn multi_peg_part_dtl(
    part_dtl: &Vec<SQLReturnRow>,
    on_hand: &Vec<OnHand>,
    part_num: &str,
) -> Vec<Demand> {
    let filtered_parts: Vec<&SQLReturnRow> = part_dtl
        .into_iter()
        .filter(|part| &part.part_num == part_num)
        .collect();
    //println!("Part: {:#?}", filtered_parts);

    let filtered_on_hand: Vec<&OnHand> = on_hand
        .into_iter()
        .filter(|row| &row.part_num == part_num)
        .collect();

    let mut intermediate_pegging: Vec<Demand> = Vec::new();

    let mut remaining_supplies: Vec<SQLReturnRow> = vec![];

    // Add remaining supplies from on hand quantity
    filtered_on_hand.iter().for_each(|row| {
        let new_oh = SQLReturnRow::new_on_hand(&row.part_num, row.qty);
        remaining_supplies.push(new_oh);
    });

    let part_dtl_supplies: Vec<&&SQLReturnRow> = filtered_parts
        .iter()
        .filter(|a| a.requirement == false)
        .collect();

    for row in part_dtl_supplies {
        remaining_supplies.push(row.to_owned().to_owned())
    }

    let mut sorted_demands: Vec<&&SQLReturnRow> = filtered_parts
        .iter()
        .filter(|a| a.requirement == true)
        .collect();

    sorted_demands.sort_by(|a, b| a.due_date.cmp(&b.due_date));

    for demand in sorted_demands.iter() {
        let mut pegged_demand = Demand {
            part_number: demand.part_num.to_owned(),
            due_date: demand.due_date,
            sourcefile: demand.sourcefile.to_owned(),
            demand_qty: demand.qty,
            job_num: demand.job_num.to_owned(),
            asm: demand.asm,
            mtl: demand.mtl,
            order: demand.order,
            order_line: demand.order_line,
            order_rel: demand.order_rel,
            supply: vec![],
            pegged_demand: dec!(0.0),
        };

        let mut demand_quantity_remaining = pegged_demand.demand_qty;

        // While the remaining demand quantity is greater than zero and there is still open supply
        while demand_quantity_remaining > dec!(0.0) && !remaining_supplies.is_empty() {
            // Calculate the quantity to be used. This should be equal to either the remaining
            // demand quantity if it is min, or the remaining supply quantity if it is min
            let supply_used_quantity =
                Decimal::min(remaining_supplies[0].qty, demand_quantity_remaining);

            pegged_demand.pegged_demand += supply_used_quantity;

            pegged_demand.supply.push(Supply {
                due_date: remaining_supplies[0].due_date,
                job_num: remaining_supplies[0].job_num.clone(),
                sourcefile: remaining_supplies[0].sourcefile.clone(),
                asm: remaining_supplies[0].asm,
                mtl: remaining_supplies[0].mtl,
                pegged_qty: supply_used_quantity,
                po_num: remaining_supplies[0].po_num,
                po_line: remaining_supplies[0].po_line,
                po_rel: remaining_supplies[0].po_rel,
            });

            // Subtract any used quantity from the supply
            demand_quantity_remaining -= supply_used_quantity;

            if remaining_supplies[0].qty > supply_used_quantity {
                let new_qty = remaining_supplies[0]
                    .qty
                    .checked_sub(supply_used_quantity)
                    .unwrap();

                remaining_supplies[0].qty = new_qty;
            } else {
                remaining_supplies.remove(0);
            }
        }

        intermediate_pegging.push(pegged_demand);
    }

    //let result: Vec<Demand> = intermediate_pegging.into_iter().collect();
    intermediate_pegging
    //result
}
