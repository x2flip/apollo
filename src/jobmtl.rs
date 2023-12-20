use async_std::net::TcpStream;

use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::io::{Error, ErrorKind};
use tiberius::{Client, Query};

use crate::{parttimephase::Demand, sql::get_sql_config};

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct JobMtl {
    pub job_num: String,
    pub asm: i32,
    pub mtl: i32,
    pub jobop: i32,
    pub part_num: String,
    pub demand: Vec<Demand>,
    pub direct: bool,
    pub req_qty: Decimal,
    pub req_date: NaiveDate,
}

pub async fn get_job_boms(job_numbers: &Vec<&str>) -> Result<Vec<JobMtl>, anyhow::Error> {
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let mut client = Client::connect(config, tcp).await?;

    // Construct Query
    let mut query_string = "
            SELECT
                JM.JobNum,
                JM.AssemblySeq,
                JM.MtlSeq,
                JM.PartNum,
                JM.Direct,
                JM.RequiredQty,
                JM.ReqDate,
                JM.RelatedOperation
            FROM 
                Erp.JobMtl as JM
            WHERE 
                JM.Company = 'AE'
                and JM.JobNum IN (
            "
    .to_string();

    job_numbers.iter().enumerate().for_each(|(i, _)| {
        let next = job_numbers.get(i + 1);
        match next {
            Some(_) => query_string.push_str(&format!("@P{}, ", i + 1)),
            None => query_string.push_str(&format!("@P{}", i + 1)),
        }
    });

    query_string.push(')');

    query_string.push_str(
        "
            ORDER BY 
                JM.JobNum,
                JM.AssemblySeq,
                JM.MtlSeq
        ",
    );

    let mut select = Query::new(query_string);

    job_numbers.iter().for_each(|job| {
        select.bind(job.to_owned());
    });

    let mut result: Vec<JobMtl> = vec![];

    // Stream Query
    let stream = select.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;

    row.iter().for_each(|val| {
        let job_num = val.get("JobNum").unwrap_or("").to_owned();
        let asm = val.get("AssemblySeq").unwrap_or(0).to_owned();
        let mtl = val.get("MtlSeq").unwrap_or(0).to_owned();
        let jobop = val.get("RelatedOperation").unwrap_or(0).to_owned();
        let part_num = val.get("PartNum").unwrap_or("").to_owned();
        let direct = val.get::<bool, _>("Direct").unwrap_or(false).to_owned();
        let req_qty = val
            .get::<Decimal, _>("RequiredQty")
            .unwrap_or(dec![0.0])
            .to_owned();
        let req_date = val
            .get::<NaiveDate, _>("ReqDate")
            .unwrap_or(NaiveDate::from_ymd_opt(1999, 1, 1).unwrap())
            .to_owned();

        result.push(JobMtl {
            job_num,
            asm,
            mtl,
            part_num,
            demand: vec![],
            direct,
            req_qty,
            req_date,
            jobop,
        });
    });

    // println!("{:?}", rows);

    // Close Client Connection
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    // Result set should be cached now

    return Ok(result);
}
pub async fn get_job_bom(job_num: &str) -> Result<Vec<JobMtl>, anyhow::Error> {
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let mut client = Client::connect(config, tcp).await?;

    // Construct Query
    let mut select = Query::new(
        "
            SELECT
                JM.JobNum,
                JM.AssemblySeq,
                JM.MtlSeq,
                JM.PartNum,
                JM.Direct,
                JM.RequiredQty,
                JM.ReqDate,
                JM.RelatedOperation
            FROM 
                Erp.JobMtl as JM
            WHERE 
                JM.JobNum = @P1
                and JM.Company = 'AE'
            ",
    );

    select.bind(job_num);

    let mut result: Vec<JobMtl> = vec![];

    // Stream Query
    let stream = select.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;

    row.iter().for_each(|val| {
        let job_num = val.get("JobNum").unwrap_or("").to_owned();
        let asm = val.get("AssemblySeq").unwrap_or(0).to_owned();
        let mtl = val.get("MtlSeq").unwrap_or(0).to_owned();
        let jobop = val.get("RelatedOperation").unwrap_or(0).to_owned();
        let part_num = val.get("PartNum").unwrap_or("").to_owned();
        let direct = val.get::<bool, _>("Direct").unwrap_or(false).to_owned();
        let req_qty = val
            .get::<Decimal, _>("RequiredQty")
            .unwrap_or(dec![0.0])
            .to_owned();
        let req_date = val
            .get::<NaiveDate, _>("ReqDate")
            .unwrap_or(NaiveDate::from_ymd_opt(1999, 1, 1).unwrap())
            .to_owned();

        result.push(JobMtl {
            job_num,
            asm,
            mtl,
            part_num,
            demand: vec![],
            direct,
            req_qty,
            req_date,
            jobop,
        });
    });

    // println!("{:?}", rows);

    // Close Client Connection
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    // Result set should be cached now

    return Ok(result);
}
