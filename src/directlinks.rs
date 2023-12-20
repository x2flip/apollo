use async_std::net::TcpStream;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::io::{Error, ErrorKind};
use tiberius::{Client, Query};

use crate::sql::get_sql_config;

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct JobProd {
    pub job_num: String,
    pub due_date: NaiveDate,
    pub prod_qty: Decimal,
    pub target_job_num: String,
    pub target_asm: i32,
    pub target_mtl: i32,
}

pub async fn get_make_direct_jobs(
    job_num: &str,
    asm: i32,
    mtl: i32,
) -> Result<Vec<JobProd>, anyhow::Error> {
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
                JP.JobNum,
                JP.TargetJobNum,
                JP.TargetAssemblySeq,
                JP.TargetMtlSeq,
                JP.ProdQty,
                JH.DueDate

            FROM 
                Erp.JobProd as JP

            INNER JOIN Erp.JobHead as JH on 
                JP.Company = JH.Company
                and JP.JobNum = JH.JobNum

            WHERE 
                JP.TargetJobNum = @P1
                and JP.TargetAssemblySeq = @P2
                and JP.TargetMtlSeq = @P3
                and JH.Company = 'AE'
            ",
    );

    select.bind(job_num);
    select.bind(asm);
    select.bind(mtl);

    let mut result: Vec<JobProd> = vec![];

    // Stream Query
    let stream = select.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;

    row.iter().for_each(|val| {
        let job_num = val.get("JobNum").unwrap_or("").to_owned();
        let target_job_num = val.get("TargetJobNum").unwrap_or("").to_owned();
        let target_asm = val.get("TargetAssemblySeq").unwrap_or(0).to_owned();
        let target_mtl = val.get("TargetMtlSeq").unwrap_or(0).to_owned();
        let due_date = val
            .get::<NaiveDate, _>("DueDate")
            .unwrap_or(NaiveDate::from_ymd_opt(1999, 1, 1).unwrap())
            .to_owned();
        let prod_qty = val
            .get::<Decimal, _>("ProdQty")
            .unwrap_or(dec![0.0])
            .to_owned();

        result.push(JobProd {
            job_num,
            target_job_num,
            target_asm,
            target_mtl,
            due_date,
            prod_qty,
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
