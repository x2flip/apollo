use crate::{orderrelease::OrderRelease, sql::get_sql_config};
use async_std::net::TcpStream;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::io::{Error, ErrorKind};
use tiberius::{Client, Query};

pub async fn get_backlog_result() -> Result<Vec<OrderRelease>, anyhow::Error> {
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let mut client = Client::connect(config, tcp).await?;

    // Construct Query
    let query_string = "
            SELECT
                OrderRel.OrderNum,
                OrderRel.OrderLine,
                OrderRel.OrderRelNum,
                OrderRel.PartNum
            FROM 
                Erp.OrderRel
            WHERE 
                OrderRel.Company = 'AE'
                and OrderRel.OpenRelease = 1
                and OrderRel.FirmRelease = 1
                and OrderRel.Plant = 'MfgSys'
            "
    .to_string();

    let select = Query::new(query_string);

    // Stream Query
    let stream = select.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;

    // Transform Rows into result type
    let mut result: Vec<OrderRelease> = vec![];

    row.iter().for_each(|val| {
        let order = val.get::<i32, _>("OrderNum");
        let line = val.get::<i32, _>("OrderLine");
        let release = val.get::<i32, _>("OrderRelNum");
        let part_number = val.get::<&str, &str>("PartNum").unwrap().to_owned();

        result.push(OrderRelease {
            order,
            line,
            release,
            part_number,
            demand: vec![],
        });
    });

    // Close Client Connection
    client
        .close()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

    // Result set should be cached now

    return Ok(result);
}
