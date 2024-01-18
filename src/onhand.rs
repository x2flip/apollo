use async_std::net::TcpStream;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::io::{Error, ErrorKind};
use tiberius::{Client, Query};

use crate::sql::get_sql_config;

#[allow(dead_code)]
#[derive(Debug, Serialize, Clone)]
pub struct OnHand {
    pub part_num: String,
    pub site: String,
    pub qty: Decimal,
}

pub async fn get_parts_on_hand() -> Result<Vec<OnHand>, anyhow::Error> {
    let config = get_sql_config();

    // Create TCP TcpStream
    let tcp = TcpStream::connect(&config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    // Connect to server
    let mut client = Client::connect(config, tcp).await?;

    // Construct Query
    let select = Query::new(
        "
        select 
	        [PartWhse].[PartNum] as [PartWhse_PartNum],
	        [Warehse].[Plant] as [Warehse_Plant],
	        ((case
                when sum(PartBin.OnhandQty) is null then 0.00
            else sum(case when WhseBin.NonNettable = 0 then PartBin.OnhandQty else 0 end)
            end)) as [Calculated_sumOfQty]

        from Erp.Warehse as Warehse

        inner join Erp.PartWhse as PartWhse on 
	        PartWhse.Company = Warehse.Company
	        and PartWhse.WarehouseCode = Warehse.WarehouseCode

        left outer join Erp.PartBin as PartBin on 
	        PartWhse.Company = PartBin.Company
	        and PartWhse.PartNum = PartBin.PartNum
	        and PartWhse.WarehouseCode = PartBin.WarehouseCode

        left outer join Erp.WhseBin as WhseBin on 
	        PartBin.Company = WhseBin.Company
	        and PartBin.WarehouseCode = WhseBin.WarehouseCode
	        and PartBin.BinNum = WhseBin.BinNum
	        and ( WhseBin.NonNettable = 0  )

        where (not Warehse.Plant like 'CONS%' and Warehse.Company = 'AE')

        group by 
            [PartWhse].[PartNum],
	        [Warehse].[Plant]
            ",
    );

    let mut result: Vec<OnHand> = vec![];

    // Stream Query
    let stream = select.query(&mut client).await?;

    // Consume stream
    let row = stream.into_first_result().await?;

    row.iter().for_each(|val| {
        let part_num = val.get("PartWhse_PartNum").unwrap_or("").to_owned();
        let site = val.get("Warehse_Plant").unwrap_or("").to_owned();
        let qty = val
            .get::<Decimal, _>("Calculated_sumOfQty")
            .unwrap_or(dec![0.0])
            .to_owned();

        result.push(OnHand {
            part_num,
            site,
            qty,
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
