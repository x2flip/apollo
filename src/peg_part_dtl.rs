use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::{onhand::OnHand, parttimephase::PartDtl};

pub fn multi_peg_part_dtl(
    part_dtl: &Vec<PartDtl>,
    on_hand: &Vec<OnHand>,
    part_num: &str,
) -> Vec<PartDtl> {
    let filtered_parts: Vec<&PartDtl> = part_dtl
        .into_iter()
        .filter(|part| &part.part_number == part_num)
        .collect();
    //println!("Part: {:#?}", filtered_parts);

    let filtered_on_hand: Vec<&OnHand> = on_hand
        .into_iter()
        .filter(|row| &row.part_num == part_num)
        .collect();

    let mut intermediate_pegging: Vec<PartDtl> = Vec::new();

    let mut remaining_supplies: Vec<PartDtl> = vec![];

    // Add remaining supplies from on hand quantity
    filtered_on_hand.iter().for_each(|row| {
        let new_oh = PartDtl::new_on_hand(&row.part_num, row.qty);
        remaining_supplies.push(new_oh);
    });

    let part_dtl_supplies: Vec<&&PartDtl> = filtered_parts
        .iter()
        .filter(|a| a.requirement == false)
        .collect();

    for row in part_dtl_supplies {
        remaining_supplies.push(row.to_owned().to_owned())
    }

    let mut sorted_demands: Vec<&&PartDtl> = filtered_parts
        .iter()
        .filter(|a| a.requirement == true)
        .collect();

    sorted_demands.sort_by(|a, b| a.due_date.cmp(&b.due_date));

    for demand in sorted_demands.iter() {
        let mut pegged_demand = PartDtl {
            part_number: demand.part_number.to_owned(),
            due_date: demand.due_date,
            sourcefile: demand.sourcefile.to_owned(),
            qty: demand.qty,
            job_num: demand.job_num.to_owned(),
            asm: demand.asm,
            mtl: demand.mtl,
            order: demand.order,
            order_line: demand.order_line,
            order_rel: demand.order_rel,
            supply: vec![],
            requirement: demand.requirement,
            direct: demand.direct,
            po_num: demand.po_num,
            po_line: demand.po_line,
            po_rel: demand.po_rel,
            bom: vec![],
        };

        let mut demand_quantity_remaining = pegged_demand.qty;

        // While the remaining demand quantity is greater than zero and there is still open supply
        while demand_quantity_remaining > dec!(0.0) && !remaining_supplies.is_empty() {
            // Calculate the quantity to be used. This should be equal to either the remaining
            // demand quantity if it is min, or the remaining supply quantity if it is min
            let supply_used_quantity =
                Decimal::min(remaining_supplies[0].qty, demand_quantity_remaining);

            pegged_demand.supply.push(PartDtl {
                due_date: remaining_supplies[0].due_date,
                job_num: remaining_supplies[0].job_num.clone(),
                sourcefile: remaining_supplies[0].sourcefile.clone(),
                asm: remaining_supplies[0].asm,
                mtl: remaining_supplies[0].mtl,
                qty: remaining_supplies[0].qty,
                po_num: remaining_supplies[0].po_num,
                po_line: remaining_supplies[0].po_line,
                po_rel: remaining_supplies[0].po_rel,
                part_number: remaining_supplies[0].part_number.to_string(),
                requirement: remaining_supplies[0].requirement,
                direct: remaining_supplies[0].direct,
                order: remaining_supplies[0].order,
                order_line: remaining_supplies[0].order_line,
                order_rel: remaining_supplies[0].order_rel,
                supply: vec![],
                bom: vec![],
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

// This is going to be a test set of data to validate it is as expected

#[cfg(test)]
mod tests {
    use super::*; // Import the outer module to the test module

    #[test]
    fn test_add_two() {
        let test_part_dtl = vec![PartDtl {
            part_number: todo!(),
            requirement: todo!(),
            direct: todo!(),
            due_date: todo!(),
            sourcefile: todo!(),
            qty: todo!(),
            job_num: todo!(),
            asm: todo!(),
            mtl: todo!(),
            po_num: todo!(),
            po_line: todo!(),
            po_rel: todo!(),
            order: todo!(),
            order_line: todo!(),
            order_rel: todo!(),
            supply: todo!(),
            bom: todo!(),
        }];
        assert_eq!(multi_peg_part_dtl(2), 4);
    }
}
