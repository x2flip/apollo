pub fn transform_zero_to_none(val: Option<i32>) -> Option<i32> {
    match val {
        Some(v) => {
            if v == 0 {
                None
            } else {
                Some(v)
            }
        }
        None => None,
    }
}
