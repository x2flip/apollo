use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BaqResult<T> {
    #[serde(rename = "odata.metadata")]
    pub odata_metadata: String,
    pub value: Vec<T>,
}
