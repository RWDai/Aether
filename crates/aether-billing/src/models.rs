use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingUnit {
    Per1MTokens,
    Per1MTokensHour,
    PerRequest,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BillingDimension {
    pub name: String,
    pub usage_field: String,
    pub price_field: String,
    pub unit: BillingUnit,
    pub default_price: f64,
}

impl BillingDimension {
    pub fn calculate(&self, usage_value: f64, price: f64) -> f64 {
        if usage_value <= 0.0 || price <= 0.0 {
            return 0.0;
        }
        match self.unit {
            BillingUnit::Per1MTokens | BillingUnit::Per1MTokensHour => {
                (usage_value / 1_000_000.0) * price
            }
            BillingUnit::PerRequest => usage_value * price,
            BillingUnit::Fixed => price,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct StandardizedUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_creation_tokens: i64,
    pub cache_read_tokens: i64,
    pub reasoning_tokens: i64,
    pub cache_storage_token_hours: f64,
    pub request_count: i64,
    pub dimensions: BTreeMap<String, serde_json::Value>,
}

impl StandardizedUsage {
    pub fn new() -> Self {
        Self {
            request_count: 1,
            ..Self::default()
        }
    }

    pub fn get(&self, field_name: &str) -> Option<serde_json::Value> {
        match field_name {
            "input_tokens" => Some(serde_json::json!(self.input_tokens)),
            "output_tokens" => Some(serde_json::json!(self.output_tokens)),
            "cache_creation_tokens" => Some(serde_json::json!(self.cache_creation_tokens)),
            "cache_read_tokens" => Some(serde_json::json!(self.cache_read_tokens)),
            "reasoning_tokens" => Some(serde_json::json!(self.reasoning_tokens)),
            "cache_storage_token_hours" => Some(serde_json::json!(self.cache_storage_token_hours)),
            "request_count" => Some(serde_json::json!(self.request_count)),
            "extra" | "dimensions" => Some(serde_json::json!(self.dimensions)),
            _ => self.dimensions.get(field_name).cloned(),
        }
    }

    pub fn set(&mut self, field_name: &str, value: impl Into<serde_json::Value>) {
        let value = value.into();
        match field_name {
            "input_tokens" => self.input_tokens = as_i64(&value, 0),
            "output_tokens" => self.output_tokens = as_i64(&value, 0),
            "cache_creation_tokens" => self.cache_creation_tokens = as_i64(&value, 0),
            "cache_read_tokens" => self.cache_read_tokens = as_i64(&value, 0),
            "reasoning_tokens" => self.reasoning_tokens = as_i64(&value, 0),
            "cache_storage_token_hours" => self.cache_storage_token_hours = as_f64(&value, 0.0),
            "request_count" => self.request_count = as_i64(&value, 0),
            "extra" | "dimensions" => {
                self.dimensions = match value {
                    serde_json::Value::Object(map) => map.into_iter().collect(),
                    _ => BTreeMap::new(),
                }
            }
            _ => {
                self.dimensions.insert(field_name.to_string(), value);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct CostBreakdown {
    pub costs: BTreeMap<String, f64>,
    pub total_cost: f64,
    pub tier_index: Option<i64>,
    pub effective_prices: BTreeMap<String, f64>,
}

fn as_i64(value: &serde_json::Value, default: i64) -> i64 {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
        .unwrap_or(default)
}

fn as_f64(value: &serde_json::Value, default: f64) -> f64 {
    value.as_f64().unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{BillingDimension, BillingUnit, StandardizedUsage};

    #[test]
    fn dimension_calculates_per_million_tokens() {
        let dimension = BillingDimension {
            name: "input".to_string(),
            usage_field: "input_tokens".to_string(),
            price_field: "input_price_per_1m".to_string(),
            unit: BillingUnit::Per1MTokens,
            default_price: 0.0,
        };
        assert_eq!(dimension.calculate(500_000.0, 2.0), 1.0);
    }

    #[test]
    fn standardized_usage_reads_and_writes_known_and_extra_fields() {
        let mut usage = StandardizedUsage::new();
        usage.set("input_tokens", 10);
        usage.set("custom_dimension", "value");

        assert_eq!(usage.get("input_tokens"), Some(serde_json::json!(10)));
        assert_eq!(
            usage.get("custom_dimension"),
            Some(serde_json::json!("value"))
        );
    }
}
