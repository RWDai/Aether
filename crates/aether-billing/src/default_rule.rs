use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::pricing::BillingModelPricingSnapshot;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VirtualBillingRule {
    pub id: String,
    pub name: String,
    pub task_type: String,
    pub expression: String,
    pub variables: BTreeMap<String, Value>,
    pub dimension_mappings: BTreeMap<String, Value>,
    pub scope: String,
}

pub struct DefaultBillingRuleGenerator;

impl DefaultBillingRuleGenerator {
    pub fn generate_for_pricing(
        pricing: &BillingModelPricingSnapshot,
        task_type: &str,
    ) -> Option<VirtualBillingRule> {
        let tiers = pricing
            .effective_tiered_pricing()
            .and_then(|value| value.get("tiers"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if tiers.is_empty() && pricing.effective_price_per_request().is_none() {
            return None;
        }

        let first_tier = tiers.first().cloned().unwrap_or_else(|| json!({}));
        let base_input_price = tier_value(&first_tier, "input_price_per_1m", 0.0);
        let base_output_price = tier_value(&first_tier, "output_price_per_1m", 0.0);
        let base_cache_creation_price =
            tier_value_with_fallback(&first_tier, "cache_creation_price_per_1m", 1.25);
        let base_cache_read_price =
            tier_value_with_fallback(&first_tier, "cache_read_price_per_1m", 0.1);
        let base_request_price = pricing.effective_price_per_request().unwrap_or(0.0);

        let mut variables = BTreeMap::new();
        variables.insert("input_price_per_1m".to_string(), json!(base_input_price));
        variables.insert("output_price_per_1m".to_string(), json!(base_output_price));
        variables.insert(
            "cache_creation_price_per_1m".to_string(),
            json!(base_cache_creation_price),
        );
        variables.insert(
            "cache_creation_ephemeral_5m_price_per_1m".to_string(),
            json!(base_cache_creation_price),
        );
        variables.insert(
            "cache_creation_ephemeral_1h_price_per_1m".to_string(),
            json!(base_cache_creation_price),
        );
        variables.insert(
            "cache_read_price_per_1m".to_string(),
            json!(base_cache_read_price),
        );
        variables.insert("price_per_request".to_string(), json!(base_request_price));

        let mut dimension_mappings = BTreeMap::new();
        for (name, key, default) in [
            ("input_tokens", "input_tokens", json!(0)),
            ("output_tokens", "output_tokens", json!(0)),
            ("cache_creation_tokens", "cache_creation_tokens", json!(0)),
            (
                "cache_creation_ephemeral_5m_tokens",
                "cache_creation_ephemeral_5m_tokens",
                json!(0),
            ),
            (
                "cache_creation_ephemeral_1h_tokens",
                "cache_creation_ephemeral_1h_tokens",
                json!(0),
            ),
            (
                "cache_creation_uncategorized_tokens",
                "cache_creation_uncategorized_tokens",
                json!(0),
            ),
            ("cache_read_tokens", "cache_read_tokens", json!(0)),
            ("request_count", "request_count", json!(1)),
        ] {
            dimension_mappings.insert(
                name.to_string(),
                json!({
                    "source": "dimension",
                    "key": key,
                    "required": false,
                    "allow_zero": true,
                    "default": default,
                }),
            );
        }

        for (name, expression) in [
            ("input_cost", "input_tokens * input_price_per_1m / 1000000"),
            (
                "output_cost",
                "output_tokens * output_price_per_1m / 1000000",
            ),
            (
                "cache_creation_uncategorized_cost",
                "cache_creation_uncategorized_tokens * cache_creation_price_per_1m / 1000000",
            ),
            (
                "cache_creation_ephemeral_5m_cost",
                "cache_creation_ephemeral_5m_tokens * cache_creation_ephemeral_5m_price_per_1m / 1000000",
            ),
            (
                "cache_creation_ephemeral_1h_cost",
                "cache_creation_ephemeral_1h_tokens * cache_creation_ephemeral_1h_price_per_1m / 1000000",
            ),
            (
                "cache_read_cost",
                "cache_read_tokens * cache_read_price_per_1m / 1000000",
            ),
            ("request_cost", "request_count * price_per_request"),
        ] {
            dimension_mappings.insert(
                name.to_string(),
                json!({
                    "source": "computed",
                    "expression": expression,
                    "required": false,
                    "default": 0,
                }),
            );
        }

        if !tiers.is_empty() {
            dimension_mappings.insert(
                "input_price_per_1m".to_string(),
                json!({
                    "source": "tiered",
                    "tier_key": "total_input_context",
                    "allow_zero": true,
                    "tiers": build_tier_entries(&tiers, "input_price_per_1m", None, false),
                    "default": base_input_price,
                }),
            );
            dimension_mappings.insert(
                "output_price_per_1m".to_string(),
                json!({
                    "source": "tiered",
                    "tier_key": "total_input_context",
                    "allow_zero": true,
                    "tiers": build_tier_entries(&tiers, "output_price_per_1m", None, false),
                    "default": base_output_price,
                }),
            );
            dimension_mappings.insert(
                "cache_creation_price_per_1m".to_string(),
                json!({
                    "source": "tiered",
                    "tier_key": "total_input_context",
                    "allow_zero": true,
                    "ttl_key": "cache_ttl_minutes",
                    "ttl_value_key": "cache_creation_price_per_1m",
                    "tiers": build_tier_entries(&tiers, "cache_creation_price_per_1m", Some(1.25), true),
                    "default": base_cache_creation_price,
                }),
            );
            dimension_mappings.insert(
                "cache_creation_ephemeral_5m_price_per_1m".to_string(),
                json!({
                    "source": "tiered",
                    "tier_key": "total_input_context",
                    "allow_zero": true,
                    "ttl_key": "cache_creation_ephemeral_5m_ttl_minutes",
                    "ttl_value_key": "cache_creation_price_per_1m",
                    "tiers": build_tier_entries(&tiers, "cache_creation_price_per_1m", Some(1.25), true),
                    "default": base_cache_creation_price,
                }),
            );
            dimension_mappings.insert(
                "cache_creation_ephemeral_1h_price_per_1m".to_string(),
                json!({
                    "source": "tiered",
                    "tier_key": "total_input_context",
                    "allow_zero": true,
                    "ttl_key": "cache_creation_ephemeral_1h_ttl_minutes",
                    "ttl_value_key": "cache_creation_price_per_1m",
                    "tiers": build_tier_entries(&tiers, "cache_creation_price_per_1m", Some(1.25), true),
                    "default": base_cache_creation_price,
                }),
            );
            dimension_mappings.insert(
                "cache_read_price_per_1m".to_string(),
                json!({
                    "source": "tiered",
                    "tier_key": "total_input_context",
                    "allow_zero": true,
                    "ttl_key": "cache_ttl_minutes",
                    "ttl_value_key": "cache_read_price_per_1m",
                    "tiers": build_tier_entries(&tiers, "cache_read_price_per_1m", Some(0.1), true),
                    "default": base_cache_read_price,
                }),
            );
        }

        Some(VirtualBillingRule {
            id: "__default__".to_string(),
            name: format!("Default rule for {}", pricing.global_model_name),
            task_type: normalize_task_type(task_type).to_string(),
            expression: "input_cost + output_cost + cache_creation_uncategorized_cost + cache_creation_ephemeral_5m_cost + cache_creation_ephemeral_1h_cost + cache_read_cost + request_cost".to_string(),
            variables,
            dimension_mappings,
            scope: "default".to_string(),
        })
    }
}

pub fn normalize_task_type(task_type: &str) -> &str {
    if task_type.trim().eq_ignore_ascii_case("cli") {
        "chat"
    } else {
        task_type.trim()
    }
}

fn tier_value(tier: &Value, key: &str, default: f64) -> f64 {
    tier.get(key).and_then(Value::as_f64).unwrap_or(default)
}

fn tier_value_with_fallback(tier: &Value, key: &str, default_multiplier: f64) -> f64 {
    if let Some(value) = tier.get(key).and_then(Value::as_f64) {
        return value;
    }
    tier.get("input_price_per_1m")
        .and_then(Value::as_f64)
        .map(|value| value * default_multiplier)
        .unwrap_or(0.0)
}

fn build_tier_entries(
    tiers: &[Value],
    key: &str,
    default_multiplier: Option<f64>,
    include_cache_ttl_pricing: bool,
) -> Vec<Value> {
    tiers
        .iter()
        .map(|tier| {
            let mut value = serde_json::Map::new();
            value.insert(
                "up_to".to_string(),
                tier.get("up_to").cloned().unwrap_or(Value::Null),
            );
            let resolved = match default_multiplier {
                Some(multiplier) => Value::from(tier_value_with_fallback(tier, key, multiplier)),
                None => Value::from(tier_value(tier, key, 0.0)),
            };
            value.insert("value".to_string(), resolved);
            if include_cache_ttl_pricing {
                if let Some(ttl_pricing) = tier.get("cache_ttl_pricing").cloned() {
                    value.insert("cache_ttl_pricing".to_string(), ttl_pricing);
                }
            }
            Value::Object(value)
        })
        .collect()
}
