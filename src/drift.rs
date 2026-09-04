//! coinset.org API drift detection.
//!
//! coinset.org publishes no OpenAPI schema, so the only way to notice a
//! breaking API change is to watch it. This module reduces a live JSON
//! response to its *shape* — the set of keys and the JSON type of each value,
//! with all concrete values discarded — and diffs that shape against a
//! committed baseline snapshot.
//!
//! Values are dropped on purpose: a block's `difficulty` or a mempool's
//! `size` changes every block, but its *shape* (the key `difficulty` holding
//! an integer) is the actual contract chia-query depends on. Drift in the
//! shape — a renamed key, a removed field, a type flip — is what breaks a
//! consumer, and is what this detects.

use serde_json::{Map, Value};

/// Reduce a JSON value to its type-shape: objects keep their keys (each mapped
/// to the shape of its value), arrays collapse to a single-element shape, and
/// every scalar becomes a type tag (`"string"`, `"integer"`, `"number"`,
/// `"boolean"`, `"null"`).
///
/// The result is itself JSON so it serializes cleanly into the committed
/// snapshot and diffs with the same machinery.
///
/// # Known false positive: `integer` vs `number` on a numerically variable field
///
/// The `integer`/`number` split is decided by `is_i64() || is_u64()`, which is a
/// property of the VALUE that arrived, not of the API's contract. A field whose
/// value is legitimately sometimes whole and sometimes fractional therefore flips
/// tag with conditions and reports drift that is not drift.
///
/// Observed on `get_blockchain_state.blockchain_state.mempool_min_fees.cost_5000000`
/// (a FEE, so whole whenever the mempool is uncongested): reported as
/// `integer -> number` on 2026-08-28 and back to `integer` days later, with no API
/// change either time. It was harmless in that instance because `mempool_min_fees`
/// is typed `serde_json::Value` (`crate::types::response`), so neither form could
/// fail to deserialize.
///
/// This is a value leaking into the shape, which is the one thing this module
/// otherwise avoids by design. It is left in place deliberately: collapsing the two
/// tags would also stop detecting a genuine `integer -> number` change on a field
/// that IS decoded into an integer type, which is a real break. So the tag stays
/// strict and the false positive is documented instead. Before acting on such a
/// drift report, check whether the field is decoded into a numeric Rust type at all
/// -- if it is a `serde_json::Value`, the report is noise.
pub fn shape_of(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let shaped: Map<String, Value> = map
                .iter()
                .map(|(key, val)| (key.clone(), shape_of(val)))
                .collect();
            Value::Object(shaped)
        }
        // An array's shape is the shape of its elements. We assume homogeneity
        // (true for every coinset list endpoint) and sample the first element;
        // an empty array carries no element shape.
        Value::Array(items) => match items.first() {
            Some(first) => Value::Array(vec![shape_of(first)]),
            None => Value::Array(vec![]),
        },
        Value::String(_) => Value::String("string".into()),
        Value::Number(n) => {
            let tag = if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            };
            Value::String(tag.into())
        }
        Value::Bool(_) => Value::String("boolean".into()),
        Value::Null => Value::String("null".into()),
    }
}

/// Compare a `current` shape against the `baseline` shape and return one
/// human-readable line per drift found. An empty vec means the shapes match.
///
/// `path` is the dotted JSON path used to locate each drift (e.g.
/// `blockchain_state.difficulty`); callers pass the endpoint name as the root.
pub fn diff_shapes(path: &str, baseline: &Value, current: &Value) -> Vec<String> {
    match (baseline, current) {
        (Value::Object(base), Value::Object(cur)) => diff_objects(path, base, cur),
        (Value::Array(base), Value::Array(cur)) => match (base.first(), cur.first()) {
            (Some(b), Some(c)) => diff_shapes(&format!("{path}[]"), b, c),
            // One side is an empty array: no element shape to compare. This is
            // not drift — an endpoint can legitimately return an empty list.
            _ => Vec::new(),
        },
        (Value::String(base_tag), Value::String(cur_tag)) if base_tag != cur_tag => {
            vec![format!("{path}: type changed {base_tag} -> {cur_tag}")]
        }
        (Value::String(_), Value::String(_)) => Vec::new(),
        _ => vec![format!(
            "{path}: structure changed {} -> {}",
            kind(baseline),
            kind(current)
        )],
    }
}

/// Diff two shaped objects: report keys that vanished, keys that appeared, and
/// recurse into keys present in both.
fn diff_objects(path: &str, base: &Map<String, Value>, cur: &Map<String, Value>) -> Vec<String> {
    let mut drifts = Vec::new();

    for (key, base_val) in base {
        let child = join(path, key);
        match cur.get(key) {
            Some(cur_val) => drifts.extend(diff_shapes(&child, base_val, cur_val)),
            None => drifts.push(format!("{child}: key removed")),
        }
    }
    for key in cur.keys() {
        if !base.contains_key(key) {
            drifts.push(format!("{}: key added", join(path, key)));
        }
    }

    drifts
}

/// The coarse structural kind of a shape node, for drift messages.
fn kind(value: &Value) -> &'static str {
    match value {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        _ => "scalar",
    }
}

/// Join a dotted JSON path with a child key, handling the empty root.
fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shape_discards_scalar_values_but_keeps_keys_and_types() {
        let response = json!({
            "difficulty": 2656,
            "average_block_time": 19.5,
            "network_name": "mainnet",
            "initialized": true,
            "node_id": null
        });
        assert_eq!(
            shape_of(&response),
            json!({
                "difficulty": "integer",
                "average_block_time": "number",
                "network_name": "string",
                "initialized": "boolean",
                "node_id": "null"
            })
        );
    }

    #[test]
    fn shape_samples_array_element() {
        let response = json!({ "coin_records": [{ "amount": 100 }, { "amount": 200 }] });
        assert_eq!(
            shape_of(&response),
            json!({ "coin_records": [{ "amount": "integer" }] })
        );
    }

    #[test]
    fn identical_shapes_show_no_drift() {
        let a = shape_of(&json!({ "blockchain_state": { "difficulty": 100 } }));
        let b = shape_of(&json!({ "blockchain_state": { "difficulty": 999 } }));
        assert!(diff_shapes("get_blockchain_state", &a, &b).is_empty());
    }

    #[test]
    fn detects_removed_key() {
        let base = shape_of(&json!({ "a": 1, "b": 2 }));
        let cur = shape_of(&json!({ "a": 1 }));
        let drift = diff_shapes("ep", &base, &cur);
        assert_eq!(drift, vec!["ep.b: key removed"]);
    }

    #[test]
    fn detects_added_key() {
        let base = shape_of(&json!({ "a": 1 }));
        let cur = shape_of(&json!({ "a": 1, "c": 3 }));
        let drift = diff_shapes("ep", &base, &cur);
        assert_eq!(drift, vec!["ep.c: key added"]);
    }

    #[test]
    fn detects_type_flip() {
        let base = shape_of(&json!({ "amount": 100 }));
        let cur = shape_of(&json!({ "amount": "100" }));
        let drift = diff_shapes("ep", &base, &cur);
        assert_eq!(drift, vec!["ep.amount: type changed integer -> string"]);
    }

    #[test]
    fn detects_structure_change() {
        let base = shape_of(&json!({ "x": { "y": 1 } }));
        let cur = shape_of(&json!({ "x": 1 }));
        let drift = diff_shapes("ep", &base, &cur);
        assert_eq!(drift, vec!["ep.x: structure changed object -> scalar"]);
    }

    #[test]
    fn recurses_into_arrays() {
        let base = shape_of(&json!({ "items": [{ "amount": 1 }] }));
        let cur = shape_of(&json!({ "items": [{ "amount": "1" }] }));
        let drift = diff_shapes("ep", &base, &cur);
        assert_eq!(
            drift,
            vec!["ep.items[].amount: type changed integer -> string"]
        );
    }

    #[test]
    fn empty_array_is_not_drift() {
        let base = shape_of(&json!({ "items": [{ "amount": 1 }] }));
        let cur = shape_of(&json!({ "items": [] }));
        assert!(diff_shapes("ep", &base, &cur).is_empty());
    }

    /// A deliberately-mutated snapshot MUST be detected as drift — the guard
    /// the CI drift-monitor relies on (unit 2 acceptance).
    #[test]
    fn mutated_snapshot_is_detected() {
        let live = shape_of(&json!({
            "get_network_info": { "network_name": "mainnet", "network_prefix": "xch", "success": true }
        }));
        let mut mutated = live.clone();
        // coinset renames a field: network_prefix -> address_prefix.
        let obj = mutated["get_network_info"].as_object_mut().unwrap();
        let tag = obj.remove("network_prefix").unwrap();
        obj.insert("address_prefix".into(), tag);

        let drift = diff_shapes("", &live, &mutated);
        assert!(!drift.is_empty());
        assert!(drift
            .iter()
            .any(|d| d.contains("network_prefix: key removed")));
        assert!(drift
            .iter()
            .any(|d| d.contains("address_prefix: key added")));
    }
}
