//! Codec between the S3 tag set and its stored JSON document.

pub(crate) fn tags_to_json(tagging: s3s::dto::Tagging) -> serde_json::Value {
    let list: Vec<serde_json::Value> = tagging
        .tag_set
        .into_iter()
        .map(|tag| {
            serde_json::json!({
                "key": tag.key.unwrap_or_default(),
                "value": tag.value.unwrap_or_default(),
            })
        })
        .collect();

    serde_json::Value::Array(list)
}

pub(crate) fn json_to_tag_set(value: &serde_json::Value) -> Vec<s3s::dto::Tag> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let key = entry.get("key")?.as_str()?.to_string();
                    let tag_value = entry.get("value")?.as_str()?.to_string();

                    Some(s3s::dto::Tag {
                        key: Some(key),
                        value: Some(tag_value),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}
