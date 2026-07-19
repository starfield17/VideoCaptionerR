//! Tolerant JSON extraction from model responses.

use serde::de::DeserializeOwned;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

/// Extract a JSON value from model text that may include fences or prose.
pub fn extract_json_value(text: &str) -> VcResult<serde_json::Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(VcError::new(
            ErrorCode::LlmInvalidResponse,
            "empty model response",
        ));
    }

    // Direct parse.
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return Ok(v);
    }

    // Fenced ```json ... ```
    if let Some(inner) = extract_fenced(trimmed) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(inner.trim()) {
            return Ok(v);
        }
    }

    // First balanced object or array.
    if let Some(slice) = find_balanced_json(trimmed) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(slice) {
            return Ok(v);
        }
    }

    Err(VcError::new(
        ErrorCode::LlmInvalidResponse,
        "could not parse JSON from model response",
    )
    .with_detail(trimmed.chars().take(200).collect::<String>()))
}

/// Deserialize typed value via tolerant extraction.
pub fn parse_json<T: DeserializeOwned>(text: &str) -> VcResult<T> {
    let value = extract_json_value(text)?;
    serde_json::from_value(value).map_err(|e| {
        VcError::new(
            ErrorCode::LlmInvalidResponse,
            format!("JSON shape mismatch: {e}"),
        )
    })
}

fn extract_fenced(text: &str) -> Option<&str> {
    let start_marker = if let Some(i) = text.find("```json") {
        i + "```json".len()
    } else if let Some(i) = text.find("```") {
        i + 3
    } else {
        return None;
    };
    let rest = &text[start_marker..];
    let end = rest.find("```")?;
    Some(&rest[..end])
}

fn find_balanced_json(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut start = None;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut opener = b'\0';

    for (i, &b) in bytes.iter().enumerate() {
        if in_string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                if depth == 0 {
                    start = Some(i);
                    opener = b;
                }
                depth += 1;
            }
            b'}' | b']' => {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        let s = start?;
                        let closer_ok = (opener == b'{' && b == b'}')
                            || (opener == b'[' && b == b']');
                        if closer_ok {
                            return Some(&text[s..=i]);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fenced_and_prose() {
        let v = extract_json_value("Here you go:\n```json\n{\"a\":1}\n```\n").unwrap();
        assert_eq!(v["a"], 1);
        let v = extract_json_value("prefix {\"items\":[]} suffix").unwrap();
        assert!(v["items"].is_array());
    }
}
