//! Conservative token estimation without an official tokenizer.

/// Default chars-per-token for packing when no tokenizer is available.
pub const DEFAULT_CHARS_PER_TOKEN: f64 = 3.5;

/// Safety margin applied on top of estimated input + reserved output.
pub const TOKEN_SAFETY_MARGIN: f64 = 1.15;

/// Estimate tokens from text using a configurable chars-per-token ratio.
pub fn estimate_tokens(text: &str, chars_per_token: f64) -> u32 {
    let cpt = if chars_per_token <= 0.0 {
        DEFAULT_CHARS_PER_TOKEN
    } else {
        chars_per_token
    };
    let chars = text.chars().count() as f64;
    ((chars / cpt).ceil() as u32).max(1)
}

/// Estimate tokens for a list of message contents.
pub fn estimate_messages_tokens<'a>(
    messages: impl IntoIterator<Item = &'a str>,
    chars_per_token: f64,
) -> u32 {
    messages
        .into_iter()
        .map(|m| estimate_tokens(m, chars_per_token))
        .sum::<u32>()
        // small overhead per message
        .saturating_add(4)
}

/// Check whether estimated input + reserved output + margin fits the context.
pub fn fits_context(
    estimated_input: u32,
    reserved_output: u32,
    max_context: u32,
    safety_margin: f64,
) -> bool {
    let needed = ((estimated_input as f64 + reserved_output as f64) * safety_margin).ceil() as u64;
    needed <= max_context as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_grows_with_text() {
        let short = estimate_tokens("hi", DEFAULT_CHARS_PER_TOKEN);
        let long = estimate_tokens(&"word ".repeat(200), DEFAULT_CHARS_PER_TOKEN);
        assert!(long > short);
    }

    #[test]
    fn fits_context_respects_margin() {
        assert!(fits_context(100, 50, 200, 1.0));
        assert!(!fits_context(100, 50, 150, TOKEN_SAFETY_MARGIN));
    }
}
