//! Language-aware text assembly. Do not use `join(" ")` as the universal strategy.

use unicode_normalization::UnicodeNormalization;

/// Assemble words into display text with CJK/Latin-aware spacing.
///
/// Rules (v1):
/// - NFC-normalize each token before joining;
/// - no space between normal CJK characters;
/// - natural spaces between Latin/numeric words;
/// - punctuation attaches by Unicode category (no leading space for closers);
/// - mixed CJK/Latin boundaries get a space only when both sides are "word-like"
///   and at least one side is Latin/numeric.
pub fn join_words(words: &[impl AsRef<str>]) -> String {
    if words.is_empty() {
        return String::new();
    }

    let tokens: Vec<String> = words
        .iter()
        .map(|w| w.as_ref().nfc().collect::<String>())
        .filter(|t| !t.is_empty())
        .collect();

    if tokens.is_empty() {
        return String::new();
    }

    let mut out = String::with_capacity(tokens.iter().map(|t| t.len() + 1).sum());
    out.push_str(&tokens[0]);

    for tok in tokens.iter().skip(1) {
        if needs_space(out.chars().next_back(), tok.chars().next()) {
            out.push(' ');
        }
        out.push_str(tok);
    }
    out
}

/// Join a slice of IR words by their `.text` field.
pub fn join_word_texts(words: &[super::transcript::Word]) -> String {
    join_words(&words.iter().map(|w| w.text.as_str()).collect::<Vec<_>>())
}

fn needs_space(prev: Option<char>, next: Option<char>) -> bool {
    let (Some(p), Some(n)) = (prev, next) else {
        return false;
    };

    // Never space before closing/terminator punctuation.
    if is_closing_punct(n) || is_connector(n) {
        return false;
    }
    // Never space after opening punctuation.
    if is_opening_punct(p) {
        return false;
    }
    // No space after CJK punctuation that already includes spacing semantics.
    if is_cjk_punct(p) {
        return false;
    }
    // Apostrophe/hyphen connectors glue.
    if is_connector(p) {
        return false;
    }

    let p_cjk = is_cjk(p);
    let n_cjk = is_cjk(n);
    let p_word = is_word_char(p);
    let n_word = is_word_char(n);

    if p_cjk && n_cjk {
        return false;
    }
    if p_word && n_word {
        // Latin-Latin, Latin-digit, CJK-Latin, Latin-CJK all take a space
        // except pure CJK-CJK (handled above).
        return true;
    }
    // Word then opening punct: "word ("
    if p_word && is_opening_punct(n) {
        return true;
    }
    // Closing punct then word: ") word" / "。word" — CJK closers usually no space.
    // Decimal point between digits must not insert a space: "3" "." "14" -> "3.14".
    if is_closing_punct(p) && n_word {
        if p == '.' && n.is_ascii_digit() {
            return false;
        }
        return !is_cjk_punct(p);
    }
    false
}

fn is_cjk(c: char) -> bool {
    matches!(
        c,
        '\u{3040}'..='\u{30FF}'   // Hiragana + Katakana
        | '\u{3400}'..='\u{4DBF}' // CJK ext A
        | '\u{4E00}'..='\u{9FFF}' // CJK Unified
        | '\u{F900}'..='\u{FAFF}' // CJK Compatibility
        | '\u{FF66}'..='\u{FF9D}' // Halfwidth katakana
        | '\u{3000}'..='\u{303F}' // CJK symbols/punct (treated as CJK context)
    )
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || is_cjk(c) || c == '%' || c == '$' || c == '€' || c == '£'
}

fn is_connector(c: char) -> bool {
    matches!(c, '\'' | '’' | '-' | '‑' | '–' | '/')
}

fn is_opening_punct(c: char) -> bool {
    matches!(
        c,
        '(' | '[' | '{' | '“' | '‘' | '「' | '『' | '（' | '【' | '《' | '〈' | '"' | '\''
    )
}

fn is_closing_punct(c: char) -> bool {
    matches!(
        c,
        '.' | ','
            | '!'
            | '?'
            | ';'
            | ':'
            | ')'
            | ']'
            | '}'
            | '”'
            | '’'
            | '」'
            | '』'
            | '）'
            | '】'
            | '》'
            | '〉'
            | '。'
            | '、'
            | '！'
            | '？'
            | '；'
            | '：'
            | '%'
            | '"'
            | '\''
    ) || is_cjk_punct(c)
}

fn is_cjk_punct(c: char) -> bool {
    matches!(
        c,
        '。' | '、'
            | '！'
            | '？'
            | '；'
            | '：'
            | '「'
            | '」'
            | '『'
            | '』'
            | '（'
            | '）'
            | '【'
            | '】'
            | '《'
            | '》'
            | '〈'
            | '〉'
            | '・'
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn latin_words_spaced() {
        assert_eq!(join_words(&["hello", "world"]), "hello world");
    }

    #[test]
    fn cjk_no_spaces() {
        assert_eq!(join_words(&["你", "好", "世界"]), "你好世界");
    }

    #[test]
    fn mixed_cjk_latin() {
        assert_eq!(join_words(&["使用", "Rust", "语言"]), "使用 Rust 语言");
    }

    #[test]
    fn punctuation_attachment() {
        assert_eq!(join_words(&["Hello", ",", "world", "!"]), "Hello, world!");
        assert_eq!(join_words(&["你好", "。"]), "你好。");
    }

    #[test]
    fn apostrophe_and_hyphen() {
        assert_eq!(join_words(&["it", "'", "s"]), "it's");
        assert_eq!(
            join_words(&["state", "-", "of", "-", "art"]),
            "state-of-art"
        );
    }

    #[test]
    fn decimals_and_percent() {
        assert_eq!(join_words(&["3", ".", "14"]), "3.14");
        assert_eq!(join_words(&["50", "%"]), "50%");
    }

    #[test]
    fn nfc_normalization() {
        // e + combining acute vs precomposed
        let a = join_words(&["cafe\u{0301}"]);
        let b = join_words(&["café"]);
        assert_eq!(a, b);
    }
}
