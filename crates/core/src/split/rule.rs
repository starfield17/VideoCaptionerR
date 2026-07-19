//! Rule-based splitting following reference-project logic on word indices.

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::transcript::{
    Cue, CueFlags, FieldOrigin, RangeUsize, TimelineSource, Transcript,
};

use crate::constants::{
    MAX_GAP_MS, MAX_WORD_COUNT_CJK, MAX_WORD_COUNT_ENGLISH, MERGE_MIN_WORDS, MERGE_SHORT_GAP_MS,
    MERGE_VERY_SHORT_GAP_MS, MERGE_VERY_SHORT_WORDS, PREFIX_WORD_RATIO, RULE_SPLIT_GAP_MS,
    SUFFIX_WORD_RATIO, TIME_GAP_MULTIPLIER, TIME_GAP_WINDOW_SIZE,
};
use crate::text_joiner::join_word_texts;

/// Configuration for rule splitting. Defaults match Appendix A.
#[derive(Debug, Clone)]
pub struct RuleSplitConfig {
    pub max_words_cjk: usize,
    pub max_words_english: usize,
    pub max_gap_ms: u64,
    pub rule_split_gap_ms: u64,
    pub merge_short_gap_ms: u64,
    pub merge_very_short_gap_ms: u64,
    pub merge_min_words: usize,
    pub merge_very_short_words: usize,
    pub time_gap_window: usize,
    pub time_gap_multiplier: u64,
    pub prefix_word_ratio: f64,
    pub suffix_word_ratio: f64,
    /// When true, treat content as primarily CJK for max-word limits.
    pub prefer_cjk: bool,
}

impl Default for RuleSplitConfig {
    fn default() -> Self {
        Self {
            max_words_cjk: MAX_WORD_COUNT_CJK,
            max_words_english: MAX_WORD_COUNT_ENGLISH,
            max_gap_ms: MAX_GAP_MS,
            rule_split_gap_ms: RULE_SPLIT_GAP_MS,
            merge_short_gap_ms: MERGE_SHORT_GAP_MS,
            merge_very_short_gap_ms: MERGE_VERY_SHORT_GAP_MS,
            merge_min_words: MERGE_MIN_WORDS,
            merge_very_short_words: MERGE_VERY_SHORT_WORDS,
            time_gap_window: TIME_GAP_WINDOW_SIZE,
            time_gap_multiplier: TIME_GAP_MULTIPLIER,
            prefix_word_ratio: PREFIX_WORD_RATIO,
            suffix_word_ratio: SUFFIX_WORD_RATIO,
            prefer_cjk: false,
        }
    }
}

impl RuleSplitConfig {
    pub fn max_words(&self) -> usize {
        if self.prefer_cjk {
            self.max_words_cjk
        } else {
            self.max_words_english
        }
    }

    pub fn detect_cjk_from_words(words: &[videocaptionerr_contracts::transcript::Word]) -> bool {
        let mut cjk = 0usize;
        let mut latin = 0usize;
        for w in words {
            for ch in w.text.chars() {
                if is_cjk_char(ch) {
                    cjk += 1;
                } else if ch.is_ascii_alphabetic() {
                    latin += 1;
                }
            }
        }
        cjk > latin
    }
}

fn is_cjk_char(c: char) -> bool {
    matches!(
        c,
        '\u{3040}'..='\u{30FF}'
            | '\u{3400}'..='\u{4DBF}'
            | '\u{4E00}'..='\u{9FFF}'
            | '\u{F900}'..='\u{FAFF}'
    )
}

/// English-ish function / soft-break words (lowercased match).
fn is_soft_break_word(text: &str) -> bool {
    let t = text.trim().to_ascii_lowercase();
    matches!(
        t.as_str(),
        "and"
            | "or"
            | "but"
            | "so"
            | "because"
            | "when"
            | "while"
            | "although"
            | "though"
            | "if"
            | "that"
            | "which"
            | "who"
            | "where"
            | "after"
            | "before"
            | "as"
            | "than"
            | "then"
            | "also"
            | "however"
            | "therefore"
            | "moreover"
            | "plus"
            | "with"
            | "without"
            | "to"
            | "of"
            | "in"
            | "on"
            | "for"
            | "from"
            | "by"
            | "at"
            | "about"
            | "into"
            | "through"
            | "during"
            | "including"
            | "until"
            | "against"
            | "among"
            | "throughout"
            | "despite"
            | "towards"
            | "upon"
            | "concerning"
            | "的"
            | "了"
            | "和"
            | "与"
            | "及"
            | "而"
            | "或"
            | "但"
            | "因为"
            | "所以"
            | "如果"
            | "虽然"
            | "但是"
            | "然后"
            | "而且"
            | "并且"
            | "以及"
            | "の"
            | "に"
            | "は"
            | "が"
            | "を"
            | "と"
            | "で"
            | "も"
            | "から"
            | "まで"
            | "より"
            | "へ"
    )
}

fn ends_sentence(text: &str) -> bool {
    text.chars()
        .rev()
        .find(|c| !c.is_whitespace())
        .is_some_and(|c| matches!(c, '.' | '!' | '?' | '。' | '！' | '？' | '…'))
}

/// Apply rule splitting to an ASR-derived transcript. Returns a new revision
/// with cues filled from word ranges. Does not mutate `words`.
pub fn rule_split(transcript: &Transcript, cfg: &RuleSplitConfig) -> VcResult<Transcript> {
    if transcript.timeline_source != TimelineSource::AsrWords {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "rule_split requires AsrWords timeline",
        ));
    }
    if transcript.words.is_empty() {
        let mut out = transcript.clone();
        out.cues.clear();
        out.next_cue_id = 1;
        out.bump_revision();
        return Ok(out);
    }

    let mut cfg = cfg.clone();
    if !cfg.prefer_cjk {
        cfg.prefer_cjk = RuleSplitConfig::detect_cjk_from_words(&transcript.words);
    }

    let n = transcript.words.len();
    let gaps: Vec<u64> = (0..n.saturating_sub(1))
        .map(|i| {
            let a = &transcript.words[i];
            let b = &transcript.words[i + 1];
            b.start_ms.saturating_sub(a.end_ms)
        })
        .collect();

    // 1) primary splits on large gaps / sentence enders.
    let mut cuts: Vec<usize> = Vec::new(); // cut AFTER word index (exclusive end)
    let mut start = 0usize;
    for i in 0..n {
        let is_last = i + 1 == n;
        let gap_cut = !is_last && gaps[i] >= cfg.rule_split_gap_ms;
        let max_gap_cut = !is_last && gaps[i] >= cfg.max_gap_ms;
        let sentence_cut = ends_sentence(&transcript.words[i].text);
        // Unusual gap in moving window.
        let unusual =
            !is_last && unusual_gap(&gaps, i, cfg.time_gap_window, cfg.time_gap_multiplier);

        if is_last || gap_cut || max_gap_cut || sentence_cut || unusual {
            let end = i + 1;
            if end > start {
                cuts.push(end);
                start = end;
            }
        }
    }
    if cuts.last().copied() != Some(n) {
        cuts.push(n);
    }

    // Expand cuts into ranges, then split oversized groups.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut prev = 0usize;
    for &end in &cuts {
        if end <= prev {
            continue;
        }
        split_oversized(prev, end, &transcript.words, &cfg, &mut ranges);
        prev = end;
    }

    // Merge very short neighboring groups under gap rules.
    let ranges = merge_short_ranges(&ranges, &transcript.words, &cfg);

    let mut out = transcript.clone();
    out.cues.clear();
    out.next_cue_id = 1;
    for (s, e) in ranges {
        if s >= e {
            continue;
        }
        let id = out.allocate_cue_id();
        let text = join_word_texts(&out.words[s..e]);
        out.cues.push(Cue {
            id,
            word_range: Some(RangeUsize::new(s, e)),
            imported_start_ms: None,
            imported_end_ms: None,
            text,
            translation: None,
            flags: CueFlags::default(),
            text_origin: Some(FieldOrigin::RuleSplit),
            translation_origin: None,
            text_revision: 0,
            translation_revision: 0,
        });
    }
    out.bump_revision();
    out.validate()?;
    Ok(out)
}

fn unusual_gap(gaps: &[u64], i: usize, window: usize, mult: u64) -> bool {
    if gaps.is_empty() || window == 0 {
        return false;
    }
    let start = i.saturating_sub(window);
    let end = (i + window + 1).min(gaps.len());
    if end <= start {
        return false;
    }
    let slice = &gaps[start..end];
    let sum: u64 = slice.iter().sum();
    let avg = sum / slice.len() as u64;
    avg > 0 && gaps[i] >= avg.saturating_mul(mult) && gaps[i] >= 200
}

fn split_oversized(
    start: usize,
    end: usize,
    words: &[videocaptionerr_contracts::transcript::Word],
    cfg: &RuleSplitConfig,
    out: &mut Vec<(usize, usize)>,
) {
    let max_w = cfg.max_words().max(1);
    let len = end - start;
    if len <= max_w {
        out.push((start, end));
        return;
    }

    // Prefer soft-break near preferred ratio region.
    let prefer_lo = start + ((len as f64 * cfg.suffix_word_ratio) as usize).max(1);
    let prefer_hi = start + ((len as f64 * cfg.prefix_word_ratio) as usize).min(len - 1);
    let prefer_lo = prefer_lo.clamp(start + 1, end - 1);
    let prefer_hi = prefer_hi.clamp(prefer_lo, end - 1);

    let mut best: Option<usize> = None;
    // Search soft-break words inside preferred window, then whole span.
    for (range_lo, range_hi) in [(prefer_lo, prefer_hi), (start + 1, end - 1)] {
        if range_lo >= range_hi {
            continue;
        }
        let mid = (range_lo + range_hi) / 2;
        let mut best_dist = usize::MAX;
        for (i, word) in words.iter().enumerate().take(range_hi + 1).skip(range_lo) {
            if is_soft_break_word(&word.text) {
                let dist = i.abs_diff(mid);
                if dist < best_dist {
                    best_dist = dist;
                    best = Some(i);
                }
            }
        }
        if best.is_some() {
            break;
        }
    }

    let cut = best.unwrap_or_else(|| {
        // Force near preferred ratio.
        let forced = start + ((len as f64 * cfg.prefix_word_ratio) as usize).max(1);
        forced.clamp(start + 1, end - 1)
    });

    // Soft-break word starts the next cue (cut before it) when it is a conjunction.
    let cut_end = if is_soft_break_word(&words[cut].text) {
        cut
    } else {
        cut + 1
    };
    let cut_end = cut_end.clamp(start + 1, end - 1);

    split_oversized(start, cut_end, words, cfg, out);
    split_oversized(cut_end, end, words, cfg, out);
}

fn merge_short_ranges(
    ranges: &[(usize, usize)],
    words: &[videocaptionerr_contracts::transcript::Word],
    cfg: &RuleSplitConfig,
) -> Vec<(usize, usize)> {
    if ranges.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<(usize, usize)> = vec![ranges[0]];
    for &(s, e) in &ranges[1..] {
        let (ps, pe) = *out.last().unwrap();
        let prev_len = pe - ps;
        let cur_len = e - s;
        let gap = if pe > 0 && pe <= words.len() && s < words.len() {
            words[s].start_ms.saturating_sub(words[pe - 1].end_ms)
        } else {
            u64::MAX
        };

        let very_short =
            prev_len <= cfg.merge_very_short_words || cur_len <= cfg.merge_very_short_words;
        let short = prev_len < cfg.merge_min_words || cur_len < cfg.merge_min_words;
        let can_merge = (very_short && gap <= cfg.merge_very_short_gap_ms)
            || (short && gap <= cfg.merge_short_gap_ms);

        let merged_len = e - ps;
        if can_merge && merged_len <= cfg.max_words().saturating_mul(2) {
            let last = out.last_mut().unwrap();
            last.1 = e;
        } else {
            out.push((s, e));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use videocaptionerr_contracts::transcript::{EngineFingerprint, Word, PROB_UNAVAILABLE};
    use videocaptionerr_test_support::sample_transcript;

    fn words_from(pairs: &[(&str, u64, u64)]) -> Vec<Word> {
        pairs
            .iter()
            .map(|(t, s, e)| Word {
                text: (*t).into(),
                start_ms: *s,
                end_ms: *e,
                prob: 0.9,
            })
            .collect()
    }

    #[test]
    fn splits_on_large_gap() {
        let words = words_from(&[
            ("hello", 0, 200),
            ("there", 210, 400),
            ("goodbye", 2000, 2300),
            ("friend", 2310, 2600),
        ]);
        let t = Transcript::new_asr("h", EngineFingerprint::unknown(), words);
        let out = rule_split(&t, &RuleSplitConfig::default()).unwrap();
        assert!(out.cues.len() >= 2);
        assert_eq!(out.cues[0].word_range.unwrap().end, 2);
        out.validate().unwrap();
    }

    #[test]
    fn produces_non_overlapping_ranges() {
        let mut t = sample_transcript();
        // stretch gaps
        t.words.push(Word {
            text: "again".into(),
            start_ms: 3000,
            end_ms: 3200,
            prob: PROB_UNAVAILABLE,
        });
        let out = rule_split(&t, &RuleSplitConfig::default()).unwrap();
        out.validate().unwrap();
        assert!(!out.cues.is_empty());
        // words immutable
        assert_eq!(out.words, t.words);
    }

    #[test]
    fn oversized_group_is_force_split() {
        let mut pairs = Vec::new();
        for i in 0..40 {
            let start = i * 100;
            pairs.push((if i % 5 == 0 { "and" } else { "word" }, start, start + 80));
        }
        // Convert to owned strings via words_from pattern
        let words: Vec<Word> = pairs
            .iter()
            .map(|(t, s, e)| Word {
                text: (*t).into(),
                start_ms: *s,
                end_ms: *e,
                prob: 0.9,
            })
            .collect();
        let t = Transcript::new_asr("h", EngineFingerprint::unknown(), words);
        let cfg = RuleSplitConfig {
            prefer_cjk: false,
            ..Default::default()
        };
        let out = rule_split(&t, &cfg).unwrap();
        assert!(out.cues.len() > 1);
        for c in &out.cues {
            let r = c.word_range.unwrap();
            assert!(r.len() <= cfg.max_words_english + 5); // soft bound with merge
        }
        out.validate().unwrap();
    }

    #[test]
    fn empty_words_ok() {
        let t = Transcript::new_asr("h", EngineFingerprint::unknown(), vec![]);
        let out = rule_split(&t, &RuleSplitConfig::default()).unwrap();
        assert!(out.cues.is_empty());
    }
}
