//! Split-stage word-range mapping helpers.
use std::collections::BTreeSet;

use videocaptionerr_domain::{Cue, RangeUsize, Transcript};

pub(crate) fn split_ranges_for_formatted(
    transcript: &Transcript,
    cue: &Cue,
    formatted: &str,
) -> Result<Vec<RangeUsize>, String> {
    let range = cue
        .word_range
        .ok_or_else(|| format!("cue {} has no word range", cue.id))?;
    let words = &transcript.words[range.start..range.end];
    let original = videocaptionerr_domain::join_words(
        &words
            .iter()
            .map(|word| word.text.as_str())
            .collect::<Vec<_>>(),
    );
    let (clean, breaks) = remove_break_markers(formatted);
    if clean != original && restore_split_spaces(&clean, &breaks) != original {
        return Err(format!("cue {} changed content while splitting", cue.id));
    }
    let boundaries = (1..words.len())
        .map(|end| {
            videocaptionerr_domain::join_words(
                &words[..end]
                    .iter()
                    .map(|word| word.text.as_str())
                    .collect::<Vec<_>>(),
            )
            .chars()
            .count()
        })
        .collect::<BTreeSet<_>>();
    let clean_chars = clean.chars().collect::<Vec<_>>();
    for position in &breaks {
        let boundary = normalized_break_position(*position, &clean_chars, &boundaries);
        if boundary.is_none() {
            return Err(format!(
                "cue {} break does not align to a word boundary",
                cue.id
            ));
        }
    }
    let mut ranges = Vec::new();
    let mut start = range.start;
    for position in breaks {
        let boundary = normalized_break_position(position, &clean_chars, &boundaries)
            .ok_or_else(|| "break offset is not a word boundary".to_string())?;
        let local_end = boundary_index(words, boundary)?;
        ranges.push(RangeUsize::new(start, range.start + local_end));
        start = range.start + local_end;
    }
    ranges.push(RangeUsize::new(start, range.end));
    Ok(ranges)
}

pub(crate) fn boundary_index(
    words: &[videocaptionerr_domain::Word],
    chars: usize,
) -> Result<usize, String> {
    for end in 1..words.len() {
        let text = videocaptionerr_domain::join_words(
            &words[..end]
                .iter()
                .map(|word| word.text.as_str())
                .collect::<Vec<_>>(),
        );
        if text.chars().count() == chars {
            return Ok(end);
        }
    }
    Err("break offset is not a word boundary".into())
}

pub(crate) fn remove_break_markers(text: &str) -> (String, Vec<usize>) {
    let mut clean = String::new();
    let mut breaks = Vec::new();
    let mut rest = text;
    while let Some(index) = rest.find("<br>") {
        clean.push_str(&rest[..index]);
        breaks.push(clean.chars().count());
        rest = &rest[index + 4..];
    }
    clean.push_str(rest);
    (clean, breaks)
}

pub(crate) fn restore_split_spaces(clean: &str, breaks: &[usize]) -> String {
    let chars = clean.chars().collect::<Vec<_>>();
    let mut out = String::new();
    for (index, ch) in chars.iter().enumerate() {
        if breaks.contains(&index) && chars.get(index.wrapping_sub(1)) != Some(&' ') {
            out.push(' ');
        }
        out.push(*ch);
    }
    out
}

pub(crate) fn normalized_break_position(
    position: usize,
    clean: &[char],
    boundaries: &BTreeSet<usize>,
) -> Option<usize> {
    if boundaries.contains(&position) {
        return Some(position);
    }
    if position > 0
        && clean
            .get(position - 1)
            .is_some_and(|character| character.is_whitespace())
    {
        let before_space = position - 1;
        if boundaries.contains(&before_space) {
            return Some(before_space);
        }
    }
    None
}
