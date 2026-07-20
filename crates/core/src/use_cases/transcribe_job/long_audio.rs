use super::*;

pub(super) fn append_chunk_words(
    words: &mut Vec<videocaptionerr_domain::Word>,
    language: &mut Option<String>,
    engine: &mut EngineFingerprint,
    raw: Transcript,
    chunk: crate::chunking::AudioChunk,
) -> AppResult<()> {
    if engine.engine_id == "unknown" {
        *engine = raw.engine.clone();
    }
    if language.is_none() {
        *language = raw.language.clone();
    }
    let shifted = apply_chunk_offset(&raw.words, chunk.read_start_ms);
    words.extend(retain_core_words(&shifted, chunk));
    Ok(())
}
