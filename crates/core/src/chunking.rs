//! Long-audio chunk planning and core/read ownership.
//!
//! This module deliberately accepts VAD/silence and energy observations from
//! an adapter. It does not read audio or perform text-based deduplication.

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_domain::Word;

use crate::application_error::{AppResult, ApplicationError};
use crate::constants::{
    CHUNK_CONTEXT_PADDING_SECS, CHUNK_SEARCH_RADIUS_SECS, GLOBAL_MAX_CHUNK_SECS,
    MINIMUM_CHUNK_SECS, VAD_MIN_SILENCE_MS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutReason {
    SingleChunk,
    Silence,
    LowEnergy,
    Forced,
    EndOfAudio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioChunk {
    pub index: u32,
    pub core_start_ms: u64,
    pub core_end_ms: u64,
    pub read_start_ms: u64,
    pub read_end_ms: u64,
    pub cut_reason: CutReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SilenceRegion {
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EnergySample {
    pub at_ms: u64,
    pub energy: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPlanOptions {
    pub search_radius_ms: u64,
    pub context_padding_ms: u64,
    pub min_chunk_ms: u64,
    pub max_chunk_ms: u64,
    pub min_silence_ms: u64,
}

impl Default for ChunkPlanOptions {
    fn default() -> Self {
        Self {
            search_radius_ms: CHUNK_SEARCH_RADIUS_SECS * 1000,
            context_padding_ms: (CHUNK_CONTEXT_PADDING_SECS * 1000.0) as u64,
            min_chunk_ms: MINIMUM_CHUNK_SECS * 1000,
            max_chunk_ms: GLOBAL_MAX_CHUNK_SECS * 1000,
            min_silence_ms: VAD_MIN_SILENCE_MS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPlan {
    pub schema_version: u32,
    pub duration_ms: u64,
    pub options: ChunkPlanOptions,
    pub chunks: Vec<AudioChunk>,
    pub plan_hash: String,
}

impl ChunkPlan {
    pub fn build(
        duration_ms: u64,
        silences: &[SilenceRegion],
        energy: &[EnergySample],
        options: ChunkPlanOptions,
    ) -> AppResult<Self> {
        validate_options(duration_ms, options)?;
        if duration_ms == 0 {
            return Ok(Self::from_chunks(duration_ms, options, Vec::new()));
        }
        if duration_ms <= options.max_chunk_ms {
            return Ok(Self::from_chunks(
                duration_ms,
                options,
                vec![AudioChunk {
                    index: 0,
                    core_start_ms: 0,
                    core_end_ms: duration_ms,
                    read_start_ms: 0,
                    read_end_ms: duration_ms,
                    cut_reason: CutReason::SingleChunk,
                }],
            ));
        }

        let mut chunks = Vec::new();
        let mut core_start_ms = 0;
        while core_start_ms < duration_ms {
            let remaining = duration_ms - core_start_ms;
            if remaining <= options.max_chunk_ms {
                chunks.push(make_chunk(
                    chunks.len() as u32,
                    core_start_ms,
                    duration_ms,
                    options.context_padding_ms,
                    CutReason::EndOfAudio,
                    duration_ms,
                ));
                break;
            }

            let target = core_start_ms + options.max_chunk_ms;
            let lower = target
                .saturating_sub(options.search_radius_ms)
                .max(core_start_ms + options.min_chunk_ms);
            let upper = target
                .saturating_add(options.search_radius_ms)
                .min(duration_ms - options.min_chunk_ms);
            let (cut, reason) = choose_cut(target, lower, upper, silences, energy, options);
            let cut = cut.max(core_start_ms + options.min_chunk_ms);
            if cut >= duration_ms {
                chunks.push(make_chunk(
                    chunks.len() as u32,
                    core_start_ms,
                    duration_ms,
                    options.context_padding_ms,
                    CutReason::EndOfAudio,
                    duration_ms,
                ));
                break;
            }
            chunks.push(make_chunk(
                chunks.len() as u32,
                core_start_ms,
                cut,
                options.context_padding_ms,
                reason,
                duration_ms,
            ));
            core_start_ms = cut;
        }

        let plan = Self::from_chunks(duration_ms, options, chunks);
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> AppResult<()> {
        validate_options(self.duration_ms, self.options)?;
        let mut expected_start = 0;
        for (index, chunk) in self.chunks.iter().enumerate() {
            if chunk.index as usize != index
                || chunk.core_start_ms != expected_start
                || chunk.core_start_ms >= chunk.core_end_ms
                || chunk.core_end_ms > self.duration_ms
                || chunk.read_start_ms > chunk.core_start_ms
                || chunk.read_end_ms < chunk.core_end_ms
                || chunk.read_end_ms > self.duration_ms
            {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::InvalidArgument,
                    "ChunkPlan has invalid ownership or read bounds",
                )));
            }
            expected_start = chunk.core_end_ms;
        }
        if expected_start != self.duration_ms && self.duration_ms != 0 {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::InvalidArgument,
                "ChunkPlan core ranges do not cover audio duration",
            )));
        }
        Ok(())
    }

    fn from_chunks(duration_ms: u64, options: ChunkPlanOptions, chunks: Vec<AudioChunk>) -> Self {
        let body = serde_json::json!({
            "schema_version": 1,
            "duration_ms": duration_ms,
            "options": options,
            "chunks": chunks,
        });
        let plan_hash = blake3::hash(body.to_string().as_bytes())
            .to_hex()
            .to_string();
        Self {
            schema_version: 1,
            duration_ms,
            options,
            chunks,
            plan_hash,
        }
    }
}

fn validate_options(duration_ms: u64, options: ChunkPlanOptions) -> AppResult<()> {
    if options.max_chunk_ms == 0
        || options.min_chunk_ms == 0
        || options.min_chunk_ms > options.max_chunk_ms
        || (duration_ms > 0 && options.min_chunk_ms > duration_ms)
    {
        return Err(ApplicationError::Adapter(VcError::new(
            ErrorCode::InvalidArgument,
            "invalid ChunkPlan minimum/maximum duration",
        )));
    }
    Ok(())
}

fn choose_cut(
    target: u64,
    lower: u64,
    upper: u64,
    silences: &[SilenceRegion],
    energy: &[EnergySample],
    options: ChunkPlanOptions,
) -> (u64, CutReason) {
    let silence = silences
        .iter()
        .filter_map(|region| {
            let start = region.start_ms.max(lower);
            let end = region.end_ms.min(upper);
            if end <= start || end - start < options.min_silence_ms {
                None
            } else {
                Some((end - start, start + (end - start) / 2))
            }
        })
        .min_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.abs_diff(target).cmp(&right.1.abs_diff(target)))
                .then_with(|| left.1.cmp(&right.1))
        });
    if let Some((_, cut)) = silence {
        return (cut, CutReason::Silence);
    }

    let low_energy = energy
        .iter()
        .filter(|sample| sample.at_ms >= lower && sample.at_ms <= upper)
        .min_by(|left, right| {
            left.energy
                .total_cmp(&right.energy)
                .then_with(|| {
                    left.at_ms
                        .abs_diff(target)
                        .cmp(&right.at_ms.abs_diff(target))
                })
                .then_with(|| left.at_ms.cmp(&right.at_ms))
        });
    if let Some(sample) = low_energy {
        return (sample.at_ms, CutReason::LowEnergy);
    }
    (target, CutReason::Forced)
}

fn make_chunk(
    index: u32,
    core_start_ms: u64,
    core_end_ms: u64,
    padding_ms: u64,
    cut_reason: CutReason,
    duration_ms: u64,
) -> AudioChunk {
    AudioChunk {
        index,
        core_start_ms,
        core_end_ms,
        read_start_ms: core_start_ms.saturating_sub(padding_ms),
        read_end_ms: core_end_ms.saturating_add(padding_ms).min(duration_ms),
        cut_reason,
    }
}

pub fn apply_chunk_offset(words: &[Word], read_start_ms: u64) -> Vec<Word> {
    words
        .iter()
        .map(|word| Word {
            text: word.text.clone(),
            start_ms: word.start_ms.saturating_add(read_start_ms),
            end_ms: word.end_ms.saturating_add(read_start_ms),
            prob: word.prob,
        })
        .collect()
}

pub fn retain_core_words(words: &[Word], chunk: AudioChunk) -> Vec<Word> {
    words
        .iter()
        .filter(|word| {
            let center = word
                .start_ms
                .saturating_add(word.end_ms.saturating_sub(word.start_ms) / 2);
            center >= chunk.core_start_ms && center < chunk.core_end_ms
        })
        .cloned()
        .collect()
}

pub fn chunk_cache_key(
    pcm_hash: &str,
    plan_hash: &str,
    chunk_index: u32,
    engine_fingerprint: &str,
    normalized_options_hash: &str,
) -> String {
    let body = format!(
        "{pcm_hash}\0{plan_hash}\0{chunk_index}\0{engine_fingerprint}\0{normalized_options_hash}"
    );
    blake3::hash(body.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> ChunkPlanOptions {
        ChunkPlanOptions {
            search_radius_ms: 1_000,
            context_padding_ms: 200,
            min_chunk_ms: 2_000,
            max_chunk_ms: 5_000,
            min_silence_ms: 300,
        }
    }

    #[test]
    fn silence_cut_has_contiguous_core_and_overlapping_read_context() {
        let plan = ChunkPlan::build(
            12_000,
            &[SilenceRegion {
                start_ms: 4_500,
                end_ms: 5_500,
            }],
            &[],
            options(),
        )
        .unwrap();
        assert_eq!(plan.chunks.len(), 3);
        assert_eq!(plan.chunks[0].cut_reason, CutReason::Silence);
        assert_eq!(plan.chunks[0].core_end_ms, 5_000);
        assert_eq!(plan.chunks[1].core_start_ms, plan.chunks[0].core_end_ms);
        assert_eq!(plan.chunks[1].read_start_ms, 4_800);
        plan.validate().unwrap();
    }

    #[test]
    fn energy_then_forced_cuts_are_deterministic() {
        let plan = ChunkPlan::build(
            12_000,
            &[],
            &[EnergySample {
                at_ms: 5_400,
                energy: 0.01,
            }],
            options(),
        )
        .unwrap();
        assert_eq!(plan.chunks[0].cut_reason, CutReason::LowEnergy);
        let forced = ChunkPlan::build(12_000, &[], &[], options()).unwrap();
        assert_eq!(forced.chunks[0].cut_reason, CutReason::Forced);
        assert_eq!(
            forced.plan_hash,
            ChunkPlan::build(12_000, &[], &[], options())
                .unwrap()
                .plan_hash
        );
    }

    #[test]
    fn center_ownership_prevents_duplicate_or_lost_words() {
        let words = vec![
            Word {
                text: "a".into(),
                start_ms: 4_900,
                end_ms: 5_100,
                prob: 1.0,
            },
            Word {
                text: "b".into(),
                start_ms: 5_100,
                end_ms: 5_300,
                prob: 1.0,
            },
        ];
        let left = AudioChunk {
            index: 0,
            core_start_ms: 0,
            core_end_ms: 5_000,
            read_start_ms: 0,
            read_end_ms: 5_200,
            cut_reason: CutReason::Forced,
        };
        let right = AudioChunk {
            index: 1,
            core_start_ms: 5_000,
            core_end_ms: 10_000,
            read_start_ms: 4_800,
            read_end_ms: 10_000,
            cut_reason: CutReason::EndOfAudio,
        };
        assert!(retain_core_words(&words, left).is_empty());
        assert_eq!(retain_core_words(&words, right).len(), 2);
        assert_eq!(apply_chunk_offset(&words, 100)[0].start_ms, 5_000);
    }
}
