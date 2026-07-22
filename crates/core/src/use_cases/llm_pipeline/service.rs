//! LlmPipeline service orchestration.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_domain::{CueTextUpdate, LlmResultBinding, LlmTextField, Transcript};

use crate::application_error::{AppResult, ApplicationError};
use crate::constants::{CORRECTION_SIMILARITY, CORRECTION_TRANSLATION_RETRIES, SPLIT_RETRIES};
use crate::ports::{
    ArtifactSource, ExpectedVersion, IdGenerator, LlmGateway, LlmMessage, LlmRequest,
    LlmRequestMetadata, LlmRequestRecorder, LlmRole, LlmStage, OutboxEvent, PreparedArtifact,
    StageCommitRequest, Versioned,
};
use videocaptionerr_domain::{ArtifactRef, WorkUnit, WorkUnitStatus};

use super::packing::{estimate_batch_tokens, pack_batches, pack_one_batch};
use super::plan::{effective_structured_output, empty_plan, make_plan};
use super::retry::{can_isolate, error_code, hash_request, temperature};
use super::split::{remove_break_markers, restore_split_spaces, split_ranges_for_formatted};
use super::types::*;
use super::validation::{
    data_prompt, is_original_residue, normalized_len, normalized_similarity, parse_json,
    parse_response_items, response_schema, validation_error,
};

impl LlmPipeline {
    pub fn new(
        gateway: Arc<dyn LlmGateway>,
        recorder: Arc<dyn LlmRequestRecorder>,
        ids: Arc<dyn IdGenerator>,
    ) -> Self {
        Self {
            gateway,
            recorder,
            ids,
            work_units: None,
            stage_commits: None,
        }
    }

    pub fn with_work_units(
        mut self,
        work_units: Arc<dyn crate::ports::WorkUnitRepository>,
    ) -> Self {
        self.work_units = Some(work_units);
        self
    }

    pub fn with_stage_commits(
        mut self,
        stage_commits: Arc<dyn crate::ports::StageCommitRepository>,
    ) -> Self {
        self.stage_commits = Some(stage_commits);
        self
    }

    fn stage_kind(stage: LlmStage) -> videocaptionerr_domain::StageKind {
        match stage {
            LlmStage::Split => videocaptionerr_domain::StageKind::Split,
            LlmStage::Correct => videocaptionerr_domain::StageKind::Correct,
            LlmStage::Translate => videocaptionerr_domain::StageKind::Translate,
        }
    }

    pub async fn execute(
        &self,
        transcript: &Transcript,
        mut request: LlmPipelineRequest,
    ) -> AppResult<LlmPipelineResult> {
        ensure_not_cancelled(request.cancel.as_ref())?;
        if request.prompt.stage != request.stage {
            return Err(ApplicationError::Invalid(
                "LLM prompt snapshot stage does not match the requested stage".into(),
            ));
        }
        // Durable path: materialize PromptSnapshot before any network call and
        // prefer the frozen artifact over re-reading editable prompt files.
        if let Some(ctx) = request.durable.clone() {
            let manifest = super::durable::materialize_prompt_artifact(&ctx, &request)?;
            request.prompt = super::durable::load_prompt_artifact(
                &ctx.job_dir,
                request.stage,
                &manifest.content_hash,
            )?;
            if !ctx.invalidate_plan {
                if let Some(existing) = super::durable::load_plan(&ctx.job_dir, request.stage)? {
                    // Restart must not re-pack when a durable plan already exists.
                    if existing.prompt_bundle_hash == request.prompt.content_hash
                        && existing.model == request.model
                    {
                        // Keep plan identity; stage methods will skip Done batches.
                        let _ = existing;
                    }
                }
            }
        }
        let capabilities = self.gateway.capabilities().await?;
        request.structured_output =
            effective_structured_output(request.structured_output, capabilities.structured_output);
        let context_limit = request
            .max_context_tokens
            .or(capabilities.max_context_tokens)
            .unwrap_or(DEFAULT_CONTEXT_TOKENS);
        let output_limit = request
            .max_output_tokens
            .or(capabilities.max_output_tokens)
            .unwrap_or(DEFAULT_OUTPUT_TOKENS);

        match request.stage {
            LlmStage::Split => {
                self.execute_split(transcript, request, context_limit, output_limit)
                    .await
            }
            LlmStage::Correct => {
                self.execute_correct(transcript, request, context_limit, output_limit)
                    .await
            }
            LlmStage::Translate => {
                self.execute_translate(transcript, request, context_limit, output_limit)
                    .await
            }
        }
    }

    async fn execute_split(
        &self,
        transcript: &Transcript,
        request: LlmPipelineRequest,
        context_limit: u32,
        output_limit: u32,
    ) -> AppResult<LlmPipelineResult> {
        if transcript.cues.is_empty() {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::LlmValidationFailed,
                "LLM split requires a rule-split transcript",
            )));
        }
        if transcript.words.is_empty() {
            return Ok(LlmPipelineResult {
                transcript: transcript.clone(),
                plan: empty_plan(&request),
                degraded_cue_ids: Vec::new(),
            });
        }

        let inputs = transcript
            .cues
            .iter()
            .map(|cue| CueInput {
                id: cue.id,
                text: cue.text.clone(),
            })
            .collect::<Vec<_>>();
        let batches = pack_batches(&inputs, &[], &request, context_limit, output_limit)?;
        let plan = self
            .ensure_plan(&request, &batches, context_limit, output_limit)
            .await?;
        let batches = if request.durable.is_some()
            && super::durable::load_plan(&request.durable.as_ref().unwrap().job_dir, request.stage)?
                .is_some_and(|p| p.plan_hash == plan.plan_hash)
        {
            self.batches_from_plan(&plan, transcript)?
        } else {
            batches
        };
        let mut formatted = BTreeMap::new();
        let mut degraded = Vec::new();
        for batch in &batches {
            ensure_not_cancelled(request.cancel.as_ref())?;
            if let Some(items) = self
                .load_durable_batch(&request, &plan, batch.index)
                .await?
            {
                formatted.extend(items);
                continue;
            }
            self.execute_split_batch(&request, batch, &mut formatted, &mut degraded)
                .await?;
            self.persist_durable_batch(&request, &plan, batch, transcript, &formatted)
                .await?;
            ensure_not_cancelled(request.cancel.as_ref())?;
        }

        let mut ranges = Vec::new();
        for cue in &transcript.cues {
            let formatted_text = formatted
                .get(&cue.id)
                .cloned()
                .unwrap_or_else(|| cue.text.clone());
            let cue_ranges = split_ranges_for_formatted(transcript, cue, &formatted_text)
                .map_err(|violation| validation_error("split", violation))?;
            ranges.extend(cue_ranges);
        }
        let request_id = self.ids.next_id().to_string();
        let mut output = transcript.apply_llm_split(&ranges, request_id)?;
        let failed_ranges = transcript
            .cues
            .iter()
            .filter(|cue| degraded.contains(&cue.id))
            .filter_map(|cue| cue.word_range)
            .collect::<Vec<_>>();
        let output_degraded = output
            .cues
            .iter()
            .filter(|cue| {
                cue.word_range.is_some_and(|range| {
                    failed_ranges
                        .iter()
                        .any(|failed| range.start >= failed.start && range.end <= failed.end)
                })
            })
            .map(|cue| cue.id)
            .collect::<Vec<_>>();
        if !output_degraded.is_empty() {
            output = output.mark_llm_failed(&output_degraded)?;
        }
        Ok(LlmPipelineResult {
            transcript: output,
            plan,
            degraded_cue_ids: output_degraded,
        })
    }

    async fn execute_split_batch(
        &self,
        request: &LlmPipelineRequest,
        batch: &BatchInput,
        accepted: &mut BTreeMap<u32, String>,
        degraded: &mut Vec<u32>,
    ) -> AppResult<()> {
        let mut pending = vec![batch.clone()];
        while let Some(current) = pending.pop() {
            match self.request_split_batch(request, &current).await {
                Ok(items) => accepted.extend(items),
                Err(error) if can_isolate(&error) && current.output.len() > 1 => {
                    let midpoint = current.output.len() / 2;
                    pending.push(BatchInput {
                        index: current.index,
                        output: current.output[midpoint..].to_vec(),
                        context: current.context.clone(),
                    });
                    pending.push(BatchInput {
                        index: current.index,
                        output: current.output[..midpoint].to_vec(),
                        context: current.context,
                    });
                }
                Err(error) if can_isolate(&error) => {
                    degraded.extend(current.output.iter().map(|item| item.id));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn request_split_batch(
        &self,
        request: &LlmPipelineRequest,
        batch: &BatchInput,
    ) -> AppResult<BTreeMap<u32, String>> {
        let body = serde_json::json!({
            "items": batch
                .output
                .iter()
                .map(|item| RequestItem { id: item.id, text: &item.text })
                .collect::<Vec<_>>(),
            "instruction": "Insert <br> only at word boundaries. Do not change any other character.",
        });
        let messages = vec![
            LlmMessage {
                role: LlmRole::System,
                content: request.prompt.system_prompt(),
            },
            LlmMessage {
                role: LlmRole::User,
                content: data_prompt(body),
            },
        ];
        let value = self
            .request_json(request, batch.index, messages, SPLIT_RETRIES, |value| {
                let items = parse_response_items(value)?;
                let expected = batch
                    .output
                    .iter()
                    .map(|item| item.id)
                    .collect::<BTreeSet<_>>();
                let actual = items.keys().copied().collect::<BTreeSet<_>>();
                if expected != actual {
                    return Err(format!(
                        "split output keys mismatch: expected {:?}, got {:?}",
                        expected, actual
                    ));
                }
                for item in &batch.output {
                    let result = items.get(&item.id).expect("key set checked");
                    let (clean, breaks) = remove_break_markers(result);
                    if clean != item.text && restore_split_spaces(&clean, &breaks) != item.text {
                        return Err(format!("split item {} changed source content", item.id));
                    }
                }
                Ok(items)
            })
            .await?;
        Ok(value)
    }

    async fn execute_correct(
        &self,
        transcript: &Transcript,
        request: LlmPipelineRequest,
        context_limit: u32,
        output_limit: u32,
    ) -> AppResult<LlmPipelineResult> {
        let inputs = transcript
            .cues
            .iter()
            .map(|cue| CueInput {
                id: cue.id,
                text: cue.text.clone(),
            })
            .collect::<Vec<_>>();
        let batches = pack_batches(&inputs, &[], &request, context_limit, output_limit)?;
        let plan = self
            .ensure_plan(&request, &batches, context_limit, output_limit)
            .await?;
        let batches = if request.durable.is_some()
            && super::durable::load_plan(&request.durable.as_ref().unwrap().job_dir, request.stage)?
                .is_some_and(|p| p.plan_hash == plan.plan_hash)
        {
            self.batches_from_plan(&plan, transcript)?
        } else {
            batches
        };
        let mut values = BTreeMap::new();
        let mut degraded = Vec::new();
        for batch in &batches {
            ensure_not_cancelled(request.cancel.as_ref())?;
            if let Some(items) = self
                .load_durable_batch(&request, &plan, batch.index)
                .await?
            {
                values.extend(items);
                continue;
            }
            self.execute_text_batch(
                LlmTextField::Source,
                &request,
                batch,
                transcript,
                &mut values,
                &mut degraded,
            )
            .await?;
            self.persist_durable_batch(&request, &plan, batch, transcript, &values)
                .await?;
            ensure_not_cancelled(request.cancel.as_ref())?;
        }
        let updates = values
            .into_iter()
            .filter_map(|(id, value)| {
                transcript
                    .cues
                    .iter()
                    .find(|cue| cue.id == id)
                    .map(|cue| CueTextUpdate {
                        cue_id: id,
                        expected_field_revision: cue.text_revision,
                        value,
                    })
            })
            .collect::<Vec<_>>();
        let binding = LlmResultBinding {
            transcript_revision: transcript.revision,
            field: LlmTextField::Source,
            request_id: self.ids.next_id().to_string(),
        };
        let mut output = transcript.apply_llm_text(&binding, &updates)?;
        if !degraded.is_empty() {
            output = output.mark_llm_failed(&degraded)?;
        }
        Ok(LlmPipelineResult {
            transcript: output,
            plan,
            degraded_cue_ids: degraded,
        })
    }

    async fn execute_text_batch(
        &self,
        field: LlmTextField,
        request: &LlmPipelineRequest,
        batch: &BatchInput,
        transcript: &Transcript,
        accepted: &mut BTreeMap<u32, String>,
        degraded: &mut Vec<u32>,
    ) -> AppResult<()> {
        let mut pending = vec![batch.clone()];
        while let Some(current) = pending.pop() {
            match self
                .request_text_batch(field, request, &current, transcript)
                .await
            {
                Ok(items) => accepted.extend(items),
                Err(error) if can_isolate(&error) && current.output.len() > 1 => {
                    let midpoint = current.output.len() / 2;
                    pending.push(BatchInput {
                        index: current.index,
                        output: current.output[midpoint..].to_vec(),
                        context: current.context.clone(),
                    });
                    pending.push(BatchInput {
                        index: current.index,
                        output: current.output[..midpoint].to_vec(),
                        context: current.context,
                    });
                }
                Err(error) if can_isolate(&error) => {
                    degraded.extend(current.output.iter().map(|item| item.id));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn request_text_batch(
        &self,
        field: LlmTextField,
        request: &LlmPipelineRequest,
        batch: &BatchInput,
        transcript: &Transcript,
    ) -> AppResult<BTreeMap<u32, String>> {
        let context = batch
            .context
            .iter()
            .map(|item| serde_json::json!({"id": item.id, "translation": item.text}))
            .collect::<Vec<_>>();
        let body = serde_json::json!({
            "items": batch
                .output
                .iter()
                .map(|item| RequestItem { id: item.id, text: &item.text })
                .collect::<Vec<_>>(),
            "context": context,
            "task": match field {
                LlmTextField::Source => "correct source text only",
                LlmTextField::Translation => "translate and reflect the final translation",
            },
            "target_language": request.target_language,
        });
        let messages = vec![
            LlmMessage {
                role: LlmRole::System,
                content: request.prompt.system_prompt(),
            },
            LlmMessage {
                role: LlmRole::User,
                content: data_prompt(body),
            },
        ];
        let similarity_threshold = if matches!(field, LlmTextField::Source) {
            CORRECTION_SIMILARITY
        } else {
            0.0
        };
        let value = self
            .request_json(
                request,
                batch.index,
                messages,
                CORRECTION_TRANSLATION_RETRIES,
                |value| {
                    let items = parse_response_items(value)?;
                    let expected = batch
                        .output
                        .iter()
                        .map(|item| item.id)
                        .collect::<BTreeSet<_>>();
                    let actual = items.keys().copied().collect::<BTreeSet<_>>();
                    if expected != actual {
                        return Err(format!(
                            "LLM output keys mismatch: expected {:?}, got {:?}",
                            expected, actual
                        ));
                    }
                    for item in &batch.output {
                        let output = items.get(&item.id).expect("key set checked");
                        if output.trim().is_empty() {
                            return Err(format!("cue {} returned empty text", item.id));
                        }
                        if similarity_threshold > 0.0
                            && normalized_similarity(&item.text, output) < similarity_threshold
                            && normalized_len(&item.text) > 3
                        {
                            return Err(format!(
                                "cue {} correction similarity is below {:.2}",
                                item.id, similarity_threshold
                            ));
                        }
                        if matches!(field, LlmTextField::Translation)
                            && is_original_residue(&item.text, output)
                        {
                            return Err(format!(
                                "cue {} translation contains unchanged source residue",
                                item.id
                            ));
                        }
                    }
                    Ok(items)
                },
            )
            .await?;
        let _ = transcript;
        Ok(value)
    }

    async fn execute_translate(
        &self,
        transcript: &Transcript,
        request: LlmPipelineRequest,
        context_limit: u32,
        output_limit: u32,
    ) -> AppResult<LlmPipelineResult> {
        if request.target_language.as_deref().is_none_or(str::is_empty) {
            return Err(ApplicationError::Invalid(
                "translation requires target_language".into(),
            ));
        }
        let inputs = transcript
            .cues
            .iter()
            .map(|cue| CueInput {
                id: cue.id,
                text: cue.text.clone(),
            })
            .collect::<Vec<_>>();
        let mut plan_entries = Vec::new();
        let mut previous_context = Vec::new();
        let mut remaining = inputs.as_slice();
        let mut batch_index = 0;
        let mut working = transcript.clone();
        let mut degraded = Vec::new();
        while !remaining.is_empty() {
            ensure_not_cancelled(request.cancel.as_ref())?;
            let batch = pack_one_batch(
                remaining,
                &previous_context,
                batch_index,
                &request,
                context_limit,
                output_limit,
            )?;
            let consumed = batch.output.len();
            let mut accepted = BTreeMap::new();
            self.execute_text_batch(
                LlmTextField::Translation,
                &request,
                &batch,
                &working,
                &mut accepted,
                &mut degraded,
            )
            .await?;
            plan_entries.push(LlmPlanEntry {
                batch_index: batch.index,
                output_cue_ids: batch.output.iter().map(|item| item.id).collect(),
                context_cue_ids: batch.context.iter().map(|item| item.id).collect(),
                estimated_input_tokens: estimate_batch_tokens(&batch, request.chars_per_token),
                reserved_output_tokens: output_limit,
                expected_text_revisions: Default::default(),
                expected_translation_revisions: Default::default(),
            });
            let updates = accepted
                .iter()
                .filter_map(|(id, value)| {
                    working
                        .cues
                        .iter()
                        .find(|cue| cue.id == *id)
                        .map(|cue| CueTextUpdate {
                            cue_id: *id,
                            expected_field_revision: cue.translation_revision,
                            value: value.clone(),
                        })
                })
                .collect::<Vec<_>>();
            let binding = LlmResultBinding {
                transcript_revision: working.revision,
                field: LlmTextField::Translation,
                request_id: self.ids.next_id().to_string(),
            };
            working = working.apply_llm_text(&binding, &updates)?;
            ensure_not_cancelled(request.cancel.as_ref())?;

            // Accepted translations from this batch are the read-only context
            // for the next wavefront batch.
            previous_context = batch
                .output
                .iter()
                .filter_map(|item| {
                    working
                        .cues
                        .iter()
                        .find(|cue| cue.id == item.id)
                        .and_then(|cue| cue.translation.clone())
                        .map(|translation| CueInput {
                            id: item.id,
                            text: translation,
                        })
                })
                .collect();
            remaining = &remaining[consumed..];
            batch_index = batch_index.saturating_add(1);
        }
        let mut plan = LlmPlan {
            schema_version: super::durable::LLM_PLAN_SCHEMA_VERSION,
            plan_id: self.ids.next_id().to_string(),
            job_id: request.durable.as_ref().map(|d| d.job_id.to_string()),
            stage: request.stage,
            input_artifact_id: request
                .durable
                .as_ref()
                .and_then(|d| d.input_artifact_id.clone()),
            transcript_revision: request
                .durable
                .as_ref()
                .map(|d| d.transcript_revision)
                .unwrap_or(0),
            model: request.model.clone(),
            provider_profile_revision: request.provider_profile_revision.clone(),
            prompt_bundle_hash: request.prompt.content_hash.clone(),
            prompt_artifact_hash: request.prompt.content_hash.clone(),
            effective_capability: format!("{:?}", request.structured_output),
            max_context_tokens: Some(context_limit),
            max_output_tokens: Some(output_limit),
            target_language: request.target_language.clone(),
            entries: plan_entries,
            plan_hash: String::new(),
        };
        plan.plan_hash = super::durable::plan_hash(&plan);
        if !degraded.is_empty() {
            working = working.mark_llm_failed(&degraded)?;
        }
        Ok(LlmPipelineResult {
            transcript: working,
            plan,
            degraded_cue_ids: degraded,
        })
    }

    async fn ensure_plan(
        &self,
        request: &LlmPipelineRequest,
        batches: &[BatchInput],
        context_limit: u32,
        output_limit: u32,
    ) -> AppResult<LlmPlan> {
        if let Some(ctx) = &request.durable {
            if !ctx.invalidate_plan {
                if let Some(existing) = super::durable::load_plan(&ctx.job_dir, request.stage)? {
                    if existing.prompt_bundle_hash == request.prompt.content_hash
                        && existing.model == request.model
                    {
                        // Restart: never re-pack when a matching plan is durable.
                        self.ensure_work_units(request, &existing).await?;
                        return Ok(existing);
                    }
                }
            }
        }
        let plan = make_plan(request, batches, context_limit, output_limit);
        if let Some(ctx) = &request.durable {
            // Plan must be durable before the first network call.
            super::durable::persist_plan(ctx, &plan)?;
        }
        self.ensure_work_units(request, &plan).await?;
        Ok(plan)
    }

    /// Reconstruct batch inputs from a durable plan so restart does not re-pack.
    fn batches_from_plan(
        &self,
        plan: &LlmPlan,
        transcript: &Transcript,
    ) -> AppResult<Vec<BatchInput>> {
        let mut batches = Vec::with_capacity(plan.entries.len());
        for entry in &plan.entries {
            let output = entry
                .output_cue_ids
                .iter()
                .map(|id| {
                    transcript
                        .cues
                        .iter()
                        .find(|c| c.id == *id)
                        .map(|c| CueInput {
                            id: c.id,
                            text: c.text.clone(),
                        })
                        .ok_or_else(|| {
                            ApplicationError::Invalid(format!(
                                "durable plan references missing cue {id}"
                            ))
                        })
                })
                .collect::<AppResult<Vec<_>>>()?;
            let context = entry
                .context_cue_ids
                .iter()
                .filter_map(|id| {
                    transcript
                        .cues
                        .iter()
                        .find(|c| c.id == *id)
                        .map(|c| CueInput {
                            id: c.id,
                            text: c.translation.clone().unwrap_or_else(|| c.text.clone()),
                        })
                })
                .collect();
            batches.push(BatchInput {
                index: entry.batch_index,
                output,
                context,
            });
        }
        Ok(batches)
    }

    async fn ensure_work_units(
        &self,
        request: &LlmPipelineRequest,
        plan: &LlmPlan,
    ) -> AppResult<()> {
        let (Some(ctx), Some(work_units)) = (&request.durable, &self.work_units) else {
            return Ok(());
        };
        let stage = Self::stage_kind(request.stage);
        for entry in &plan.entries {
            let cue_revs: Vec<(u32, u64)> = entry
                .expected_text_revisions
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect();
            let input_hash = super::durable::work_unit_input_hash(plan, entry, &cue_revs);
            let existing = work_units
                .find_work_unit(
                    &ctx.job_id,
                    stage,
                    "llm_batch",
                    entry.batch_index,
                    &input_hash,
                )
                .await?;
            if existing.is_some() {
                continue;
            }
            let unit = WorkUnit::new(
                self.ids.next_id(),
                ctx.job_id.clone(),
                stage,
                "llm_batch",
                entry.batch_index,
                input_hash,
            )?;
            let mut versioned = Versioned::new(unit);
            work_units
                .save_work_unit(&mut versioned, ExpectedVersion::New)
                .await?;
        }
        Ok(())
    }

    async fn load_durable_batch(
        &self,
        request: &LlmPipelineRequest,
        plan: &LlmPlan,
        batch_index: u32,
    ) -> AppResult<Option<BTreeMap<u32, String>>> {
        let Some(ctx) = &request.durable else {
            return Ok(None);
        };
        let Some(result) =
            super::durable::load_batch_result(&ctx.job_dir, request.stage, batch_index)?
        else {
            return Ok(None);
        };
        if result.plan_hash != plan.plan_hash {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::StaleResult,
                "durable LLM batch result plan hash does not match active plan",
            )));
        }
        if result.transcript_revision != plan.transcript_revision {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::StaleResult,
                "durable LLM batch result transcript revision is stale",
            )));
        }
        // Corrupt/empty file body is already rejected by load; verify items non-empty for Done.
        if result.items.is_empty() {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("LLM batch {batch_index} result artifact has no items"),
            )));
        }
        Ok(Some(result.items))
    }

    async fn persist_durable_batch(
        &self,
        request: &LlmPipelineRequest,
        plan: &LlmPlan,
        batch: &BatchInput,
        transcript: &Transcript,
        accepted: &BTreeMap<u32, String>,
    ) -> AppResult<()> {
        let Some(ctx) = &request.durable else {
            return Ok(());
        };
        let mut items = BTreeMap::new();
        let mut cue_revisions = BTreeMap::new();
        for cue in &batch.output {
            if let Some(text) = accepted.get(&cue.id) {
                items.insert(cue.id, text.clone());
            }
            if let Some(c) = transcript.cues.iter().find(|c| c.id == cue.id) {
                cue_revisions.insert(cue.id, c.text_revision);
            }
        }
        // Stale CAS: reject if any expected cue revision no longer matches.
        if let Some(entry) = plan.entries.iter().find(|e| e.batch_index == batch.index) {
            for (cue_id, expected) in &entry.expected_text_revisions {
                let actual = transcript
                    .cues
                    .iter()
                    .find(|c| c.id == *cue_id)
                    .map(|c| c.text_revision)
                    .unwrap_or(0);
                if actual != *expected {
                    return Err(ApplicationError::Adapter(VcError::new(
                        ErrorCode::StaleResult,
                        format!("cue {cue_id} text revision {actual} != plan expected {expected}"),
                    )));
                }
            }
        }
        let artifact_payload = super::durable::LlmBatchResultArtifact {
            schema_version: 1,
            plan_id: plan.plan_id.clone(),
            plan_hash: plan.plan_hash.clone(),
            batch_index: batch.index,
            stage: request.stage,
            items: items.clone(),
            transcript_revision: plan.transcript_revision,
            input_artifact_id: plan.input_artifact_id.clone(),
            cue_revisions: cue_revisions.clone(),
        };
        super::durable::persist_batch_result(ctx, &artifact_payload)?;

        // Prefer atomic StageCommit when WorkUnit control plane is wired.
        if let (Some(work_units), Some(stage_commits)) = (&self.work_units, &self.stage_commits) {
            let stage = Self::stage_kind(request.stage);
            let entry = plan
                .entries
                .iter()
                .find(|e| e.batch_index == batch.index)
                .ok_or_else(|| {
                    ApplicationError::Invalid(format!(
                        "plan missing entry for batch {}",
                        batch.index
                    ))
                })?;
            let cue_revs: Vec<(u32, u64)> = cue_revisions.iter().map(|(k, v)| (*k, *v)).collect();
            let input_hash = super::durable::work_unit_input_hash(plan, entry, &cue_revs);
            let unit = work_units
                .find_work_unit(&ctx.job_id, stage, "llm_batch", batch.index, &input_hash)
                .await?
                .ok_or_else(|| {
                    ApplicationError::Invalid(format!(
                        "llm_batch WorkUnit for batch {} not found",
                        batch.index
                    ))
                })?;
            if unit.status() == WorkUnitStatus::Done {
                return Ok(());
            }
            let path = super::durable::batch_result_path(&ctx.job_dir, request.stage, batch.index);
            let bytes = serde_json::to_vec_pretty(&artifact_payload).map_err(|e| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode llm batch for commit: {e}"),
                ))
            })?;
            let artifact = ArtifactRef {
                id: self.ids.next_id(),
                stage,
                path: path.to_string_lossy().into_owned(),
                content_hash: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
                schema_version: 1,
                producer_fingerprint: format!("llm-{}", request.stage.as_str()),
            };
            let mut completed = unit.clone();
            completed.complete(artifact.clone())?;
            let prepared = PreparedArtifact {
                job_id: ctx.job_id.clone(),
                artifact: artifact.clone(),
                source: ArtifactSource::ExistingFile { path },
            };
            stage_commits
                .commit_stage(StageCommitRequest {
                    job: None,
                    work_unit: Some((completed, ExpectedVersion::Exact(unit.version))),
                    artifact: Some(prepared),
                    event: Some(OutboxEvent {
                        aggregate_type: "work_unit".into(),
                        aggregate_id: unit.id().to_string(),
                        event_type: "llm_batch.done".into(),
                        payload_json: serde_json::json!({
                            "job_id": ctx.job_id.to_string(),
                            "stage": request.stage.as_str(),
                            "batch_index": batch.index,
                            "artifact_id": artifact.id.to_string(),
                        })
                        .to_string(),
                        created_at: chrono::Utc::now().to_rfc3339(),
                    }),
                })
                .await?;
            let _ = work_units;
        }
        Ok(())
    }

    async fn request_json<T, F>(
        &self,
        request: &LlmPipelineRequest,
        batch_index: u32,
        mut messages: Vec<LlmMessage>,
        retries: u32,
        validate: F,
    ) -> AppResult<T>
    where
        F: Fn(&Value) -> Result<T, String>,
    {
        // Semantic/JSON validation retries via application agent repair loop.
        let max_semantic_attempts = super::agent::max_semantic_attempts(retries);
        // Transport retries: two automatic retries per WorkUnit by default.
        let max_transport_attempts = super::agent::MAX_TRANSPORT_ATTEMPTS;
        let mut last_violation: Option<String> = None;
        let mut semantic_attempt = 0u32;
        let mut transport_attempt = 0u32;
        loop {
            ensure_not_cancelled(request.cancel.as_ref())?;
            if semantic_attempt >= max_semantic_attempts {
                break;
            }
            if let Some(violation) = last_violation.take() {
                messages.push(super::agent::repair_user_message(&violation));
            }
            let request_id = self.ids.next_id().to_string();
            let request_hash = hash_request(request, &messages);
            let started = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let response = self
                .gateway
                .chat(LlmRequest {
                    model: request.model.clone(),
                    messages: messages.clone(),
                    temperature: Some(temperature(request.stage)),
                    max_output_tokens: request.max_output_tokens,
                    seed: request.seed,
                    structured_output: request.structured_output,
                    schema: Some(response_schema()),
                })
                .await;
            match response {
                Err(error) => {
                    let code = error_code(&error);
                    let retry_after = match &error {
                        ApplicationError::Adapter(vc) => vc.retry_after_ms,
                        _ => None,
                    };
                    if let Some(ctx) = &request.durable {
                        let _ = super::durable::append_attempt(
                            ctx,
                            request.stage,
                            &super::durable::LlmAttemptRecord {
                                request_id: request_id.clone(),
                                work_unit_id: None,
                                attempt: transport_attempt,
                                request_hash: request_hash.clone(),
                                provider_model_revision: request.provider_profile_revision.clone(),
                                started_at_ms: started,
                                finished_at_ms: Some(started),
                                prompt_tokens: None,
                                completion_tokens: None,
                                error_code: Some(code.as_str().into()),
                                retry_after_ms: retry_after,
                            },
                        );
                    }
                    self.recorder
                        .record(LlmRequestMetadata {
                            request_id,
                            stage: request.stage,
                            batch_index,
                            attempt: transport_attempt,
                            model: request.model.clone(),
                            request_hash,
                            prompt_tokens: None,
                            completion_tokens: None,
                            error_code: Some(code.as_str().into()),
                        })
                        .await?;
                    match super::durable::classify_transport_error(code) {
                        super::durable::TransportRetryClass::FailFast
                        | super::durable::TransportRetryClass::Cancelled => {
                            return Err(error);
                        }
                        super::durable::TransportRetryClass::RateLimited
                        | super::durable::TransportRetryClass::Transient => {
                            transport_attempt = transport_attempt.saturating_add(1);
                            if transport_attempt >= max_transport_attempts {
                                return Err(error);
                            }
                            // Honor Retry-After when present; else exponential backoff + jitter.
                            let wait_ms = retry_after.unwrap_or_else(|| {
                                let base = 200u64.saturating_mul(1u64 << transport_attempt.min(4));
                                base + (started % 50)
                            });
                            tokio::time::sleep(std::time::Duration::from_millis(
                                wait_ms.min(30_000),
                            ))
                            .await;
                            continue;
                        }
                    }
                }
                Ok(response) => {
                    ensure_not_cancelled(request.cancel.as_ref())?;
                    if let Some(ctx) = &request.durable {
                        let _ = super::durable::append_attempt(
                            ctx,
                            request.stage,
                            &super::durable::LlmAttemptRecord {
                                request_id: request_id.clone(),
                                work_unit_id: None,
                                attempt: semantic_attempt,
                                request_hash: request_hash.clone(),
                                provider_model_revision: request.provider_profile_revision.clone(),
                                started_at_ms: started,
                                finished_at_ms: Some(started),
                                prompt_tokens: response.prompt_tokens,
                                completion_tokens: response.completion_tokens,
                                error_code: None,
                                retry_after_ms: None,
                            },
                        );
                    }
                    self.recorder
                        .record(LlmRequestMetadata {
                            request_id,
                            stage: request.stage,
                            batch_index,
                            attempt: semantic_attempt,
                            model: request.model.clone(),
                            request_hash,
                            prompt_tokens: response.prompt_tokens,
                            completion_tokens: response.completion_tokens,
                            error_code: None,
                        })
                        .await?;
                    match parse_json(&response.content).and_then(|value| {
                        validate(&value).map_err(|message| {
                            VcError::new(ErrorCode::LlmValidationFailed, message)
                        })
                    }) {
                        Ok(value) => return Ok(value),
                        Err(error) if semantic_attempt + 1 < max_semantic_attempts => {
                            last_violation = Some(error.to_string());
                            semantic_attempt = semantic_attempt.saturating_add(1);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        Err(ApplicationError::Adapter(VcError::new(
            ErrorCode::LlmValidationFailed,
            "LLM validation retries exhausted",
        )))
    }
}

fn ensure_not_cancelled(token: Option<&crate::ports::AsrCancelToken>) -> AppResult<()> {
    if token.is_some_and(crate::ports::AsrCancelToken::is_requested) {
        Err(ApplicationError::Cancelled)
    } else {
        Ok(())
    }
}
