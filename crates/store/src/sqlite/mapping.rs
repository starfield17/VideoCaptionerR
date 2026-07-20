use super::*;

pub(crate) fn next_version(current: u64, expected: ExpectedVersion) -> u64 {
    match expected {
        ExpectedVersion::New => 1,
        ExpectedVersion::Exact(_) => current.saturating_add(1),
    }
}

pub(crate) fn stage_fault_at(
    configured: Option<StageCommitFaultPoint>,
    point: StageCommitFaultPoint,
) -> VcResult<()> {
    if configured == Some(point) {
        return Err(VcError::new(
            ErrorCode::Internal,
            format!("injected stage commit interruption at {point:?}"),
        ));
    }
    Ok(())
}

pub(crate) fn artifact_meta_for(
    prepared: &videocaptionerr_core::ports::PreparedArtifact,
) -> ArtifactMeta {
    let artifact = &prepared.artifact;
    ArtifactMeta {
        schema_version: artifact.schema_version,
        id: artifact.id.to_string(),
        job_id: prepared.job_id.to_string(),
        stage: artifact.stage.as_str().into(),
        kind: artifact_kind(artifact.stage),
        path: artifact.path.clone(),
        content_hash: artifact.content_hash.clone(),
        producer_fingerprint: artifact.producer_fingerprint.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        committed: true,
    }
}

pub(crate) fn artifact_kind(stage: videocaptionerr_domain::StageKind) -> ArtifactKind {
    match stage {
        videocaptionerr_domain::StageKind::Probe => ArtifactKind::MediaProbe,
        videocaptionerr_domain::StageKind::ExtractAudio => ArtifactKind::AudioWav,
        videocaptionerr_domain::StageKind::Asr => ArtifactKind::Transcript,
        videocaptionerr_domain::StageKind::Split
        | videocaptionerr_domain::StageKind::Correct
        | videocaptionerr_domain::StageKind::Translate => ArtifactKind::Transcript,
        videocaptionerr_domain::StageKind::Export => ArtifactKind::Other,
    }
}

pub(crate) fn snapshot_projection(
    tx: &rusqlite::Transaction<'_>,
    snapshot_id: Option<&videocaptionerr_domain::UlidStr>,
) -> VcResult<Option<(String, String, String)>> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(None);
    };
    tx.query_row(
        "SELECT canonical_source_path, job_dir, profile_revision
         FROM execution_snapshots WHERE snapshot_id = ?1",
        [snapshot_id.as_str()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )
    .optional()
    .map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("load execution snapshot projection: {error}"),
        )
    })
}

pub(crate) fn job_status_name(status: videocaptionerr_domain::JobStatus) -> &'static str {
    match status {
        videocaptionerr_domain::JobStatus::Pending => "pending",
        videocaptionerr_domain::JobStatus::Running => "running",
        videocaptionerr_domain::JobStatus::Done => "done",
        videocaptionerr_domain::JobStatus::DoneDegraded => "done_degraded",
        videocaptionerr_domain::JobStatus::Failed => "failed",
        videocaptionerr_domain::JobStatus::Cancelled => "cancelled",
    }
}

pub(crate) fn stage_status_name(status: videocaptionerr_domain::StageStatus) -> &'static str {
    match status {
        videocaptionerr_domain::StageStatus::Pending => "pending",
        videocaptionerr_domain::StageStatus::WaitingResource => "waiting_resource",
        videocaptionerr_domain::StageStatus::Running => "running",
        videocaptionerr_domain::StageStatus::Retrying => "retrying",
        videocaptionerr_domain::StageStatus::Done => "done",
        videocaptionerr_domain::StageStatus::DoneDegraded => "done_degraded",
        videocaptionerr_domain::StageStatus::Failed => "failed",
        videocaptionerr_domain::StageStatus::Skipped => "skipped",
        videocaptionerr_domain::StageStatus::Cancelled => "cancelled",
        videocaptionerr_domain::StageStatus::WaitingProvider => "waiting_provider",
    }
}

pub(crate) fn work_unit_status_name(
    status: videocaptionerr_domain::WorkUnitStatus,
) -> &'static str {
    match status {
        videocaptionerr_domain::WorkUnitStatus::Pending => "pending",
        videocaptionerr_domain::WorkUnitStatus::Running => "running",
        videocaptionerr_domain::WorkUnitStatus::Done => "done",
        videocaptionerr_domain::WorkUnitStatus::Failed => "failed",
        videocaptionerr_domain::WorkUnitStatus::Cancelled => "cancelled",
    }
}

#[cfg(test)]
pub(crate) fn parse_work_unit_status(status: &str) -> Option<WorkUnitStatus> {
    Some(match status {
        "pending" => WorkUnitStatus::Pending,
        "running" => WorkUnitStatus::Running,
        "done" => WorkUnitStatus::Done,
        "failed" => WorkUnitStatus::Failed,
        "cancelled" => WorkUnitStatus::Cancelled,
        _ => return None,
    })
}

pub(crate) fn stage_rank(stage: &str) -> u8 {
    match stage {
        "probe" => 0,
        "extract_audio" => 1,
        "asr" => 2,
        "split" => 3,
        "correct" => 4,
        "translate" => 5,
        "export" => 6,
        _ => u8::MAX,
    }
}
