use crate::capability::{decode_probe_result, structured_mode_label};
use crate::{ApplicationRuntime, RuntimeConfig};
use videocaptionerr_contracts::error::ErrorCode;
use videocaptionerr_core::ports::{ExpectedVersion, Versioned};
use videocaptionerr_domain::{
    Batch, BatchExecutionProfile, BatchId, BatchStatus, Job, JobId, JobStatus,
};
use videocaptionerr_llm::probe::{CapabilityProbe, ProbeConfig, ProbeResult};
use videocaptionerr_llm::provider::{
    ProviderCapabilities, StructuredMode, CAPABILITY_PROBE_VERSION,
};
use videocaptionerr_platform::{InstanceLock, LlmCapabilityOverride, LockOwner};

fn probe_config() -> ProbeConfig {
    ProbeConfig::new(
        "primary",
        7,
        "https://example.test/v1/",
        "test-only-secret",
        "model-a",
    )
}

fn probe_fixture(config: &ProbeConfig) -> ProbeResult {
    ProbeResult {
        probe_version: CAPABILITY_PROBE_VERSION,
        provider_profile_id: config.provider_profile_id.clone(),
        profile_revision: config.profile_revision,
        base_url: config.base_url.trim_end_matches('/').into(),
        model: config.model.clone(),
        probe_hash: CapabilityProbe::new(config.clone()).cache_key(),
        capabilities: ProviderCapabilities::conservative_default(),
        warnings: vec![],
    }
}

#[test]
fn cached_probe_requires_full_identity_match() {
    let config = probe_config();
    let fixture = probe_fixture(&config);
    let encoded = serde_json::to_string(&fixture).unwrap();
    let decoded = decode_probe_result(&encoded, &config, None).unwrap();
    assert_eq!(decoded.probe_hash, fixture.probe_hash);

    let mut corrupt = fixture;
    corrupt.model = "different-model".into();
    let error =
        decode_probe_result(&serde_json::to_string(&corrupt).unwrap(), &config, None).unwrap_err();
    assert_eq!(error.code, ErrorCode::CacheCorrupt);
}

#[test]
fn manual_capability_override_wins_over_cached_auto_result() {
    let config = probe_config();
    let fixture = probe_fixture(&config);
    let override_config = LlmCapabilityOverride {
        structured_mode: Some("json_schema".into()),
        ..Default::default()
    };
    let decoded = decode_probe_result(
        &serde_json::to_string(&fixture).unwrap(),
        &config,
        Some(&override_config),
    )
    .unwrap();
    assert_eq!(
        decoded.capabilities.effective_structured_mode(),
        StructuredMode::JsonSchema
    );
    assert!(decoded.capabilities.manual_override);
}

#[test]
fn capability_view_uses_stable_structured_mode_labels() {
    assert_eq!(
        structured_mode_label(StructuredMode::JsonSchema),
        "json_schema"
    );
    assert_eq!(
        structured_mode_label(StructuredMode::JsonObject),
        "json_object"
    );
    assert_eq!(
        structured_mode_label(StructuredMode::PromptOnly),
        "prompt_only"
    );
}

#[tokio::test]
async fn probe_without_a_profile_fails_before_any_provider_request() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ApplicationRuntime::open(RuntimeConfig {
        home: Some(dir.path().to_path_buf()),
        engine: Some("fake".into()),
        model_path: None,
        helper_path: None,
        prompt_dir: None,
        profile: None,
    })
    .unwrap();
    let error = runtime
        .probe_llm_capabilities(None, false)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::LlmProviderUnavailable);
}

#[tokio::test]
async fn read_only_runtime_open_does_not_recover_a_live_processing_owner() {
    let dir = tempfile::tempdir().unwrap();
    let owner = ApplicationRuntime::open(RuntimeConfig {
        home: Some(dir.path().to_path_buf()),
        engine: Some("fake".into()),
        model_path: None,
        helper_path: None,
        prompt_dir: None,
        profile: None,
    })
    .unwrap();
    // The first owner has already completed its startup recovery; acquire the
    // OS lease directly to model its later live processing window.
    let _owner_lock =
        InstanceLock::try_acquire(&owner.paths().instance_lock_path(), LockOwner::Cli).unwrap();
    let job_id: JobId = ulid::Ulid::new().into();
    let batch_id: BatchId = ulid::Ulid::new().into();
    let mut job = Versioned::new(Job::new(
        job_id.clone(),
        Some(batch_id.clone()),
        ulid::Ulid::new().into(),
        "/media/live.mp4",
    ));
    job.start().unwrap();
    let mut batch = Versioned::new(
        Batch::new(
            batch_id.clone(),
            vec![job_id],
            BatchExecutionProfile {
                asr_engine: "fake".into(),
                asr_model: "fake".into(),
                device: "cpu".into(),
                compute_type: "default".into(),
            },
        )
        .unwrap(),
    );
    batch.start().unwrap();
    owner
        .batches
        .save_batch(&mut batch, ExpectedVersion::New)
        .await
        .unwrap();
    owner
        .jobs
        .save_job(&mut job, ExpectedVersion::New)
        .await
        .unwrap();
    // list/show/probe and Desktop startup all share this read-only open path.
    let second = ApplicationRuntime::open(RuntimeConfig {
        home: Some(dir.path().to_path_buf()),
        engine: Some("fake".into()),
        model_path: None,
        helper_path: None,
        prompt_dir: None,
        profile: None,
    })
    .unwrap();
    assert_eq!(
        second.list_jobs().await.unwrap()[0].status(),
        JobStatus::Running
    );
    assert_eq!(
        second
            .batches
            .load_batch(&batch_id)
            .await
            .unwrap()
            .unwrap()
            .status(),
        BatchStatus::Running
    );
    second.pause_batch(batch_id.as_ref()).await.unwrap();
    second.pause_batch(batch_id.as_ref()).await.unwrap();
    assert!(second
        .batches
        .load_batch(&batch_id)
        .await
        .unwrap()
        .unwrap()
        .pause_requested());
    // A cross-process resume only changes durable state here; the live owner
    // remains the sole executor and observes the signal/polling boundary.
    second.resume_batch(batch_id.as_ref()).await.unwrap();
    second.resume_batch(batch_id.as_ref()).await.unwrap();
    let resumed = second.batches.load_batch(&batch_id).await.unwrap().unwrap();
    assert_eq!(resumed.status(), BatchStatus::Running);
    assert!(!resumed.pause_requested());
}

#[tokio::test]
async fn a_new_processing_owner_recovers_after_the_previous_owner_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let initial = ApplicationRuntime::open(RuntimeConfig {
        home: Some(dir.path().to_path_buf()),
        engine: Some("fake".into()),
        model_path: None,
        helper_path: None,
        prompt_dir: None,
        profile: None,
    })
    .unwrap();
    let job_id: JobId = ulid::Ulid::new().into();
    let batch_id: BatchId = ulid::Ulid::new().into();
    let mut job = Versioned::new(Job::new(
        job_id.clone(),
        Some(batch_id.clone()),
        ulid::Ulid::new().into(),
        "/media/dead.mp4",
    ));
    job.start().unwrap();
    let mut batch = Versioned::new(
        Batch::new(
            batch_id.clone(),
            vec![job_id],
            BatchExecutionProfile {
                asr_engine: "fake".into(),
                asr_model: "fake".into(),
                device: "cpu".into(),
                compute_type: "default".into(),
            },
        )
        .unwrap(),
    );
    batch.start().unwrap();
    initial
        .batches
        .save_batch(&mut batch, ExpectedVersion::New)
        .await
        .unwrap();
    initial
        .jobs
        .save_job(&mut job, ExpectedVersion::New)
        .await
        .unwrap();
    drop(initial);

    let next = ApplicationRuntime::open(RuntimeConfig {
        home: Some(dir.path().to_path_buf()),
        engine: Some("fake".into()),
        model_path: None,
        helper_path: None,
        prompt_dir: None,
        profile: None,
    })
    .unwrap();
    let _lock = next.acquire_cli_processing_lock().unwrap();
    assert_eq!(
        next.list_jobs().await.unwrap()[0].status(),
        JobStatus::Pending
    );
    assert_eq!(
        next.batches
            .load_batch(&batch_id)
            .await
            .unwrap()
            .unwrap()
            .status(),
        BatchStatus::Pending
    );
    assert_eq!(next.recovery_report().recovered_jobs, 1);
    assert_eq!(next.recovery_report().recovered_batches, 1);
}
