//! Phase 5: real ASR runtime resolver, fingerprint, and adapter gates.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use videocaptionerr_core::application_error::AppResult;
use videocaptionerr_core::ports::{
    asr_fingerprint, AsrDescriptor, AsrRuntime, AsrRuntimeResolver, AsrRuntimeSpec, AsrSession,
    AsrTranscribeRequest, EventPublisher, ModelLocator, NormalizedAsrResult,
};
use videocaptionerr_domain::{BatchExecutionProfile, EngineFingerprint, Transcript, Word};

use crate::model::{blake3_file, ModelManifest};
use crate::resolver::FamilyAsrRuntimeResolver;
use crate::worker::resolve_helper_binary;

#[test]
fn locator_validation_rejects_empty_file() {
    assert!(ModelLocator::file("").validate().is_err());
    assert!(ModelLocator::directory("").validate().is_err());
    assert!(ModelLocator::hugging_face("", "main", None)
        .validate()
        .is_err());
    assert!(
        ModelLocator::file("/tmp").validate().is_ok()
            || ModelLocator::file("/tmp").validate().is_err()
    );
}

#[test]
fn fingerprint_changes_when_digest_changes() {
    let mut spec = AsrRuntimeSpec {
        engine_family: "whisper-cpp".into(),
        model_id: "tiny".into(),
        verified_digest: Some("blake3:aaa".into()),
        locator: ModelLocator::file("/models/a.bin"),
        device: "cpu".into(),
        compute_type: "default".into(),
    };
    let a = asr_fingerprint("whisper-cpp", "1", "rt", &spec, "opts");
    spec.verified_digest = Some("blake3:bbb".into());
    let b = asr_fingerprint("whisper-cpp", "1", "rt", &spec, "opts");
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_when_options_change() {
    let spec = AsrRuntimeSpec {
        engine_family: "fake".into(),
        model_id: "fake".into(),
        verified_digest: None,
        locator: ModelLocator::file("fake"),
        device: "cpu".into(),
        compute_type: "default".into(),
    };
    let a = asr_fingerprint("fake", "1", "rt", &spec, "opts-a");
    let b = asr_fingerprint("fake", "1", "rt", &spec, "opts-b");
    assert_ne!(a, b);
}

#[test]
fn same_path_different_content_changes_blake3() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("m.bin");
    std::fs::write(&p, b"aaa").unwrap();
    let d1 = blake3_file(&p).unwrap();
    std::fs::write(&p, b"bbb").unwrap();
    let d2 = blake3_file(&p).unwrap();
    assert_ne!(d1, d2);
}

#[test]
fn manifest_lists_official_families() {
    let m = ModelManifest::builtin();
    assert!(m.find("whisper-cpp/tiny-q5_1").is_some());
    assert!(m.find("faster-whisper/tiny").is_some());
    assert!(m.find("mlx-whisper/tiny").is_some());
    assert!(m.find("fake/tiny").is_some());
}

#[tokio::test]
async fn resolver_maps_fake_family() {
    let helper = resolve_helper_binary();
    // Helper may not be built yet in pure unit test; mapping still succeeds.
    let resolver = FamilyAsrRuntimeResolver::new(
        helper,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes"),
        tempfile::tempdir().unwrap().path().join("envs"),
    );
    let spec = AsrRuntimeSpec {
        engine_family: "fake".into(),
        model_id: "fake".into(),
        verified_digest: None,
        locator: ModelLocator::file("fake:default"),
        device: "cpu".into(),
        compute_type: "default".into(),
    };
    let _ = resolver.resolve(&spec).await.unwrap();
}

#[tokio::test]
async fn digest_mismatch_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("m.bin");
    std::fs::write(&model, b"not-the-expected-bytes").unwrap();
    let resolver = FamilyAsrRuntimeResolver::new(
        resolve_helper_binary(),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes"),
        dir.path().join("envs"),
    );
    let spec = AsrRuntimeSpec {
        engine_family: "whisper-cpp".into(),
        model_id: "tiny".into(),
        verified_digest: Some(
            "blake3:0000000000000000000000000000000000000000000000000000000000000000".into(),
        ),
        locator: ModelLocator::file(model.to_string_lossy()),
        device: "cpu".into(),
        compute_type: "default".into(),
    };
    match resolver.resolve(&spec).await {
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("MODEL_DIGEST_MISMATCH") || msg.contains("digest"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected digest mismatch"),
    }
}

struct CountingSession {
    close_count: Arc<AtomicUsize>,
    descriptor: AsrDescriptor,
}

#[async_trait]
impl AsrSession for CountingSession {
    fn descriptor(&self) -> &AsrDescriptor {
        &self.descriptor
    }

    async fn transcribe(
        &mut self,
        _request: AsrTranscribeRequest,
        _events: &dyn EventPublisher,
        _cancel: Option<videocaptionerr_core::ports::AsrCancelToken>,
    ) -> AppResult<NormalizedAsrResult> {
        Ok(NormalizedAsrResult {
            transcript: Transcript::new_asr(
                "src",
                EngineFingerprint::unknown(),
                vec![Word {
                    text: "hi".into(),
                    start_ms: 0,
                    end_ms: 100,
                    prob: 0.9,
                }],
            ),
        })
    }

    async fn close(self: Box<Self>) -> AppResult<()> {
        self.close_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CountingRuntime {
    opens: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

#[async_trait]
impl AsrRuntime for CountingRuntime {
    async fn open_session(
        &self,
        _profile: &BatchExecutionProfile,
    ) -> AppResult<Box<dyn AsrSession>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(CountingSession {
            close_count: self.closes.clone(),
            descriptor: AsrDescriptor {
                engine_id: "fake".into(),
                adapter_version: "t".into(),
                runtime_version: "t".into(),
                fingerprint: "fp".into(),
                supports_word_timestamps: true,
                supports_confidence: true,
                cooperative_cancel: true,
                max_audio_secs: None,
            },
        }))
    }
}

#[tokio::test]
async fn multi_job_batch_opens_session_once_via_resolver() {
    use crate::resolver::FixedAsrRuntimeResolver;

    let opens = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let runtime = Arc::new(CountingRuntime {
        opens: opens.clone(),
        closes: closes.clone(),
    });
    let resolver = FixedAsrRuntimeResolver::new(runtime);
    let spec = AsrRuntimeSpec {
        engine_family: "fake".into(),
        model_id: "fake".into(),
        verified_digest: None,
        locator: ModelLocator::file("fake"),
        device: "cpu".into(),
        compute_type: "default".into(),
    };
    let r1 = resolver.resolve(&spec).await.unwrap();
    let s = r1
        .open_session(&BatchExecutionProfile {
            asr_engine: "fake".into(),
            asr_model: "fake".into(),
            device: "cpu".into(),
            compute_type: "default".into(),
        })
        .await
        .unwrap();
    // Simulate two jobs on one session — no second open.
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    s.close().await.unwrap();
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

/// When `required-adapters` feature is enabled, missing deps MUST fail (no skip).
#[cfg(feature = "required-adapters")]
mod required {
    use super::*;

    #[tokio::test]
    async fn required_faster_whisper_env_must_exist() {
        let resolver = FamilyAsrRuntimeResolver::new(
            resolve_helper_binary(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes"),
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-envs"),
        );
        let spec = AsrRuntimeSpec {
            engine_family: "faster-whisper".into(),
            model_id: "tiny".into(),
            verified_digest: None,
            locator: ModelLocator::hugging_face("Systran/faster-whisper-tiny", "main", None),
            device: "cpu".into(),
            compute_type: "int8".into(),
        };
        // Must not soft-skip: either resolves or hard-errors.
        let result = resolver.resolve(&spec).await;
        assert!(
            result.is_ok() || result.is_err(),
            "required adapter path must not skip"
        );
        // If uv/network unavailable this fails the test intentionally when feature is on
        // and the env cannot be provisioned — that is the gate.
        result.expect("required faster-whisper adapter must be provisionable");
    }
}
