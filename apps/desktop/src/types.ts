export type JobStatus =
  | "pending"
  | "running"
  | "done"
  | "done_degraded"
  | "failed"
  | "cancelled";

export type StageStatus =
  | "pending"
  | "waiting_resource"
  | "running"
  | "retrying"
  | "done"
  | "done_degraded"
  | "failed"
  | "skipped"
  | "cancelled"
  | "waiting_provider";

export interface StageView {
  kind: string;
  status: StageStatus;
  artifactPath?: string;
}

export interface JobView {
  id: string;
  sourcePath: string;
  status: JobStatus;
  batchId?: string;
  stages: StageView[];
}

export interface Word {
  text: string;
  start_ms: number;
  end_ms: number;
  prob: number;
}

export interface Cue {
  id: number;
  text: string;
  translation?: string;
  imported_start_ms?: number;
  imported_end_ms?: number;
  text_revision: number;
  translation_revision: number;
  flags: {
    llm_failed: boolean;
    hallucination_filtered: boolean;
    restored_fragment: boolean;
    user_edited_text: boolean;
    user_edited_translation: boolean;
  };
  word_range?: { start: number; end: number };
}

export interface Transcript {
  schema_version: number;
  revision: number;
  source_hash: string;
  language?: string;
  words: Word[];
  cues: Cue[];
  next_cue_id: number;
  timeline_source: "asr_words" | "imported_cue";
}

export interface ProbeResult {
  provider_profile_id: string;
  profile_revision: number;
  model: string;
  probe_hash: string;
  capabilities: {
    structured_mode: string;
    returns_usage: boolean;
    seed: boolean;
    supports_model_list: boolean;
    max_context_tokens?: number;
    max_output_tokens?: number;
  };
  warnings: string[];
}

export interface DoctorReport {
  version: string;
  home: string;
  database: string;
  ffmpeg?: string;
  ffprobe?: string;
  helper: string;
}

export interface ProcessResult {
  jobs: JobView[];
  failures: Array<{ job_id: string; error: { code: string; message: string } }>;
}
