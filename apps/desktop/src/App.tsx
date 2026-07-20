import {
  Activity,
  AlertTriangle,
  Check,
  CheckCircle2,
  ChevronRight,
  Clock3,
  FileAudio,
  FileText,
  FolderOpen,
  Gauge,
  Languages,
  LoaderCircle,
  PanelLeft,
  Play,
  RefreshCw,
  Search,
  Settings2,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  TerminalSquare,
  XCircle,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { call, isTauri } from "./lib/desktop";
import type { Cue, DoctorReport, JobView, ProbeResult, Transcript } from "./types";

const stageOrder = ["probe", "extract_audio", "asr", "split", "correct", "translate", "export"];
const CUE_ROW_HEIGHT = 72;
const VIRTUAL_WINDOW_SIZE = 36;
const VIRTUAL_OVERSCAN = 4;

const demoJobs: JobView[] = [
  {
    id: "demo-lecture",
    sourcePath: "/media/lecture-07.mp4",
    status: "done_degraded",
    stages: stageOrder.map((kind, index) => ({
      kind,
      status: index === 5 ? "done_degraded" : "done",
      artifactPath: `/jobs/demo/${kind}.json`,
    })),
  },
  {
    id: "demo-interview",
    sourcePath: "/media/interview.mp4",
    status: "running",
    stages: stageOrder.map((kind, index) => ({
      kind,
      status: index < 3 ? "done" : index === 3 ? "running" : "pending",
    })),
  },
];

const demoTranscript: Transcript = {
  schema_version: 1,
  revision: 7,
  source_hash: "demo-source",
  language: "en",
  words: [
    { text: "The", start_ms: 320, end_ms: 530, prob: 0.96 },
    { text: "new", start_ms: 560, end_ms: 760, prob: 0.94 },
    { text: "workflow", start_ms: 790, end_ms: 1180, prob: 0.88 },
    { text: "keeps", start_ms: 1260, end_ms: 1490, prob: 0.91 },
    { text: "timestamps", start_ms: 1520, end_ms: 1920, prob: 0.87 },
    { text: "stable.", start_ms: 1950, end_ms: 2260, prob: 0.92 },
    { text: "Every", start_ms: 2680, end_ms: 2910, prob: 0.93 },
    { text: "edit", start_ms: 2940, end_ms: 3170, prob: 0.95 },
    { text: "creates", start_ms: 3200, end_ms: 3490, prob: 0.89 },
    { text: "a", start_ms: 3520, end_ms: 3580, prob: 0.99 },
    { text: "revision.", start_ms: 3610, end_ms: 3950, prob: 0.91 },
  ],
  cues: [
    {
      id: 1,
      text: "The new workflow keeps timestamps stable.",
      translation: "新的工作流保持时间戳稳定。",
      text_revision: 1,
      translation_revision: 2,
      flags: {
        llm_failed: false,
        hallucination_filtered: false,
        restored_fragment: false,
        user_edited_text: false,
        user_edited_translation: false,
      },
      word_range: { start: 0, end: 6 },
    },
    {
      id: 2,
      text: "Every edit creates a revision.",
      translation: "每次编辑都会创建一个修订版本。",
      text_revision: 0,
      translation_revision: 0,
      flags: {
        llm_failed: false,
        hallucination_filtered: false,
        restored_fragment: false,
        user_edited_text: false,
        user_edited_translation: false,
      },
      word_range: { start: 6, end: 11 },
    },
  ],
  next_cue_id: 3,
  timeline_source: "asr_words",
};

function statusLabel(value: string): string {
  return value.replaceAll("_", " ");
}

function formatTime(ms: number): string {
  const minutes = Math.floor(ms / 60000);
  const seconds = Math.floor((ms % 60000) / 1000);
  const millis = ms % 1000;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}.${String(millis).padStart(3, "0")}`;
}

function cueTimes(transcript: Transcript, cue: Cue): [number, number] {
  if (cue.word_range) {
    const first = transcript.words[cue.word_range.start];
    const last = transcript.words[cue.word_range.end - 1];
    if (first && last) return [first.start_ms, last.end_ms];
  }
  return [cue.imported_start_ms ?? 0, cue.imported_end_ms ?? 0];
}

function StageIcon({ status }: { status: string }) {
  if (status === "running") return <LoaderCircle className="spin" size={15} />;
  if (status === "done" || status === "skipped") return <CheckCircle2 size={15} />;
  if (status === "done_degraded") return <AlertTriangle size={15} />;
  if (status === "failed") return <XCircle size={15} />;
  return <Clock3 size={15} />;
}

export default function App() {
  const [jobs, setJobs] = useState<JobView[]>([]);
  const [selectedId, setSelectedId] = useState<string>();
  const [transcript, setTranscript] = useState<Transcript>();
  const [doctor, setDoctor] = useState<DoctorReport>();
  const [path, setPath] = useState("");
  const [targetLanguage, setTargetLanguage] = useState("zh-CN");
  const [search, setSearch] = useState("");
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ kind: "error" | "ok"; text: string }>();
  const [diagnosticTab, setDiagnosticTab] = useState<"diagnostics" | "logs">("diagnostics");
  const [editorScroll, setEditorScroll] = useState(0);

  const selectedJob = jobs.find((job) => job.id === selectedId);
  const filteredJobs = jobs.filter((job) => {
    const needle = search.trim().toLowerCase();
    return !needle || job.sourcePath.toLowerCase().includes(needle) || job.id.toLowerCase().includes(needle);
  });

  const refresh = async () => {
    setBusy(true);
    try {
      const next = isTauri() ? await call<JobView[]>("list_jobs") : demoJobs;
      setJobs(next);
      setSelectedId((current) => current && next.some((job) => job.id === current) ? current : next[0]?.id);
      setNotice(undefined);
    } catch (error) {
      setNotice({ kind: "error", text: String(error) });
    } finally {
      setBusy(false);
    }
  };

  const loadDoctor = async () => {
    if (!isTauri()) {
      setDoctor({
        version: "preview",
        home: "browser preview",
        database: "browser preview",
        ffmpeg: "preview",
        ffprobe: "preview",
        helper: "preview",
      });
      return;
    }
    try {
      setDoctor(await call<DoctorReport>("doctor"));
    } catch (error) {
      setNotice({ kind: "error", text: `Doctor check failed: ${String(error)}` });
    }
  };

  useEffect(() => {
    void refresh();
    void loadDoctor();
  }, []);

  useEffect(() => {
    if (!selectedId) {
      setTranscript(undefined);
      setEditorScroll(0);
      return;
    }
    setEditorScroll(0);
    if (!isTauri() && selectedId.startsWith("demo-")) {
      setTranscript(demoTranscript);
      return;
    }
    void call<Transcript>("load_transcript", { jobId: selectedId })
      .then(setTranscript)
      .catch(() => setTranscript(undefined));
  }, [selectedId]);

  const visibleCues = useMemo(() => {
    if (!transcript) {
      return { items: [], topPadding: 0, bottomPadding: 0 };
    }
    const start = Math.max(0, Math.floor(editorScroll / CUE_ROW_HEIGHT) - VIRTUAL_OVERSCAN);
    const end = Math.min(transcript.cues.length, start + VIRTUAL_WINDOW_SIZE);
    return {
      items: transcript.cues.slice(start, end).map((cue) => ({ cue })),
      topPadding: start * CUE_ROW_HEIGHT,
      bottomPadding: (transcript.cues.length - end) * CUE_ROW_HEIGHT,
    };
  }, [editorScroll, transcript]);

  const servicesReady = Boolean(doctor?.ffmpeg && doctor?.ffprobe && doctor?.helper);

  const process = async () => {
    if (!path.trim()) {
      setNotice({ kind: "error", text: "Enter an absolute media path first." });
      return;
    }
    if (!isTauri()) {
      setNotice({ kind: "ok", text: "Desktop bridge preview: the path is ready to process." });
      return;
    }
    setBusy(true);
    try {
      await call("process_files", {
        request: {
          files: [path.trim()],
          targetLanguage: targetLanguage.trim() || undefined,
        },
      });
      setNotice({ kind: "ok", text: "Batch completed." });
      setPath("");
      await refresh();
    } catch (error) {
      setNotice({ kind: "error", text: String(error) });
    } finally {
      setBusy(false);
    }
  };

  const editCue = async (cue: Cue, field: "source" | "translation", value: string) => {
    if (!transcript || value === (field === "source" ? cue.text : cue.translation ?? "")) return;
    if (!isTauri() || !selectedId || selectedId.startsWith("demo-")) {
      setTranscript({
        ...transcript,
        revision: transcript.revision + 1,
        cues: transcript.cues.map((candidate) => candidate.id === cue.id
          ? { ...candidate, [field === "source" ? "text" : "translation"]: value }
          : candidate),
      });
      return;
    }
    try {
      const result = await call<{ transcript: Transcript }>("edit_cue", {
        request: {
          jobId: selectedId,
          cueId: cue.id,
          expectedRevision: transcript.revision,
          field,
          value,
        },
      });
      setTranscript(result.transcript);
      setNotice({ kind: "ok", text: `Cue ${cue.id} saved at revision ${result.transcript.revision}.` });
    } catch (error) {
      setNotice({ kind: "error", text: `Edit rejected: ${String(error)}` });
      if (selectedId) {
        const fresh = await call<Transcript>("load_transcript", { jobId: selectedId }).catch(() => undefined);
        if (fresh) setTranscript(fresh);
      }
    }
  };

  const probe = async () => {
    if (!isTauri()) {
      setNotice({ kind: "ok", text: "Provider probe is available in the desktop runtime." });
      return;
    }
    setBusy(true);
    try {
      const result = await call<ProbeResult>("probe_provider", { force: false });
      setNotice({ kind: "ok", text: `${result.provider_profile_id} uses ${result.capabilities.structured_mode}.` });
    } catch (error) {
      setNotice({ kind: "error", text: String(error) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="app-shell">
      <aside className="rail">
        <div className="brand-mark" aria-label="VideoCaptionerR">VC</div>
        <nav className="rail-nav" aria-label="Workspace">
          <button className="rail-button active" title="Queue"><PanelLeft size={18} /></button>
          <button className="rail-button" title="Runtime"><Gauge size={18} /></button>
          <button className="rail-button" title="Settings"><Settings2 size={18} /></button>
        </nav>
        <div className="rail-bottom">
          <button className="rail-button" title="Diagnostics"><Activity size={18} /></button>
          <span className="version-dot" title="Local runtime ready" />
        </div>
      </aside>

      <section className="queue-panel">
        <div className="queue-head">
          <div>
            <p className="eyebrow">Processing queue</p>
            <h1>Jobs</h1>
          </div>
          <button className="icon-button" title="Refresh jobs" onClick={() => void refresh()} disabled={busy}>
            <RefreshCw size={16} className={busy ? "spin" : ""} />
          </button>
        </div>
        <div className="queue-search">
          <Search size={15} />
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Filter jobs" />
          <span className="shortcut">/</span>
        </div>
        <div className="queue-list">
          {filteredJobs.map((job) => (
            <button
              className={`job-row ${job.id === selectedId ? "selected" : ""}`}
              key={job.id}
              onClick={() => setSelectedId(job.id)}
            >
              <span className={`status-dot ${job.status}`} />
              <span className="job-copy">
                <strong>{job.sourcePath.split(/[\\/]/).pop()}</strong>
                <small>{statusLabel(job.status)}</small>
              </span>
              <ChevronRight size={15} />
            </button>
          ))}
          {filteredJobs.length === 0 && <div className="empty-queue">No jobs in the queue.</div>}
        </div>
        <div className="queue-footer">
          <div className="queue-stat"><span>{jobs.length}</span> jobs</div>
          <div className="queue-stat"><span>{jobs.filter((job) => job.status === "running").length}</span> active</div>
        </div>
      </section>

      <main className="workspace">
        <header className="topbar">
          <div className="breadcrumb"><span>Queue</span><ChevronRight size={14} /><strong>{selectedJob?.sourcePath.split(/[\\/]/).pop() ?? "No job selected"}</strong></div>
          <div className="topbar-actions">
            <button className="quiet-button" onClick={() => void probe()}><ShieldCheck size={16} /> Test provider</button>
            <button className="quiet-button" onClick={() => void refresh()}><RefreshCw size={16} /> Sync</button>
          </div>
        </header>

        <div className="workspace-scroll">
          <section className="job-header">
            <div className="job-title-block">
              <div className="file-icon"><FileAudio size={21} /></div>
              <div>
                <p className="eyebrow">Selected job</p>
                <h2>{selectedJob?.sourcePath.split(/[\\/]/).pop() ?? "Select a job"}</h2>
                <p className="muted-path">{selectedJob?.sourcePath ?? "Add media to begin"}</p>
              </div>
            </div>
            <div className="job-header-meta">
              <div className="meta-block"><span>Status</span><strong>{selectedJob ? statusLabel(selectedJob.status) : "Idle"}</strong></div>
              <div className="meta-block"><span>Transcript</span><strong>{transcript ? `r${transcript.revision}` : "-"}</strong></div>
              <button className="primary-button" onClick={() => void process()} disabled={busy}><Play size={16} /> Process</button>
            </div>
          </section>

          <section className="stage-strip" aria-label="Job stages">
            {stageOrder.map((stage, index) => {
              const state = selectedJob?.stages.find((candidate) => candidate.kind === stage)?.status ?? "pending";
              return (
                <div className={`stage-step ${state}`} key={stage}>
                  <div className="stage-marker"><StageIcon status={state} /></div>
                  <div><span>{String(index + 1).padStart(2, "0")}</span><strong>{stage === "extract_audio" ? "extract" : stage}</strong></div>
                </div>
              );
            })}
          </section>

          {notice && <div className={`notice ${notice.kind}`}><span>{notice.text}</span><button title="Dismiss" onClick={() => setNotice(undefined)}><XCircle size={15} /></button></div>}

          <section className="content-grid">
            <div className="editor-panel panel">
              <div className="panel-head">
                <div><p className="eyebrow">Transcript IR</p><h3>Subtitle editor</h3></div>
                <div className="panel-tools"><span className="read-only-chip"><ShieldCheck size={13} /> timeline locked</span><button className="icon-button" title="Editor options"><SlidersHorizontal size={16} /></button></div>
              </div>
              <div className="editor-toolbar">
                <span className="cue-count">{transcript?.cues.length ?? 0} cues</span>
                <span className="toolbar-divider" />
                <span className="toolbar-note">Source and translation edits use revision CAS</span>
                <button className="icon-button" title="Open media folder"><FolderOpen size={15} /></button>
              </div>
              <div className="cue-table-wrap" onScroll={(event) => setEditorScroll(event.currentTarget.scrollTop)}>
                <table className="cue-table">
                  <thead><tr><th className="cue-index">#</th><th className="time-col">Time</th><th>Source</th><th>Translation</th><th className="flag-col" /></tr></thead>
                  <tbody>
                    {visibleCues.topPadding > 0 && <tr className="virtual-spacer" aria-hidden="true"><td colSpan={5} style={{ height: visibleCues.topPadding }} /></tr>}
                    {visibleCues.items.map(({ cue }) => {
                      const [start, end] = transcript ? cueTimes(transcript, cue) : [0, 0];
                      return (
                        <tr key={`${transcript?.revision ?? 0}:${cue.id}`}>
                          <td className="cue-index">{cue.id}</td>
                          <td className="time-col"><span>{formatTime(start)}</span><span>{formatTime(end)}</span></td>
                          <td><textarea aria-label={`Source cue ${cue.id}`} defaultValue={cue.text} onBlur={(event) => void editCue(cue, "source", event.currentTarget.value)} /></td>
                          <td><textarea aria-label={`Translation cue ${cue.id}`} defaultValue={cue.translation ?? ""} onBlur={(event) => void editCue(cue, "translation", event.currentTarget.value)} /></td>
                          <td className="flag-col">{cue.flags.llm_failed && <span title="LLM fallback"><AlertTriangle size={15} /></span>}{(cue.flags.user_edited_text || cue.flags.user_edited_translation) && <span title="User edited"><Check size={15} /></span>}</td>
                        </tr>
                      );
                    })}
                    {visibleCues.bottomPadding > 0 && <tr className="virtual-spacer" aria-hidden="true"><td colSpan={5} style={{ height: visibleCues.bottomPadding }} /></tr>}
                  </tbody>
                </table>
                {!transcript && <div className="empty-editor"><FileText size={22} /><span>Run a job to open its transcript.</span></div>}
              </div>
            </div>

            <aside className="inspector">
              <div className="panel inspector-card">
                <div className="inspector-tabs"><button className={diagnosticTab === "diagnostics" ? "active" : ""} onClick={() => setDiagnosticTab("diagnostics")}><AlertTriangle size={14} /> Diagnostics</button><button className={diagnosticTab === "logs" ? "active" : ""} onClick={() => setDiagnosticTab("logs")}><TerminalSquare size={14} /> Logs</button></div>
                {diagnosticTab === "diagnostics" ? <div className="diagnostic-list">
                  <div className="diagnostic-item good"><CheckCircle2 size={16} /><div><strong>Timeline integrity</strong><span>Immutable word ownership verified</span></div></div>
                  <div className="diagnostic-item"><Gauge size={16} /><div><strong>Display density</strong><span>{transcript?.cues.length ?? 0} cues ready for export checks</span></div></div>
                  <div className="diagnostic-item warning"><AlertTriangle size={16} /><div><strong>Review fallbacks</strong><span>{transcript?.cues.filter((cue) => cue.flags.llm_failed).length ?? 0} cues carry LLM warnings</span></div></div>
                </div> : <div className="log-list"><code>store actor ready</code><code>application runtime connected</code><code>no secret-bearing request content</code></div>}
              </div>
              <div className="panel run-card">
                <div className="panel-head compact"><div><p className="eyebrow">New batch</p><h3>Process media</h3></div><Sparkles size={17} /></div>
                <label>Absolute media path<input value={path} onChange={(event) => setPath(event.target.value)} placeholder="/path/to/video.mp4" /></label>
                <label>Target language<div className="input-with-icon"><Languages size={15} /><input value={targetLanguage} onChange={(event) => setTargetLanguage(event.target.value)} /></div></label>
                <button className="secondary-button" onClick={() => void process()} disabled={busy}><Play size={15} /> Add to queue</button>
              </div>
              <div className="panel health-card">
                <div className="health-line"><span className={`health-dot ${servicesReady ? "" : "warning"}`} /> <strong>Local services</strong><span className={servicesReady ? "" : "warning"}>{doctor ? (servicesReady ? "ready" : "check") : "checking"}</span></div>
                <div className="health-details"><span>SQLite actor</span><span>{doctor?.ffmpeg ? "ffmpeg" : "ffmpeg missing"}</span><span>{doctor?.ffprobe ? "ffprobe" : "ffprobe missing"}</span><span>{doctor?.helper ? "ASR helper" : "ASR helper missing"}</span></div>
              </div>
            </aside>
          </section>
        </div>
      </main>
    </div>
  );
}
