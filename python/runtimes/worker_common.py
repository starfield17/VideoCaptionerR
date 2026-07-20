#!/usr/bin/env python3
"""Thin stdio worker used by the managed ASR runtime families.

Business policy stays in Rust. This module only loads an engine, translates
its native result to the shared protocol, and exposes cooperative cancellation.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import wave
from pathlib import Path
from typing import Any

PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 4 * 1024 * 1024


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def duration_ms(path: str) -> int | None:
    try:
        with wave.open(path, "rb") as audio:
            return int(audio.getnframes() * 1000 / audio.getframerate())
    except (OSError, wave.Error, ZeroDivisionError):
        return None


class Worker:
    def __init__(self, engine: str) -> None:
        self.engine = engine
        self.session_id = f"py-{os.getpid()}-{time.time_ns()}"
        self.sequence = 0
        self.output_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.active_request: int | None = None
        self.cancel_events: dict[int, threading.Event] = {}
        self.active_threads: dict[int, threading.Thread] = {}
        self.model: Any = None
        self.model_path: str | None = None
        self.device = "cpu"
        self.compute_type = "default"
        self.stopping = False

    def emit(
        self,
        message_type: str,
        request_id: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        with self.output_lock:
            if self.stopping and message_type != "result":
                return
            envelope = {
                "protocol_version": PROTOCOL_VERSION,
                "session_id": self.session_id,
                "request_id": request_id,
                "seq": self.sequence,
                "type": message_type,
            }
            if data is not None:
                envelope["data"] = data
            self.sequence += 1
            sys.stdout.write(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n")
            sys.stdout.flush()

    def error(self, request_id: int | None, code: str, message: str) -> None:
        self.emit("error", request_id, {"code": code, "message": message})

    def _runtime_version(self) -> str:
        if self.engine == "faster-whisper":
            try:
                import faster_whisper
                import ctranslate2

                fw = getattr(faster_whisper, "__version__", "unknown")
                ct2 = getattr(ctranslate2, "__version__", "unknown")
                return f"faster-whisper={fw};ctranslate2={ct2};python={sys.version.split()[0]}"
            except Exception as exc:  # noqa: BLE001 — report import failure in hello
                return f"faster-whisper-unavailable:{type(exc).__name__}"
        if self.engine == "mlx-whisper":
            try:
                import mlx_whisper

                ver = getattr(mlx_whisper, "__version__", "unknown")
                return f"mlx-whisper={ver};python={sys.version.split()[0]}"
            except Exception as exc:  # noqa: BLE001
                return f"mlx-whisper-unavailable:{type(exc).__name__}"
        return sys.version.split()[0]

    def hello(self) -> None:
        is_mlx = self.engine == "mlx-whisper"
        # MLX must not claim A2 merely because word_timestamps was requested.
        # Only claim word timestamps when the engine actually returns them later;
        # hello advertises capability intent, and Rust validates real words.
        self.emit(
            "hello_ok",
            data={
                "engine_id": self.engine,
                "adapter_version": "1",
                "runtime_version": self._runtime_version(),
                "devices": ["cpu" if not is_mlx else "apple-silicon"],
                "native_vad": self.engine == "faster-whisper",
                "language_detection": True,
                "streaming_events": True,
                "cooperative_cancel": True,
                "max_audio_secs": None,
                "timestamp_granularity": "word",
                # Prefer honest confidence: MLX often lacks word probs.
                "confidence_kind": "none" if is_mlx else "word_prob",
            },
        )

    def load_model(self, request_id: int, data: dict[str, Any]) -> None:
        with self.state_lock:
            if self.active_request is not None:
                self.error(request_id, "WORKER_BUSY", "transcription is active")
                return
        model_path = data.get("model_path")
        device = data.get("device") or ("auto" if self.engine == "faster-whisper" else "cpu")
        compute_type = data.get("compute_type") or "default"
        if model_path is None and self.engine != "fake":
            self.error(request_id, "MODEL_NOT_FOUND", "model_path is required")
            return
        # HuggingFace snapshot form: repo@revision
        if isinstance(model_path, str) and "@" in model_path and not Path(model_path).exists():
            repo, _rev = model_path.split("@", 1)
            model_path = repo
        try:
            if self.engine == "fake":
                model = object()
            elif self.engine == "faster-whisper":
                from faster_whisper import WhisperModel

                model = WhisperModel(
                    str(model_path),
                    device=str(device),
                    compute_type=str(compute_type),
                )
            elif self.engine == "mlx-whisper":
                if sys.platform != "darwin":
                    self.error(
                        request_id,
                        "RUNTIME_UNAVAILABLE",
                        "mlx-whisper real runtime is only available on macOS Apple Silicon",
                    )
                    return
                import mlx_whisper  # noqa: F401

                model = str(model_path)
            else:
                self.error(request_id, "RUNTIME_UNAVAILABLE", f"unsupported engine {self.engine}")
                return
        except Exception as exc:  # runtime import/model errors are boundary errors
            log(f"model load failed: {type(exc).__name__}: {exc}")
            self.error(request_id, "RUNTIME_SMOKE_TEST_FAILED", "ASR model could not be loaded")
            return
        with self.state_lock:
            self.model = model
            self.model_path = str(model_path) if model_path is not None else None
            self.device = str(device)
            self.compute_type = str(compute_type)
        self.emit("result", request_id, {"loaded": True, "device": device, "compute_type": compute_type})

    def unload_model(self, request_id: int) -> None:
        with self.state_lock:
            if self.active_request is not None:
                self.error(request_id, "WORKER_BUSY", "transcription is active")
                return
            self.model = None
            self.model_path = None
        self.emit("result", request_id, {"unloaded": True})

    def start_transcription(self, request_id: int, data: dict[str, Any]) -> None:
        with self.state_lock:
            if self.active_request is not None:
                self.error(request_id, "WORKER_BUSY", "only one transcription is allowed")
                return
            if self.model is None:
                self.error(request_id, "MODEL_NOT_FOUND", "load_model is required before transcribe")
                return
            cancel_event = threading.Event()
            self.active_request = request_id
            self.cancel_events[request_id] = cancel_event
        thread = threading.Thread(
            target=self.transcribe,
            args=(request_id, data, cancel_event),
            name=f"asr-{request_id}",
            daemon=True,
        )
        with self.state_lock:
            self.active_threads[request_id] = thread
        thread.start()

    def cancel(self, data: dict[str, Any]) -> None:
        target = data.get("target_request_id")
        if not isinstance(target, int):
            self.error(None, "WORKER_PROTOCOL_ERROR", "target_request_id must be an integer")
            return
        with self.state_lock:
            event = self.cancel_events.get(target)
        if event is not None:
            event.set()

    def transcribe(
        self,
        request_id: int,
        data: dict[str, Any],
        cancel_event: threading.Event,
    ) -> None:
        try:
            if data.get("inject_delay_ms"):
                deadline = time.monotonic() + float(data["inject_delay_ms"]) / 1000
                while time.monotonic() < deadline:
                    if cancel_event.is_set():
                        self.emit("cancelled", request_id, {"reason": "cooperative_cancel"})
                        return
                    self.emit("progress", request_id, {"message": "waiting for injected delay"})
                    time.sleep(0.05)

            if self.engine == "fake":
                self.fake_transcribe(request_id, data, cancel_event)
            elif self.engine == "faster-whisper":
                self.faster_transcribe(request_id, data, cancel_event)
            elif self.engine == "mlx-whisper":
                self.mlx_transcribe(request_id, data, cancel_event)
            else:
                self.error(request_id, "RUNTIME_UNAVAILABLE", f"unsupported engine {self.engine}")
        except MemoryError:
            self.error(request_id, "ASR_OOM", "ASR runtime ran out of memory")
        except Exception as exc:
            log(f"transcription failed: {type(exc).__name__}: {exc}")
            self.error(request_id, "ASR_FAILED", "ASR runtime transcription failed")
        finally:
            with self.state_lock:
                self.cancel_events.pop(request_id, None)
                self.active_threads.pop(request_id, None)
                if self.active_request == request_id:
                    self.active_request = None

    def fake_transcribe(
        self,
        request_id: int,
        data: dict[str, Any],
        cancel_event: threading.Event,
    ) -> None:
        total = duration_ms(str(data.get("audio_path", "")))
        words = [
            {"text": "hello", "start_ms": 0, "end_ms": 180, "prob": 0.95},
            {"text": "world", "start_ms": 220, "end_ms": 480, "prob": 0.92},
        ]
        if cancel_event.is_set():
            self.emit("cancelled", request_id, {"reason": "cooperative_cancel"})
            return
        segment = {"text": "hello world", "start_ms": 0, "end_ms": 480, "words": words}
        self.emit("segment", request_id, segment)
        self.emit("progress", request_id, {"processed_ms": total or 480, "total_ms": total})
        self.emit("language", request_id, {"language": data.get("language", "en")})
        self.emit("result", request_id, {"language": data.get("language", "en"), "segments": [segment], "duration_ms": total})

    def faster_transcribe(
        self,
        request_id: int,
        data: dict[str, Any],
        cancel_event: threading.Event,
    ) -> None:
        segments, info = self.model.transcribe(
            str(data["audio_path"]),
            language=data.get("language"),
            word_timestamps=bool(data.get("word_timestamps", True)),
            vad_filter=bool(data.get("vad_filter", False)),
        )
        self.emit_segments(request_id, segments, info, cancel_event)

    def mlx_transcribe(
        self,
        request_id: int,
        data: dict[str, Any],
        cancel_event: threading.Event,
    ) -> None:
        import mlx_whisper

        result = mlx_whisper.transcribe(
            str(data["audio_path"]),
            path_or_hf_repo=self.model,
            word_timestamps=bool(data.get("word_timestamps", True)),
            language=data.get("language"),
        )
        self.emit_segments(request_id, result.get("segments", []), result, cancel_event)

    def emit_segments(
        self,
        request_id: int,
        segments: Any,
        info: Any,
        cancel_event: threading.Event,
    ) -> None:
        normalized: list[dict[str, Any]] = []
        saw_words = False
        for segment in segments:
            if cancel_event.is_set():
                self.emit("cancelled", request_id, {"reason": "cooperative_cancel"})
                return
            if isinstance(segment, dict):
                text = str(segment.get("text", "")).strip()
                start = float(segment.get("start", 0))
                end = float(segment.get("end", start))
                native_words = segment.get("words") or []
            else:
                text = str(getattr(segment, "text", "")).strip()
                start = float(getattr(segment, "start", 0))
                end = float(getattr(segment, "end", start))
                native_words = getattr(segment, "words", None) or []
            words = []
            for word in native_words:
                if isinstance(word, dict):
                    token = str(word.get("word", word.get("text", "")))
                    word_start = float(word.get("start", start))
                    word_end = float(word.get("end", word_start))
                    probability = float(word.get("probability", -1.0))
                else:
                    token = str(getattr(word, "word", getattr(word, "text", "")))
                    word_start = float(getattr(word, "start", start))
                    word_end = float(getattr(word, "end", word_start))
                    probability = float(getattr(word, "probability", -1.0))
                if token.strip():
                    saw_words = True
                words.append({
                    "text": token,
                    "start_ms": max(0, round(word_start * 1000)),
                    "end_ms": max(0, round(word_end * 1000)),
                    "prob": probability,
                })
            item = {
                "text": text,
                "start_ms": max(0, round(start * 1000)),
                "end_ms": max(0, round(end * 1000)),
                "words": words,
            }
            normalized.append(item)
            self.emit("segment", request_id, item)
            self.emit("progress", request_id, {"message": "inference"})
        # A2 requires actual word timestamps. Never claim success without them.
        if not saw_words:
            self.error(
                request_id,
                "ENGINE_CAPABILITY_INSUFFICIENT",
                "engine produced no word timestamps; cannot claim A2",
            )
            return
        language = None
        duration = None
        if isinstance(info, dict):
            language = info.get("language")
            duration = info.get("duration")
        else:
            language = getattr(info, "language", None)
            duration = getattr(info, "duration", None)
        self.emit("result", request_id, {
            "language": language,
            "segments": normalized,
            "duration_ms": round(float(duration) * 1000) if duration is not None else None,
        })

    def run(self) -> None:
        for raw_line in sys.stdin:
            if len(raw_line.encode("utf-8")) > MAX_LINE_BYTES:
                self.error(None, "WORKER_PROTOCOL_ERROR", "NDJSON line exceeds 4 MiB")
                continue
            try:
                envelope = json.loads(raw_line)
                if envelope.get("protocol_version") != PROTOCOL_VERSION:
                    self.error(None, "WORKER_PROTOCOL_ERROR", "protocol version mismatch")
                    continue
                message_type = envelope.get("type")
                request_id = envelope.get("request_id")
                data = envelope.get("data") or {}
                if message_type == "hello":
                    self.hello()
                elif message_type == "ping":
                    self.emit("pong", request_id)
                elif message_type == "load_model" and isinstance(request_id, int):
                    self.load_model(request_id, data)
                elif message_type == "unload_model" and isinstance(request_id, int):
                    self.unload_model(request_id)
                elif message_type == "transcribe" and isinstance(request_id, int):
                    self.start_transcription(request_id, data)
                elif message_type == "cancel":
                    self.cancel(data)
                elif message_type == "shutdown":
                    with self.state_lock:
                        active = list(self.cancel_events.values())
                        threads = list(self.active_threads.values())
                    for event in active:
                        event.set()
                    for thread in threads:
                        thread.join(timeout=3.0)
                    with self.state_lock:
                        self.stopping = True
                    if request_id is not None:
                        self.emit("result", request_id, {"shutdown": True})
                    return
                else:
                    self.error(request_id, "WORKER_PROTOCOL_ERROR", f"unsupported message type {message_type}")
            except json.JSONDecodeError:
                self.error(request_id if "request_id" in locals() else None, "WORKER_PROTOCOL_ERROR", "invalid JSON")
            except Exception as exc:
                log(f"protocol handling failed: {type(exc).__name__}: {exc}")
                self.error(None, "WORKER_PROTOCOL_ERROR", "worker request failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True)
    args = parser.parse_args()
    Worker(args.engine).run()


if __name__ == "__main__":
    main()
