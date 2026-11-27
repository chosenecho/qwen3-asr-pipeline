#!/usr/bin/env python3
"""Simple MP4 -> Audio extraction & ASR transcription state pipeline backed by SQLite.

Zero third-party dependencies (Python 3.10+ stdlib + ffmpeg/ffprobe).
Stores audio & transcript files on the filesystem and tracks both audio extraction
and ASR transcription progress in SQLite.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import mimetypes
import os
import queue
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = REPO_ROOT / "data" / "pipeline.db"
DEFAULT_AUDIO_DIR = REPO_ROOT / "data" / "audio"
DEFAULT_TRANSCRIPT_DIR = REPO_ROOT / "data" / "transcripts"


def load_dotenv(env_path: Path | None = None) -> None:
    path = env_path or (REPO_ROOT / ".env")
    if not path.is_file():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception:
        pass

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".webm", ".flv", ".avi", ".m4v", ".ts"}
DONE_ASR_STATUSES = {"completed", "succeeded", "success", "done", "finished"}
FAIL_ASR_STATUSES = {"failed", "error", "cancelled", "canceled"}

BV_PATTERN = re.compile(r"\[(BV[0-9A-Za-z_]+|av\d+|[A-Za-z0-9_-]{8,})\]")


@dataclass(frozen=True)
class AudioPreset:
    name: str
    ext: str
    ffmpeg_args: tuple[str, ...]
    fallback_args: tuple[str, ...] | None = None
    description: str = ""


AUDIO_PRESETS: dict[str, AudioPreset] = {
    "asr-mp3": AudioPreset(
        name="asr-mp3",
        ext=".mp3",
        ffmpeg_args=("-vn", "-ac", "1", "-ar", "16000", "-c:a", "libmp3lame", "-b:a", "48k"),
        description="16kHz mono 48kbps MP3 (compact, ideal for ASR & fast upload)",
    ),
    "copy": AudioPreset(
        name="copy",
        ext=".m4a",
        ffmpeg_args=("-vn", "-c:a", "copy", "-movflags", "+faststart"),
        fallback_args=("-vn", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart"),
        description="Lossless stream-copy to M4A (fastest, falls back to AAC 128k if needed)",
    ),
    "opus": AudioPreset(
        name="opus",
        ext=".opus",
        ffmpeg_args=("-vn", "-ac", "1", "-ar", "16000", "-c:a", "libopus", "-b:a", "32k", "-application", "voip"),
        description="16kHz mono 32kbps Opus (smallest footprint)",
    ),
    "wav": AudioPreset(
        name="wav",
        ext=".wav",
        ffmpeg_args=("-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le"),
        description="16kHz mono 16-bit PCM WAV (uncompressed)",
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_video_id(stem: str) -> str:
    m = BV_PATTERN.search(stem)
    return m.group(1) if m else stem


def format_bytes(num_bytes: int | float | None) -> str:
    if num_bytes is None:
        return "0 B"
    n = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.2f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024.0
    return f"{n:.2f} PB"


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds <= 0:
        return "0s"
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def probe_duration_sec(file_path: Path) -> float | None:
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
        if proc.returncode != 0:
            return None
        val = float(proc.stdout.strip())
        return val if val >= 0 else None
    except Exception:
        return None


SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 30000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS media_items (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    source_group      TEXT NOT NULL,
    video_id          TEXT NOT NULL,
    title             TEXT NOT NULL,
    video_path        TEXT NOT NULL UNIQUE,
    video_rel_path    TEXT NOT NULL,
    video_size_bytes  INTEGER NOT NULL DEFAULT 0,
    video_mtime       REAL NOT NULL DEFAULT 0,
    duration_sec      REAL,

    -- Audio extraction state
    audio_status      TEXT NOT NULL DEFAULT 'pending',
    audio_preset      TEXT,
    audio_path        TEXT,
    audio_format      TEXT,
    audio_size_bytes  INTEGER,
    audio_error       TEXT,
    audio_started_at  TEXT,
    audio_finished_at TEXT,

    -- ASR transcription progress state
    asr_status        TEXT NOT NULL DEFAULT 'pending',
    asr_mode          TEXT,
    asr_server_url    TEXT,
    asr_task_id       TEXT,
    asr_progress      REAL NOT NULL DEFAULT 0.0,
    asr_message       TEXT,
    txt_path          TEXT,
    srt_path          TEXT,
    json_path         TEXT,
    asr_error         TEXT,
    asr_attempts      INTEGER NOT NULL DEFAULT 0,
    asr_started_at    TEXT,
    asr_updated_at    TEXT,
    asr_finished_at   TEXT,

    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_media_group ON media_items (source_group);
CREATE INDEX IF NOT EXISTS idx_media_audio_status ON media_items (source_group, audio_status);
CREATE INDEX IF NOT EXISTS idx_media_asr_status ON media_items (source_group, asr_status);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type      TEXT NOT NULL,
    source_group  TEXT,
    config_json   TEXT,
    total_count   INTEGER NOT NULL DEFAULT 0,
    done_count    INTEGER NOT NULL DEFAULT 0,
    failed_count  INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'running',
    started_at    TEXT NOT NULL,
    finished_at   TEXT
);
"""


class StateDB:
    """Thread-safe SQLite state store for extraction and transcription progress."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path.resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.executescript(SCHEMA_SQL)
            self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def create_run(self, run_type: str, source_group: str | None, config: dict[str, Any], total_count: int) -> int:
        now = utc_now()
        with self._lock:
            cur = self.conn.execute(
                """
                INSERT INTO pipeline_runs (run_type, source_group, config_json, total_count, status, started_at)
                VALUES (?, ?, ?, ?, 'running', ?)
                """,
                (run_type, source_group, json.dumps(config, ensure_ascii=False), total_count, now),
            )
            self.conn.commit()
            return int(cur.lastrowid)

    def finish_run(
        self,
        run_id: int,
        *,
        done_count: int,
        failed_count: int,
        skipped_count: int,
        status: str,
    ) -> None:
        with self._lock:
            self.conn.execute(
                """
                UPDATE pipeline_runs
                SET done_count = ?, failed_count = ?, skipped_count = ?, status = ?, finished_at = ?
                WHERE id = ?
                """,
                (done_count, failed_count, skipped_count, status, utc_now(), run_id),
            )
            self.conn.commit()

    def upsert_videos(
        self,
        source_group: str,
        records: list[tuple[str, str, str, str, int, float]],
    ) -> int:
        """Upsert discovered video files. Each tuple: (video_id, title, video_path, video_rel_path, size, mtime)."""
        now = utc_now()
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO media_items (
                    source_group, video_id, title, video_path, video_rel_path,
                    video_size_bytes, video_mtime, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_path) DO UPDATE SET
                    source_group = excluded.source_group,
                    video_id = excluded.video_id,
                    title = excluded.title,
                    video_rel_path = excluded.video_rel_path,
                    video_size_bytes = excluded.video_size_bytes,
                    video_mtime = excluded.video_mtime,
                    updated_at = excluded.updated_at
                """,
                [
                    (source_group, vid, title, vpath, vrel, vsize, vmtime, now, now)
                    for (vid, title, vpath, vrel, vsize, vmtime) in records
                ],
            )
            self.conn.commit()
        return len(records)

    def reset_stale_running_audio(self, source_group: str | None = None) -> int:
        now = utc_now()
        with self._lock:
            if source_group:
                cur = self.conn.execute(
                    "UPDATE media_items SET audio_status = 'pending', updated_at = ? WHERE source_group = ? AND audio_status = 'running'",
                    (now, source_group),
                )
            else:
                cur = self.conn.execute(
                    "UPDATE media_items SET audio_status = 'pending', updated_at = ? WHERE audio_status = 'running'",
                    (now,),
                )
            self.conn.commit()
            return cur.rowcount

    def get_items_for_extraction(
        self,
        source_group: str,
        *,
        overwrite: bool,
        retry_failed: bool,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        with self._lock:
            if overwrite:
                sql = "SELECT * FROM media_items WHERE source_group = ? ORDER BY video_rel_path"
                params: list[Any] = [source_group]
            elif retry_failed:
                sql = """
                    SELECT * FROM media_items
                    WHERE source_group = ? AND audio_status IN ('pending', 'failed', 'running')
                    ORDER BY video_rel_path
                """
                params = [source_group]
            else:
                sql = """
                    SELECT * FROM media_items
                    WHERE source_group = ? AND audio_status IN ('pending', 'running')
                    ORDER BY video_rel_path
                """
                params = [source_group]
            if limit and limit > 0:
                sql += " LIMIT ?"
                params.append(limit)
            return list(self.conn.execute(sql, params).fetchall())

    def get_all_group_items(self, source_group: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    "SELECT * FROM media_items WHERE source_group = ? ORDER BY video_rel_path",
                    (source_group,),
                ).fetchall()
            )

    def mark_audio_running(self, item_id: int, preset: str, audio_path: Path) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET audio_status = 'running',
                    audio_preset = ?,
                    audio_path = ?,
                    audio_error = NULL,
                    audio_started_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (preset, str(audio_path), now, now, item_id),
            )
            self.conn.commit()

    def mark_audio_done(
        self,
        item_id: int,
        *,
        preset: str,
        audio_path: Path,
        audio_format: str,
        audio_size_bytes: int,
        duration_sec: float | None,
    ) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET audio_status = 'done',
                    audio_preset = ?,
                    audio_path = ?,
                    audio_format = ?,
                    audio_size_bytes = ?,
                    duration_sec = COALESCE(?, duration_sec),
                    audio_error = NULL,
                    audio_finished_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (preset, str(audio_path), audio_format, audio_size_bytes, duration_sec, now, now, item_id),
            )
            self.conn.commit()

    def mark_audio_pending(self, item_id: int) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET audio_status = 'pending',
                    audio_error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, item_id),
            )
            self.conn.commit()

    def mark_audio_failed(self, item_id: int, error: str) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET audio_status = 'failed',
                    audio_error = ?,
                    audio_finished_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (error[:2000], now, now, item_id),
            )
            self.conn.commit()

    # ---- ASR transcription progress updates ----

    def get_items_for_transcription(
        self,
        *,
        source_group: str | None,
        overwrite: bool,
        retry_failed: bool,
        item_ids: list[int] | None = None,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        clauses = ["audio_status = 'done'", "audio_path IS NOT NULL"]
        params: list[Any] = []
        if source_group:
            clauses.append("source_group = ?")
            params.append(source_group)
        if item_ids:
            placeholders = ",".join("?" for _ in item_ids)
            clauses.append(f"id IN ({placeholders})")
            params.extend(item_ids)
        if not overwrite:
            if retry_failed:
                clauses.append("asr_status IN ('pending', 'queued', 'running', 'failed')")
            else:
                clauses.append("asr_status IN ('pending', 'queued', 'running')")
        sql = f"SELECT * FROM media_items WHERE {' AND '.join(clauses)} ORDER BY source_group, video_rel_path"
        if limit and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            return list(self.conn.execute(sql, params).fetchall())

    def count_unfinished_audio(self, source_group: str | None) -> int:
        clauses = ["audio_status IN ('pending', 'running')"]
        params: list[Any] = []
        if source_group:
            clauses.append("source_group = ?")
            params.append(source_group)
        sql = f"SELECT COUNT(*) FROM media_items WHERE {' AND '.join(clauses)}"
        with self._lock:
            row = self.conn.execute(sql, params).fetchone()
            return int(row[0]) if row else 0

    def mark_asr_started(
        self,
        item_id: int,
        *,
        mode: str,
        server_url: str,
        status: str = "running",
        progress: float = 0.0,
        message: str | None = None,
        task_id: str | None = None,
    ) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET asr_status = ?,
                    asr_mode = ?,
                    asr_server_url = ?,
                    asr_task_id = COALESCE(?, asr_task_id),
                    asr_progress = ?,
                    asr_message = ?,
                    asr_error = NULL,
                    asr_attempts = asr_attempts + 1,
                    asr_started_at = COALESCE(asr_started_at, ?),
                    asr_updated_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, mode, server_url, task_id, progress, message, now, now, now, item_id),
            )
            self.conn.commit()

    def update_asr_progress(
        self,
        item_id: int,
        *,
        status: str,
        progress: float | None = None,
        message: str | None = None,
        task_id: str | None = None,
    ) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET asr_status = ?,
                    asr_task_id = COALESCE(?, asr_task_id),
                    asr_progress = COALESCE(?, asr_progress),
                    asr_message = COALESCE(?, asr_message),
                    asr_updated_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, task_id, progress, message, now, now, item_id),
            )
            self.conn.commit()

    def mark_asr_done(
        self,
        item_id: int,
        *,
        txt_path: Path,
        srt_path: Path,
        json_path: Path | None = None,
    ) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET asr_status = 'done',
                    asr_progress = 100.0,
                    asr_message = 'completed',
                    txt_path = ?,
                    srt_path = ?,
                    json_path = ?,
                    asr_error = NULL,
                    asr_updated_at = ?,
                    asr_finished_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    str(txt_path),
                    str(srt_path),
                    str(json_path) if json_path else None,
                    now,
                    now,
                    now,
                    item_id,
                ),
            )
            self.conn.commit()

    def mark_asr_failed(self, item_id: int, error: str) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET asr_status = 'failed',
                    asr_error = ?,
                    asr_updated_at = ?,
                    asr_finished_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (error[:2000], now, now, now, item_id),
            )
            self.conn.commit()

    def reset_items(self, stage: str, statuses: list[str], source_group: str | None = None) -> int:
        now = utc_now()
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [now]
        if stage == "audio":
            sql = f"""
                UPDATE media_items
                SET audio_status = 'pending', audio_error = NULL, updated_at = ?
                WHERE audio_status IN ({placeholders})
            """
        else:
            sql = f"""
                UPDATE media_items
                SET asr_status = 'pending', asr_progress = 0.0, asr_task_id = NULL,
                    asr_message = NULL, asr_error = NULL, updated_at = ?
                WHERE asr_status IN ({placeholders})
            """
        params.extend(statuses)
        if source_group:
            sql += " AND source_group = ?"
            params.append(source_group)
        with self._lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur.rowcount

    def get_summary(self, source_group: str | None = None) -> list[dict[str, Any]]:
        where = "WHERE source_group = ?" if source_group else ""
        params = [source_group] if source_group else []
        sql = f"""
            SELECT
                source_group,
                COUNT(*) AS total,
                SUM(CASE WHEN audio_status = 'done' THEN 1 ELSE 0 END) AS audio_done,
                SUM(CASE WHEN audio_status = 'pending' THEN 1 ELSE 0 END) AS audio_pending,
                SUM(CASE WHEN audio_status = 'running' THEN 1 ELSE 0 END) AS audio_running,
                SUM(CASE WHEN audio_status = 'failed' THEN 1 ELSE 0 END) AS audio_failed,
                SUM(CASE WHEN asr_status = 'done' THEN 1 ELSE 0 END) AS asr_done,
                SUM(CASE WHEN asr_status IN ('running', 'queued') THEN 1 ELSE 0 END) AS asr_in_progress,
                SUM(CASE WHEN asr_status = 'pending' THEN 1 ELSE 0 END) AS asr_pending,
                SUM(CASE WHEN asr_status = 'failed' THEN 1 ELSE 0 END) AS asr_failed,
                COALESCE(SUM(video_size_bytes), 0) AS video_bytes,
                COALESCE(SUM(audio_size_bytes), 0) AS audio_bytes,
                COALESCE(SUM(duration_sec), 0) AS duration_sec,
                COALESCE(AVG(CASE WHEN audio_status = 'done' THEN asr_progress END), 0) AS avg_asr_progress
            FROM media_items
            {where}
            GROUP BY source_group
            ORDER BY source_group
        """
        with self._lock:
            return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def get_recent_failures(self, source_group: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        where = "WHERE (audio_status = 'failed' OR asr_status = 'failed')"
        params: list[Any] = []
        if source_group:
            where += " AND source_group = ?"
            params.append(source_group)
        params.append(limit)
        sql = f"""
            SELECT id, source_group, video_id, title, audio_status, audio_error, asr_status, asr_error, updated_at
            FROM media_items
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
        """
        with self._lock:
            return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def list_items(
        self,
        *,
        source_group: str | None = None,
        audio_status: str | None = None,
        asr_status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source_group:
            clauses.append("source_group = ?")
            params.append(source_group)
        if audio_status:
            clauses.append("audio_status = ?")
            params.append(audio_status)
        if asr_status:
            clauses.append("asr_status = ?")
            params.append(asr_status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        sql = f"""
            SELECT id, source_group, video_id, title, duration_sec,
                   audio_status, audio_size_bytes, audio_path,
                   asr_status, asr_progress, asr_task_id, txt_path, srt_path,
                   audio_error, asr_error
            FROM media_items
            {where}
            ORDER BY id
            LIMIT ?
        """
        with self._lock:
            return [dict(row) for row in self.conn.execute(sql, params).fetchall()]


def scan_videos(input_path: Path, recursive: bool = True) -> tuple[Path, list[Path]]:
    resolved = input_path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Input path does not exist: {resolved}")
    if resolved.is_file():
        return resolved.parent, [resolved]
    iterator = resolved.rglob("*") if recursive else resolved.glob("*")
    files = sorted(
        p for p in iterator if p.is_file() and p.suffix.lower() in VIDEO_EXTENSIONS
    )
    return resolved, files


def run_ffmpeg_extract(video_path: Path, target_path: Path, preset: AudioPreset) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_name(f".{target_path.stem}.{uuid.uuid4().hex[:8]}.tmp{preset.ext}")

    def _invoke(extra_args: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        cmd = [
            "ffmpeg",
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(video_path),
            *extra_args,
            str(tmp_path),
        ]
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    try:
        proc = _invoke(preset.ffmpeg_args)
        if proc.returncode != 0 and preset.fallback_args:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            proc = _invoke(preset.fallback_args)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or f"ffmpeg exit code {proc.returncode}").strip()
            raise RuntimeError(err)
        if not tmp_path.exists() or tmp_path.stat().st_size == 0:
            raise RuntimeError("ffmpeg produced an empty audio file")
        os.replace(tmp_path, target_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def cmd_extract(args: argparse.Namespace) -> int:
    if not shutil.which("ffmpeg"):
        print("Error: ffmpeg not found in PATH.", file=sys.stderr)
        return 1

    preset = AUDIO_PRESETS[args.preset]
    input_path = Path(args.input)
    base_dir, video_files = scan_videos(input_path, recursive=not args.no_recursive)
    source_group = args.group or (input_path.resolve().stem if input_path.resolve().is_file() else input_path.resolve().name)
    audio_root = Path(args.audio_dir).resolve() / source_group

    db = StateDB(Path(args.db))
    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: Any) -> None:
        if not stop_event.is_set():
            print("\n[Interrupt] Stopping gracefully after in-flight files finish...", flush=True)
            stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    try:
        print(f"Scanning {input_path.resolve()} ...", flush=True)
        records: list[tuple[str, str, str, str, int, float]] = []
        for vf in video_files:
            st = vf.stat()
            rel = vf.relative_to(base_dir).as_posix()
            records.append((extract_video_id(vf.stem), vf.stem, str(vf), rel, st.st_size, st.st_mtime))

        db.upsert_videos(source_group, records)
        reset_cnt = db.reset_stale_running_audio(source_group)
        if reset_cnt:
            print(f"Reset {reset_cnt} stale 'running' item(s) to 'pending'.", flush=True)

        # Verify previously done items still exist on disk
        all_rows = db.get_all_group_items(source_group)
        missing_requeued = 0
        for row in all_rows:
            if row["audio_status"] == "done" and not args.overwrite:
                ap = Path(row["audio_path"]) if row["audio_path"] else None
                if not ap or not ap.exists() or ap.stat().st_size == 0:
                    db.mark_audio_pending(int(row["id"]))
                    missing_requeued += 1
        if missing_requeued:
            print(f"Re-queued {missing_requeued} item(s) whose audio files were missing on disk.", flush=True)

        todo_rows = db.get_items_for_extraction(
            source_group,
            overwrite=args.overwrite,
            retry_failed=not args.no_retry_failed,
            limit=args.limit,
        )

        total_in_group = len(all_rows)
        already_done = sum(1 for r in all_rows if r["audio_status"] == "done")
        todo_total = len(todo_rows)

        print(
            f"Group: {source_group} | Total videos: {total_in_group} | "
            f"Already done: {already_done} | To convert now: {todo_total}",
            flush=True,
        )
        print(
            f"Preset: {preset.name} ({preset.description}) | Workers: {args.workers} | "
            f"Output: {audio_root} | DB: {db.db_path}",
            flush=True,
        )

        if todo_total == 0:
            print("Nothing to extract. All matching videos are already converted.", flush=True)
            return 0

        run_id = db.create_run(
            "extract",
            source_group,
            {
                "input": str(input_path.resolve()),
                "audio_dir": str(audio_root),
                "preset": preset.name,
                "workers": args.workers,
                "overwrite": args.overwrite,
            },
            todo_total,
        )

        audio_root.mkdir(parents=True, exist_ok=True)
        counters = {"done": 0, "failed": 0, "skipped": 0, "audio_bytes": 0}
        counter_lock = threading.Lock()
        started_ts = time.monotonic()
        last_print_ts = 0.0

        def _worker(row: sqlite3.Row) -> None:
            if stop_event.is_set():
                return
            item_id = int(row["id"])
            video_path = Path(row["video_path"])
            rel_path = Path(row["video_rel_path"])
            target_path = (audio_root / rel_path).with_suffix(preset.ext)

            if not args.overwrite and row["audio_status"] == "done" and target_path.exists() and target_path.stat().st_size > 0:
                with counter_lock:
                    counters["skipped"] += 1
                return

            db.mark_audio_running(item_id, preset.name, target_path)
            try:
                run_ffmpeg_extract(video_path, target_path, preset)
                out_size = target_path.stat().st_size
                dur = probe_duration_sec(target_path)
                db.mark_audio_done(
                    item_id,
                    preset=preset.name,
                    audio_path=target_path,
                    audio_format=preset.ext.lstrip("."),
                    audio_size_bytes=out_size,
                    duration_sec=dur,
                )
                with counter_lock:
                    counters["done"] += 1
                    counters["audio_bytes"] += out_size
            except Exception as exc:
                db.mark_audio_failed(item_id, str(exc))
                with counter_lock:
                    counters["failed"] += 1
                    print(f"[FAILED] {row['video_rel_path']}: {exc}", file=sys.stderr, flush=True)

            nonlocal last_print_ts
            now_mono = time.monotonic()
            with counter_lock:
                processed = counters["done"] + counters["failed"] + counters["skipped"]
                should_log = (
                    processed == todo_total
                    or processed % args.progress_every == 0
                    or (now_mono - last_print_ts) >= 10.0
                )
                if should_log:
                    last_print_ts = now_mono
                    elapsed = max(now_mono - started_ts, 0.001)
                    rate = processed / elapsed
                    remaining = max(todo_total - processed, 0)
                    eta = remaining / rate if rate > 0 else 0
                    pct = (processed / todo_total) * 100.0
                    print(
                        f"[{processed}/{todo_total} | {pct:5.1f}%] "
                        f"done={counters['done']} failed={counters['failed']} "
                        f"out={format_bytes(counters['audio_bytes'])} "
                        f"rate={rate:.2f} file/s ETA={format_duration(eta)}",
                        flush=True,
                    )

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [pool.submit(_worker, row) for row in todo_rows]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()

        run_status = "interrupted" if stop_event.is_set() else "completed"
        db.finish_run(
            run_id,
            done_count=counters["done"],
            failed_count=counters["failed"],
            skipped_count=counters["skipped"],
            status=run_status,
        )

        elapsed = time.monotonic() - started_ts
        print(
            f"\nFinished ({run_status}) in {format_duration(elapsed)}: "
            f"done={counters['done']}, failed={counters['failed']}, skipped={counters['skipped']}, "
            f"audio_written={format_bytes(counters['audio_bytes'])}",
            flush=True,
        )
        return 0 if counters["failed"] == 0 and not stop_event.is_set() else 2
    finally:
        db.close()


# =============================================================================
# ASR endpoint resolution & transcription helpers (sync & async)
# =============================================================================

@dataclass(frozen=True)
class AsrEndpointConfig:
    name: str
    base_url: str
    api_key: str | None
    headers: dict[str, str]
    include_words: bool = False


def _parse_bool_env(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _normalize_endpoint_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name.strip().upper()).strip("_")


def _parse_headers_spec(raw: str | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if not raw or not raw.strip():
        return headers
    text = raw.strip()
    if text.startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                for k, v in data.items():
                    ks = str(k).strip()
                    vs = str(v).strip()
                    if ks and vs:
                        headers[ks] = vs
                return headers
        except Exception:
            pass
    for part in re.split(r"[;\r\n]+", text):
        item = part.strip()
        if not item or ":" not in item:
            continue
        k, v = item.split(":", 1)
        ks, vs = k.strip(), v.strip()
        if ks and vs:
            headers[ks] = vs
    return headers


def _parse_cli_headers(raw_headers: list[str] | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in raw_headers or []:
        if ":" not in item:
            raise ValueError(f"Invalid --header format (expected 'Key: Value'): {item}")
        k, v = item.split(":", 1)
        ks, vs = k.strip(), v.strip()
        if not ks:
            raise ValueError(f"Invalid --header name: {item}")
        headers[ks] = vs
    return headers


def _mask_secret(val: str | None) -> str:
    if not val or not val.strip():
        return "(not set)"
    s = val.strip()
    if len(s) <= 6:
        return "*" * len(s)
    return f"{s[:4]}...{s[-3:]}"


def _is_modal_url(url: str) -> bool:
    try:
        host = (urllib.parse.urlparse(url).hostname or "").lower()
        return host.endswith(".modal.run")
    except Exception:
        return False


def _load_named_endpoint_from_env(name: str) -> AsrEndpointConfig:
    norm = _normalize_endpoint_name(name)
    lower_name = name.strip().lower()
    if not norm or lower_name in ("default", "local"):
        if os.environ.get("ASR_LOCAL_BASE_URL"):
            prefix = "ASR_LOCAL_"
            base_url = os.environ.get(f"{prefix}BASE_URL", "").strip().rstrip("/")
            api_key = os.environ.get(f"{prefix}API_KEY", "").strip() or None
            headers = _parse_headers_spec(os.environ.get(f"{prefix}HEADERS"))
            modal_key = os.environ.get(f"{prefix}MODAL_KEY", "").strip()
            modal_secret = os.environ.get(f"{prefix}MODAL_SECRET", "").strip()
            include_words = _parse_bool_env(os.environ.get(f"{prefix}INCLUDE_WORDS"))
        else:
            base_url = os.environ.get("ASR_BASE_URL", "").strip().rstrip("/")
            api_key = os.environ.get("ASR_API_KEY", "").strip() or None
            headers = _parse_headers_spec(os.environ.get("ASR_HEADERS") or os.environ.get("ASR_EXTRA_HEADERS"))
            modal_key = ""
            modal_secret = ""
            include_words = _parse_bool_env(os.environ.get("ASR_INCLUDE_WORDS"))
        if modal_key:
            headers["Modal-Key"] = modal_key
        if modal_secret:
            headers["Modal-Secret"] = modal_secret
        return AsrEndpointConfig(
            name="local",
            base_url=base_url,
            api_key=api_key,
            headers=headers,
            include_words=include_words,
        )

    prefix = f"ASR_{norm}_"
    base_url = os.environ.get(f"{prefix}BASE_URL", "").strip().rstrip("/")
    api_key = os.environ.get(f"{prefix}API_KEY", "").strip() or None
    headers = _parse_headers_spec(os.environ.get(f"{prefix}HEADERS"))
    modal_key = os.environ.get(f"{prefix}MODAL_KEY", "").strip()
    modal_secret = os.environ.get(f"{prefix}MODAL_SECRET", "").strip()
    if norm == "MODAL":
        modal_key = modal_key or os.environ.get("ASR_MODAL_KEY", "").strip()
        modal_secret = modal_secret or os.environ.get("ASR_MODAL_SECRET", "").strip()
    if modal_key:
        headers["Modal-Key"] = modal_key
    if modal_secret:
        headers["Modal-Secret"] = modal_secret
    include_words = _parse_bool_env(os.environ.get(f"{prefix}INCLUDE_WORDS"))
    return AsrEndpointConfig(
        name=lower_name,
        base_url=base_url,
        api_key=api_key,
        headers=headers,
        include_words=include_words,
    )


def _discover_env_endpoints() -> list[AsrEndpointConfig]:
    found: dict[str, AsrEndpointConfig] = {}
    default_ep = _load_named_endpoint_from_env("local")
    if default_ep.base_url:
        found["local"] = default_ep

    pattern = re.compile(r"^ASR_([A-Z0-9_]+)_BASE_URL$")
    for key in sorted(os.environ.keys()):
        m = pattern.match(key)
        if not m:
            continue
        raw_name = m.group(1).lower()
        if raw_name in ("local", "default"):
            found["local"] = _load_named_endpoint_from_env("local")
            continue
        ep = _load_named_endpoint_from_env(raw_name)
        if ep.base_url:
            found[raw_name] = ep

    ordered: list[AsrEndpointConfig] = []
    if "local" in found:
        ordered.append(found.pop("local"))
    for k in sorted(found.keys()):
        ordered.append(found[k])
    return ordered


def resolve_asr_endpoint(
    *,
    endpoint: str | None = None,
    url: str | None = None,
    api_key: str | None = None,
    modal_key: str | None = None,
    modal_secret: str | None = None,
    cli_headers: list[str] | None = None,
    include_words: bool | None = None,
) -> AsrEndpointConfig:
    explicit_endpoint = (endpoint or "").strip()
    env_default_endpoint = os.environ.get("ASR_ENDPOINT", "").strip()
    chosen_name = explicit_endpoint or env_default_endpoint

    # If user passed --url without --endpoint, try matching a configured endpoint by URL
    clean_cli_url = (url or "").strip().rstrip("/")
    base_ep: AsrEndpointConfig | None = None
    if not explicit_endpoint and clean_cli_url:
        for candidate in _discover_env_endpoints():
            if candidate.base_url.rstrip("/") == clean_cli_url:
                base_ep = candidate
                break

    if base_ep is None:
        base_ep = _load_named_endpoint_from_env(chosen_name or "local")

    final_url = clean_cli_url or base_ep.base_url
    final_api_key = (api_key.strip() if api_key else None) or base_ep.api_key
    final_headers = dict(base_ep.headers)
    if modal_key and modal_key.strip():
        final_headers["Modal-Key"] = modal_key.strip()
    if modal_secret and modal_secret.strip():
        final_headers["Modal-Secret"] = modal_secret.strip()
    if cli_headers:
        final_headers.update(_parse_cli_headers(cli_headers))

    final_include_words = base_ep.include_words if include_words is None else bool(include_words)
    return AsrEndpointConfig(
        name=base_ep.name,
        base_url=final_url,
        api_key=final_api_key,
        headers=final_headers,
        include_words=final_include_words,
    )


def _validate_endpoint_auth(ep: AsrEndpointConfig) -> list[str]:
    """Return fatal errors (empty if OK); print non-fatal warnings to stderr."""
    errors: list[str] = []
    if not ep.base_url:
        if ep.name != "local":
            norm = _normalize_endpoint_name(ep.name)
            errors.append(f"Endpoint '{ep.name}' has no URL configured. Set ASR_{norm}_BASE_URL in .env or pass --url.")
        else:
            errors.append("Please provide --url, --endpoint, or set ASR_BASE_URL in .env.")
        return errors

    needs_modal = ep.name == "modal" or _is_modal_url(ep.base_url)
    if needs_modal:
        mk = ep.headers.get("Modal-Key", "").strip()
        ms = ep.headers.get("Modal-Secret", "").strip()
        if not mk or mk in ("wk-...", "<Modal-Key>"):
            errors.append("Missing Modal-Key header for Modal endpoint. Set ASR_MODAL_KEY in .env or pass --modal-key.")
        if not ms or ms in ("ws-...", "<Modal-Secret>"):
            errors.append("Missing Modal-Secret header for Modal endpoint. Set ASR_MODAL_SECRET in .env or pass --modal-secret.")
        if not ep.api_key:
            print(
                "[WARN] ASR_MODAL_API_KEY is empty; if your Modal ASR server enforces app-layer Bearer auth, fill ASR_MODAL_API_KEY in .env or pass --api-key.",
                file=sys.stderr,
                flush=True,
            )
    return errors


def _encode_multipart(
    fields: dict[str, str],
    file_field: str,
    file_path: Path,
) -> tuple[bytes, str]:
    boundary = f"----AsrPipeline{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
    parts: list[bytes] = []

    for k, v in fields.items():
        parts.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode("utf-8")
        )

    filename = file_path.name.replace('"', "_")
    parts.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'
            f"Content-Type: {mime_type}\r\n\r\n"
        ).encode("utf-8")
    )
    parts.append(file_path.read_bytes())
    parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)
    return body, f"multipart/form-data; boundary={boundary}"


def _http_request(
    method: str,
    url: str,
    *,
    api_key: str | None = None,
    extra_headers: dict[str, str] | None = None,
    body: bytes | None = None,
    content_type: str | None = None,
    timeout: float = 300.0,
) -> tuple[int, bytes, str]:
    headers: dict[str, str] = {}
    if extra_headers:
        headers.update(extra_headers)
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if content_type:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {e.code} {url}: {err_body}") from e


def _fmt_srt_time(seconds: float) -> str:
    ms_total = max(0, int(round(seconds * 1000)))
    h, rem = divmod(ms_total, 3_600_000)
    m, rem = divmod(rem, 60_000)
    s, ms = divmod(rem, 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def _extract_segments_and_text(payload: Any) -> tuple[str, list[dict[str, Any]]]:
    root = payload
    if isinstance(root, dict):
        for key in ("result", "data", "output"):
            if isinstance(root.get(key), dict):
                root = root[key]
                break

    if not isinstance(root, dict):
        return "", []

    segments: list[dict[str, Any]] = []
    raw_segs = root.get("segments") or root.get("utterances") or root.get("sentences")
    if isinstance(raw_segs, list):
        for seg in raw_segs:
            if not isinstance(seg, dict):
                continue
            start = seg.get("start", seg.get("start_time"))
            end = seg.get("end", seg.get("end_time"))
            text = str(seg.get("text", seg.get("content", ""))).strip()
            if isinstance(start, (int, float)) and isinstance(end, (int, float)) and text:
                segments.append({"start": float(start), "end": float(end), "text": text})

    if not segments and isinstance(root.get("words"), list):
        buf_text: list[str] = []
        buf_start: float | None = None
        buf_end: float = 0.0
        for w in root["words"]:
            if not isinstance(w, dict):
                continue
            ws = w.get("start", w.get("start_time"))
            we = w.get("end", w.get("end_time"))
            wt = str(w.get("word", w.get("text", "")))
            if not isinstance(ws, (int, float)) or not isinstance(we, (int, float)) or not wt:
                continue
            if buf_start is None:
                buf_start = float(ws)
            buf_text.append(wt)
            buf_end = float(we)
            joined = "".join(buf_text).strip()
            if len(joined) >= 24 or (joined and joined[-1] in "。！？.!?") or (buf_end - buf_start) >= 6.0:
                segments.append({"start": buf_start, "end": buf_end, "text": joined})
                buf_text = []
                buf_start = None
        if buf_start is not None and buf_text:
            segments.append({"start": buf_start, "end": buf_end, "text": "".join(buf_text).strip()})

    text = str(root.get("text") or "").strip()
    if not text and segments:
        text = "\n".join(s["text"] for s in segments)
    return text, segments


def _build_srt(segments: list[dict[str, Any]], fallback_text: str = "", duration_sec: float | None = None) -> str:
    if not segments:
        if not fallback_text.strip():
            return ""
        end = duration_sec if duration_sec and duration_sec > 0 else 10.0
        return f"1\n00:00:00,000 --> {_fmt_srt_time(end)}\n{fallback_text.strip()}\n"
    blocks = []
    for idx, seg in enumerate(segments, start=1):
        blocks.append(f"{idx}\n{_fmt_srt_time(seg['start'])} --> {_fmt_srt_time(seg['end'])}\n{seg['text']}")
    return "\n\n".join(blocks) + "\n"


def _parse_progress(payload: dict[str, Any]) -> tuple[str, float | None, str | None]:
    raw_status = str(payload.get("status") or payload.get("state") or "running").lower()
    completed = payload.get("completed_chunks")
    if completed is None:
        completed = payload.get("done_chunks")
    total = payload.get("total_chunks")
    if total is None:
        total = payload.get("num_chunks")

    prog = payload.get("progress")
    if isinstance(prog, (int, float)):
        progress = float(prog) * 100.0 if 0.0 <= float(prog) <= 1.0 and raw_status not in DONE_ASR_STATUSES else float(prog)
        if raw_status in DONE_ASR_STATUSES:
            progress = 100.0
        progress = max(0.0, min(100.0, progress))
    elif isinstance(completed, (int, float)) and isinstance(total, (int, float)) and total > 0:
        progress = max(0.0, min(100.0, (float(completed) / float(total)) * 100.0))
    elif raw_status in DONE_ASR_STATUSES:
        progress = 100.0
    else:
        progress = None

    msg = payload.get("message") or payload.get("stage") or payload.get("detail")
    if msg is None and isinstance(completed, (int, float)) and isinstance(total, (int, float)) and total > 0:
        msg = f"{int(completed)}/{int(total)} chunks ({raw_status})"
    return raw_status, progress, str(msg) if msg is not None else None


def _submit_async_task(
    base_url: str,
    audio_path: Path,
    *,
    language: str | None,
    api_key: str | None,
    extra_headers: dict[str, str] | None = None,
    include_words: bool = False,
    timeout: float,
) -> str:
    fields = {"return_timestamps": "true"}
    if include_words:
        fields["include_words"] = "true"
    if language:
        fields["language"] = language
    body, ctype = _encode_multipart(fields, "file", audio_path)
    _, create_bytes, _ = _http_request(
        "POST",
        f"{base_url}/v1/asr/tasks",
        api_key=api_key,
        extra_headers=extra_headers,
        body=body,
        content_type=ctype,
        timeout=timeout,
    )
    created = json.loads(create_bytes.decode("utf-8"))
    task_id = str(created.get("task_id") or created.get("id") or (created.get("data") or {}).get("task_id") or "")
    if not task_id:
        raise RuntimeError(f"Server did not return task_id: {created}")
    return task_id


def _delete_remote_task(
    base_url: str,
    task_id: str,
    *,
    api_key: str | None,
    extra_headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> None:
    try:
        _http_request(
            "DELETE",
            f"{base_url}/v1/asr/tasks/{urllib.parse.quote(task_id)}",
            api_key=api_key,
            extra_headers=extra_headers,
            timeout=timeout,
        )
    except Exception as exc:
        print(f"[WARN] Failed to DELETE remote task {task_id}: {exc}", file=sys.stderr, flush=True)


def _resolve_transcribe_mode(row: sqlite3.Row, audio_path: Path, args: argparse.Namespace) -> tuple[str, float | None]:
    dur = row["duration_sec"]
    if dur is None:
        dur = probe_duration_sec(audio_path)

    if args.mode in ("sync", "async"):
        mode = args.mode
    elif dur is not None:
        mode = "async" if dur > args.async_threshold else "sync"
    else:
        mode = "async" if audio_path.stat().st_size > 3 * 1024 * 1024 else "sync"
    return mode, dur


def cmd_transcribe(args: argparse.Namespace) -> int:
    try:
        ep = resolve_asr_endpoint(
            endpoint=args.endpoint,
            url=args.url,
            api_key=args.api_key,
            modal_key=args.modal_key,
            modal_secret=args.modal_secret,
            cli_headers=args.header,
            include_words=args.include_words,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    auth_errors = _validate_endpoint_auth(ep)
    if auth_errors:
        for err in auth_errors:
            print(f"Error: {err}", file=sys.stderr)
        return 1

    base_url = ep.base_url
    api_key = ep.api_key
    extra_headers = ep.headers
    include_words = ep.include_words
    transcript_root = Path(args.output_dir).resolve()
    concurrency = max(1, int(args.concurrency))
    prefetch = max(0, int(args.prefetch))
    delete_remote = not args.keep_remote_task
    db = StateDB(Path(args.db))
    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: Any) -> None:
        if not stop_event.is_set():
            print("\n[Interrupt] Stopping transcription after active requests settle...", flush=True)
            stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    try:
        item_ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()] if args.ids else None
        rows = db.get_items_for_transcription(
            source_group=args.group,
            overwrite=args.overwrite,
            retry_failed=not args.no_retry_failed,
            item_ids=item_ids,
            limit=args.limit,
        )
        total = len(rows)
        unfinished_audio = db.count_unfinished_audio(args.group) if args.watch else 0
        auth_tags: list[str] = []
        if api_key:
            auth_tags.append("Bearer")
        for hk in sorted(extra_headers.keys()):
            auth_tags.append(hk)
        print(
            f"ASR Endpoint: {ep.name} ({base_url}) | Auth: {', '.join(auth_tags) if auth_tags else 'none'} | "
            f"Group: {args.group or 'ALL'} | Ready items: {total} | Concurrency: {concurrency} | Prefetch: {prefetch} | "
            f"Watch: {'on' if args.watch else 'off'}"
            + (f" (audio pending={unfinished_audio})" if args.watch else "")
            + f" | Language: {args.language or 'auto'} | Words: {'on' if include_words else 'off'} | "
            f"Auto-delete: {'on' if delete_remote else 'off'} | Output: {transcript_root}",
            flush=True,
        )
        if total == 0 and (not args.watch or unfinished_audio == 0):
            print("No items ready for transcription.", flush=True)
            return 0

        run_id = db.create_run(
            "transcribe",
            args.group,
            {
                "endpoint": ep.name,
                "base_url": base_url,
                "mode": args.mode,
                "concurrency": concurrency,
                "prefetch": prefetch,
                "watch": bool(args.watch),
                "language": args.language,
                "include_words": include_words,
                "output_dir": str(transcript_root),
                "save_json": args.save_json,
                "delete_remote_task": delete_remote,
            },
            total,
        )

        counters = {"done": 0, "failed": 0, "skipped": 0, "total": total}
        lock = threading.Lock()
        started_ts = time.monotonic()

        def _process_one(
            row: sqlite3.Row,
            *,
            pre_mode: str | None = None,
            pre_dur: float | None = None,
            pre_task_id: str | None = None,
        ) -> None:
            if stop_event.is_set():
                return
            item_id = int(row["id"])
            audio_path = Path(row["audio_path"])
            if not audio_path.exists():
                db.mark_asr_failed(item_id, f"Audio file not found: {audio_path}")
                with lock:
                    counters["failed"] += 1
                return

            if pre_mode is not None:
                mode, dur = pre_mode, pre_dur
            else:
                mode, dur = _resolve_transcribe_mode(row, audio_path, args)

            out_base = (transcript_root / row["source_group"] / row["video_rel_path"]).with_suffix("")
            out_base.parent.mkdir(parents=True, exist_ok=True)
            txt_path = out_base.with_suffix(".txt")
            srt_path = out_base.with_suffix(".srt")
            json_path = out_base.with_suffix(".json") if args.save_json else None
            task_id: str | None = None

            try:
                if mode == "sync":
                    db.mark_asr_started(item_id, mode="sync", server_url=base_url, status="running", progress=10.0, message="uploading")
                    fields = {"response_format": "json", "return_timestamps": "true"}
                    if include_words:
                        fields["include_words"] = "true"
                    if args.language:
                        fields["language"] = args.language
                    body, ctype = _encode_multipart(fields, "file", audio_path)
                    _, raw_bytes, _ = _http_request(
                        "POST",
                        f"{base_url}/v1/asr/transcribe",
                        api_key=api_key,
                        extra_headers=extra_headers,
                        body=body,
                        content_type=ctype,
                        timeout=args.timeout,
                    )
                    payload = json.loads(raw_bytes.decode("utf-8"))
                    text, segments = _extract_segments_and_text(payload)
                    if segments:
                        srt_text = _build_srt(segments, text, dur)
                    else:
                        srt_fields = {"response_format": "srt", "return_timestamps": "true"}
                        if args.language:
                            srt_fields["language"] = args.language
                        sbody, sctype = _encode_multipart(srt_fields, "file", audio_path)
                        _, srt_bytes, _ = _http_request(
                            "POST",
                            f"{base_url}/v1/asr/transcribe",
                            api_key=api_key,
                            extra_headers=extra_headers,
                            body=sbody,
                            content_type=sctype,
                            timeout=args.timeout,
                        )
                        srt_text = srt_bytes.decode("utf-8")
                else:
                    if pre_task_id:
                        task_id = pre_task_id
                    else:
                        same_server = (row["asr_server_url"] or "").rstrip("/") == base_url
                        existing_task_id = (
                            row["asr_task_id"]
                            if (not args.overwrite and same_server and row["asr_status"] in ("queued", "running"))
                            else None
                        )
                        task_id = existing_task_id
                        if not task_id:
                            db.mark_asr_started(item_id, mode="async", server_url=base_url, status="queued", progress=0.0, message="submitting task")
                            task_id = _submit_async_task(
                                base_url,
                                audio_path,
                                language=args.language,
                                api_key=api_key,
                                extra_headers=extra_headers,
                                include_words=include_words,
                                timeout=args.timeout,
                            )
                            db.update_asr_progress(item_id, status="queued", progress=1.0, message="task created", task_id=task_id)
                        else:
                            db.update_asr_progress(item_id, status="running", message="resuming task poll", task_id=task_id)

                    deadline = time.monotonic() + args.task_timeout
                    payload = {}
                    while True:
                        if stop_event.is_set():
                            return
                        if time.monotonic() > deadline:
                            raise RuntimeError(f"Async task {task_id} timed out after {args.task_timeout}s")
                        _, poll_bytes, _ = _http_request(
                            "GET",
                            f"{base_url}/v1/asr/tasks/{urllib.parse.quote(task_id)}",
                            api_key=api_key,
                            extra_headers=extra_headers,
                            timeout=60.0,
                        )
                        payload = json.loads(poll_bytes.decode("utf-8"))
                        raw_status, progress, msg = _parse_progress(payload)
                        if raw_status in DONE_ASR_STATUSES:
                            db.update_asr_progress(item_id, status="running", progress=99.0, message="downloading subtitle", task_id=task_id)
                            break
                        if raw_status in FAIL_ASR_STATUSES:
                            err_msg = payload.get("error") or payload.get("message") or raw_status
                            raise RuntimeError(f"Remote task {task_id} failed: {err_msg}")
                        mapped_status = "queued" if raw_status in ("queued", "pending") else "running"
                        db.update_asr_progress(item_id, status=mapped_status, progress=progress, message=msg or raw_status, task_id=task_id)
                        time.sleep(args.poll_interval)

                    text, segments = _extract_segments_and_text(payload)
                    try:
                        _, srt_bytes, _ = _http_request(
                            "GET",
                            f"{base_url}/v1/asr/tasks/{urllib.parse.quote(task_id)}/subtitle?format=srt",
                            api_key=api_key,
                            extra_headers=extra_headers,
                            timeout=60.0,
                        )
                        srt_text = srt_bytes.decode("utf-8")
                    except Exception:
                        srt_text = _build_srt(segments, text, dur)

                txt_path.write_text(text + ("\n" if text and not text.endswith("\n") else ""), encoding="utf-8")
                srt_path.write_text(srt_text, encoding="utf-8")
                if json_path is not None:
                    json_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

                db.mark_asr_done(item_id, txt_path=txt_path, srt_path=srt_path, json_path=json_path)
                if mode == "async" and delete_remote and task_id:
                    _delete_remote_task(base_url, task_id, api_key=api_key, extra_headers=extra_headers)
                with lock:
                    counters["done"] += 1
                    done_now = counters["done"] + counters["failed"]
                    total_now = counters["total"]
                    print(f"[{done_now}/{total_now}] DONE ({mode}) {row['video_rel_path']}", flush=True)
            except Exception as exc:
                db.mark_asr_failed(item_id, str(exc))
                with lock:
                    counters["failed"] += 1
                    print(f"[FAILED] {row['video_rel_path']}: {exc}", file=sys.stderr, flush=True)

        if prefetch > 0 or args.watch:
            in_flight_sem = threading.Semaphore(concurrency + prefetch)
            ready_q: queue.Queue[tuple[sqlite3.Row, str | None, float | None, str | None] | None] = queue.Queue()

            def _producer() -> None:
                seen_ids: set[int] = {int(r["id"]) for r in rows}
                pending_batch: list[sqlite3.Row] = list(rows)
                scheduled = 0
                waiting_logged = False
                try:
                    while not stop_event.is_set():
                        if not pending_batch:
                            if not args.watch:
                                break
                            if args.limit and scheduled >= args.limit:
                                break
                            fetched = db.get_items_for_transcription(
                                source_group=args.group,
                                overwrite=args.overwrite,
                                retry_failed=not args.no_retry_failed,
                                item_ids=item_ids,
                                limit=None,
                            )
                            new_rows = [r for r in fetched if int(r["id"]) not in seen_ids]
                            if args.limit:
                                remaining = max(0, args.limit - scheduled)
                                new_rows = new_rows[:remaining]
                            if new_rows:
                                waiting_logged = False
                                for r in new_rows:
                                    seen_ids.add(int(r["id"]))
                                with lock:
                                    counters["total"] += len(new_rows)
                                    total_now = counters["total"]
                                print(f"[Watch] Discovered {len(new_rows)} new extracted audio item(s) (queued total={total_now}).", flush=True)
                                pending_batch = new_rows
                            else:
                                rem_audio = db.count_unfinished_audio(args.group)
                                if rem_audio == 0:
                                    # Final re-check in case last audio completed right before count
                                    fetched2 = db.get_items_for_transcription(
                                        source_group=args.group,
                                        overwrite=args.overwrite,
                                        retry_failed=not args.no_retry_failed,
                                        item_ids=item_ids,
                                        limit=None,
                                    )
                                    new_rows2 = [r for r in fetched2 if int(r["id"]) not in seen_ids]
                                    if args.limit:
                                        new_rows2 = new_rows2[: max(0, args.limit - scheduled)]
                                    if new_rows2:
                                        for r in new_rows2:
                                            seen_ids.add(int(r["id"]))
                                        with lock:
                                            counters["total"] += len(new_rows2)
                                        pending_batch = new_rows2
                                        continue
                                    print("[Watch] All audio extraction completed and all ready items have been queued.", flush=True)
                                    break
                                if not waiting_logged:
                                    print(f"[Watch] Waiting for audio extraction ({rem_audio} audio item(s) still pending/running)...", flush=True)
                                    waiting_logged = True
                                sleep_end = time.monotonic() + max(1.0, float(args.watch_interval))
                                while not stop_event.is_set() and time.monotonic() < sleep_end:
                                    time.sleep(0.5)
                                continue

                        row = pending_batch.pop(0)
                        acquired = False
                        while not stop_event.is_set():
                            if in_flight_sem.acquire(timeout=0.5):
                                acquired = True
                                break
                        if not acquired:
                            break

                        scheduled += 1
                        enqueued = False
                        try:
                            if prefetch > 0:
                                item_id = int(row["id"])
                                audio_path = Path(row["audio_path"])
                                if not audio_path.exists():
                                    db.mark_asr_failed(item_id, f"Audio file not found: {audio_path}")
                                    with lock:
                                        counters["failed"] += 1
                                        print(f"[FAILED] {row['video_rel_path']}: Audio file not found: {audio_path}", file=sys.stderr, flush=True)
                                    continue

                                mode, dur = _resolve_transcribe_mode(row, audio_path, args)
                                if mode == "async":
                                    same_server = (row["asr_server_url"] or "").rstrip("/") == base_url
                                    existing_task_id = (
                                        row["asr_task_id"]
                                        if (not args.overwrite and same_server and row["asr_status"] in ("queued", "running"))
                                        else None
                                    )
                                    if existing_task_id:
                                        task_id = existing_task_id
                                        db.update_asr_progress(item_id, status="running", message="resuming task poll", task_id=task_id)
                                    else:
                                        db.mark_asr_started(item_id, mode="async", server_url=base_url, status="queued", progress=0.0, message="uploading (prefetch)")
                                        task_id = _submit_async_task(
                                            base_url,
                                            audio_path,
                                            language=args.language,
                                            api_key=api_key,
                                            extra_headers=extra_headers,
                                            include_words=include_words,
                                            timeout=args.timeout,
                                        )
                                        db.update_asr_progress(item_id, status="queued", progress=1.0, message="task created", task_id=task_id)
                                    ready_q.put((row, mode, dur, task_id))
                                    enqueued = True
                                else:
                                    ready_q.put((row, mode, dur, None))
                                    enqueued = True
                            else:
                                ready_q.put((row, None, None, None))
                                enqueued = True
                        except Exception as exc:
                            db.mark_asr_failed(int(row["id"]), str(exc))
                            with lock:
                                counters["failed"] += 1
                                print(f"[FAILED] {row['video_rel_path']}: {exc}", file=sys.stderr, flush=True)
                        finally:
                            if not enqueued:
                                in_flight_sem.release()
                finally:
                    for _ in range(concurrency):
                        ready_q.put(None)

            def _worker() -> None:
                while not stop_event.is_set():
                    try:
                        item = ready_q.get(timeout=0.5)
                    except queue.Empty:
                        continue
                    if item is None:
                        break
                    row, mode, dur, task_id = item
                    try:
                        _process_one(row, pre_mode=mode, pre_dur=dur, pre_task_id=task_id)
                    finally:
                        in_flight_sem.release()

            producer = threading.Thread(target=_producer, name="asr-producer", daemon=True)
            workers = [threading.Thread(target=_worker, name=f"asr-worker-{i + 1}") for i in range(concurrency)]
            producer.start()
            for w in workers:
                w.start()
            producer.join()
            for w in workers:
                w.join()
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = [pool.submit(_process_one, row) for row in rows]
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()

        run_status = "interrupted" if stop_event.is_set() else "completed"
        db.finish_run(
            run_id,
            done_count=counters["done"],
            failed_count=counters["failed"],
            skipped_count=counters["skipped"],
            status=run_status,
        )
        print(
            f"\nTranscription finished ({run_status}) in {format_duration(time.monotonic() - started_ts)}: "
            f"done={counters['done']}, failed={counters['failed']}",
            flush=True,
        )
        return 0 if counters["failed"] == 0 and not stop_event.is_set() else 2
    finally:
        db.close()


def cmd_status(args: argparse.Namespace) -> int:
    db = StateDB(Path(args.db))
    try:
        summary = db.get_summary(args.group)
        failures = db.get_recent_failures(args.group, limit=args.errors)
        if args.json:
            print(json.dumps({"db": str(db.db_path), "groups": summary, "recent_failures": failures}, ensure_ascii=False, indent=2))
            return 0

        print(f"SQLite DB: {db.db_path}")
        if not summary:
            print("No records found.")
            return 0

        for g in summary:
            total = g["total"]
            a_pct = (g["audio_done"] / total * 100.0) if total else 0.0
            t_pct = (g["asr_done"] / total * 100.0) if total else 0.0
            print(f"\nGroup: {g['source_group']} (total={total})")
            print(
                f"  Audio : done={g['audio_done']}/{total} ({a_pct:5.1f}%) | "
                f"pending={g['audio_pending']} running={g['audio_running']} failed={g['audio_failed']}"
            )
            print(
                f"  ASR   : done={g['asr_done']}/{total} ({t_pct:5.1f}%) | "
                f"in_progress={g['asr_in_progress']} pending={g['asr_pending']} failed={g['asr_failed']} "
                f"(avg_progress={g['avg_asr_progress']:.1f}%)"
            )
            print(
                f"  Volume: video={format_bytes(g['video_bytes'])} -> audio={format_bytes(g['audio_bytes'])} | "
                f"total_duration={format_duration(g['duration_sec'])}"
            )

        if failures:
            print(f"\nRecent failures (up to {args.errors}):")
            for f in failures:
                stage = "audio" if f["audio_status"] == "failed" else "asr"
                err = f["audio_error"] if stage == "audio" else f["asr_error"]
                print(f"  - [{f['source_group']}] #{f['id']} {f['video_id']} ({stage}): {err}")
        return 0
    finally:
        db.close()


def cmd_list(args: argparse.Namespace) -> int:
    db = StateDB(Path(args.db))
    try:
        rows = db.list_items(
            source_group=args.group,
            audio_status=args.audio_status,
            asr_status=args.asr_status,
            limit=args.limit,
        )
        if args.json:
            print(json.dumps(rows, ensure_ascii=False, indent=2))
            return 0
        for r in rows:
            dur = format_duration(r["duration_sec"])
            asize = format_bytes(r["audio_size_bytes"])
            print(
                f"#{r['id']:4d} [{r['source_group']}] audio={r['audio_status']:7s} ({asize:>9s}, {dur:>8s}) | "
                f"asr={r['asr_status']:7s} ({r['asr_progress']:5.1f}%) | {r['title']}"
            )
        return 0
    finally:
        db.close()


def cmd_reset(args: argparse.Namespace) -> int:
    db = StateDB(Path(args.db))
    try:
        statuses = [s.strip() for s in args.status.split(",") if s.strip()]
        count = db.reset_items(args.stage, statuses, args.group)
        print(f"Reset {count} item(s) in stage={args.stage} (status in {statuses}, group={args.group or 'ALL'}).")
        return 0
    finally:
        db.close()


def cmd_endpoints(args: argparse.Namespace) -> int:
    endpoints = _discover_env_endpoints()
    default_name = (os.environ.get("ASR_ENDPOINT") or "local").strip().lower() or "local"

    if args.endpoint:
        try:
            ep = resolve_asr_endpoint(
                endpoint=args.endpoint,
                url=args.url,
                api_key=args.api_key,
                modal_key=args.modal_key,
                modal_secret=args.modal_secret,
                cli_headers=args.header,
            )
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        targets = [ep]
    else:
        targets = endpoints

    if not targets:
        print("No ASR endpoints configured in .env or environment.")
        return 0

    exit_code = 0
    for ep in targets:
        marker = " (default)" if ep.name == default_name else ""
        print(f"[{ep.name}]{marker}")
        print(f"  URL          : {ep.base_url or '(not set)'}")
        print(f"  Bearer Key   : {_mask_secret(ep.api_key)}")
        if ep.name == "modal" or _is_modal_url(ep.base_url) or "Modal-Key" in ep.headers or "Modal-Secret" in ep.headers:
            print(f"  Modal-Key    : {_mask_secret(ep.headers.get('Modal-Key'))}")
            print(f"  Modal-Secret : {_mask_secret(ep.headers.get('Modal-Secret'))}")
        other_headers = {k: v for k, v in ep.headers.items() if k not in ("Modal-Key", "Modal-Secret")}
        if other_headers:
            masked_others = ", ".join(f"{k}={_mask_secret(v)}" for k, v in sorted(other_headers.items()))
            print(f"  Extra Headers: {masked_others}")
        print(f"  Include Words: {'true' if ep.include_words else 'false'}")

        if args.check:
            errs = _validate_endpoint_auth(ep)
            if errs:
                for err in errs:
                    print(f"  Health Check : SKIPPED ({err})")
                exit_code = 1
                continue
            try:
                status, body, _ = _http_request(
                    "GET",
                    f"{ep.base_url}/health",
                    api_key=ep.api_key,
                    extra_headers=ep.headers,
                    timeout=args.timeout,
                )
                preview = body.decode("utf-8", errors="replace").strip().replace("\n", " ")[:200]
                print(f"  Health Check : OK (HTTP {status}) {preview}")
            except Exception as exc:
                print(f"  Health Check : FAILED ({exc})")
                exit_code = 1
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="MP4 -> Audio extraction & ASR transcription state pipeline (SQLite + filesystem).",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help=f"SQLite database path (default: {DEFAULT_DB_PATH})")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ext = sub.add_parser("extract", help="Scan videos and extract audio files to filesystem")
    p_ext.add_argument("input", help="Video file or directory to scan")
    p_ext.add_argument("--group", help="Source group name (default: input folder name)")
    p_ext.add_argument("--audio-dir", default=str(DEFAULT_AUDIO_DIR), help=f"Audio output root (default: {DEFAULT_AUDIO_DIR})")
    p_ext.add_argument("--preset", choices=list(AUDIO_PRESETS.keys()), default="asr-mp3", help="Audio encoding preset (default: asr-mp3)")
    p_ext.add_argument("--workers", type=int, default=8, help="Parallel ffmpeg workers (default: 8)")
    p_ext.add_argument("--overwrite", action="store_true", help="Re-extract even if already marked done")
    p_ext.add_argument("--no-retry-failed", action="store_true", help="Do not retry previously failed items")
    p_ext.add_argument("--no-recursive", action="store_true", help="Do not scan subdirectories")
    p_ext.add_argument("--limit", type=int, help="Optional max items to process this run")
    p_ext.add_argument("--progress-every", type=int, default=20, help="Print progress every N files (default: 20)")
    p_ext.set_defaults(func=cmd_extract)

    p_tr = sub.add_parser("transcribe", help="Transcribe extracted audio via Qwen3-ASR server and track progress in SQLite")
    p_tr.add_argument("--group", help="Filter by source_group")
    p_tr.add_argument("--ids", help="Optional comma-separated item IDs to transcribe")
    p_tr.add_argument("--endpoint", help="Named ASR endpoint from .env (e.g. local, modal; or env ASR_ENDPOINT)")
    p_tr.add_argument("--url", help="ASR server base URL override (or env ASR_BASE_URL / ASR_<ENDPOINT>_BASE_URL)")
    p_tr.add_argument("--api-key", help="Optional Bearer token override (or env ASR_API_KEY / ASR_<ENDPOINT>_API_KEY)")
    p_tr.add_argument("--modal-key", help="Optional Modal-Key header override (or env ASR_MODAL_KEY)")
    p_tr.add_argument("--modal-secret", help="Optional Modal-Secret header override (or env ASR_MODAL_SECRET)")
    p_tr.add_argument("--header", action="append", help="Extra HTTP header in 'Key: Value' format (repeatable)")
    p_tr.add_argument(
        "--include-words",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Request word-level timestamps (include_words=true); defaults to endpoint config",
    )
    p_tr.add_argument("--output-dir", default=str(DEFAULT_TRANSCRIPT_DIR), help=f"Transcript output root (default: {DEFAULT_TRANSCRIPT_DIR})")
    p_tr.add_argument("--mode", choices=["auto", "sync", "async"], default="auto", help="ASR endpoint mode (default: auto)")
    p_tr.add_argument("--async-threshold", type=float, default=150.0, help="Duration seconds above which auto mode uses async tasks (default: 150)")
    p_tr.add_argument("--concurrency", type=int, default=8, help="Concurrent ASR workers (default: 8)")
    p_tr.add_argument("--prefetch", type=int, default=1, help="Async items to pre-upload ahead of active workers (default: 1, 0 to disable)")
    p_tr.add_argument("--watch", action="store_true", help="Continuously watch SQLite for newly extracted audio until audio extraction completes")
    p_tr.add_argument("--watch-interval", type=float, default=5.0, help="Seconds between DB checks while waiting for audio extraction (default: 5)")
    p_tr.add_argument("--language", help="Optional language hint")
    p_tr.add_argument("--save-json", action="store_true", help="Also save raw compact JSON response")
    p_tr.add_argument("--keep-remote-task", action="store_true", help="Keep completed async tasks on server instead of auto-calling DELETE")
    p_tr.add_argument("--overwrite", action="store_true", help="Re-transcribe items already marked done")
    p_tr.add_argument("--no-retry-failed", action="store_true", help="Do not retry previously failed ASR items")
    p_tr.add_argument("--limit", type=int, help="Optional max items to transcribe this run")
    p_tr.add_argument("--timeout", type=float, default=300.0, help="HTTP request timeout in seconds (default: 300)")
    p_tr.add_argument("--poll-interval", type=float, default=3.0, help="Async task poll interval in seconds (default: 3)")
    p_tr.add_argument("--task-timeout", type=float, default=7200.0, help="Async task max wait in seconds (default: 7200)")
    p_tr.set_defaults(func=cmd_transcribe)

    p_ep = sub.add_parser("endpoints", help="List configured ASR endpoints and optionally run /health checks")
    p_ep.add_argument("--endpoint", help="Show/check a specific endpoint (e.g. local, modal)")
    p_ep.add_argument("--url", help="Optional URL override")
    p_ep.add_argument("--api-key", help="Optional Bearer token override")
    p_ep.add_argument("--modal-key", help="Optional Modal-Key header override")
    p_ep.add_argument("--modal-secret", help="Optional Modal-Secret header override")
    p_ep.add_argument("--header", action="append", help="Extra HTTP header in 'Key: Value' format (repeatable)")
    p_ep.add_argument("--check", action="store_true", help="Send GET /health with configured auth headers")
    p_ep.add_argument("--timeout", type=float, default=15.0, help="Health check timeout in seconds (default: 15)")
    p_ep.set_defaults(func=cmd_endpoints)

    p_st = sub.add_parser("status", help="Show SQLite extraction and transcription progress summary")
    p_st.add_argument("--group", help="Filter by source_group")
    p_st.add_argument("--errors", type=int, default=10, help="Max recent failures to display (default: 10)")
    p_st.add_argument("--json", action="store_true", help="Output JSON")
    p_st.set_defaults(func=cmd_status)

    p_ls = sub.add_parser("list", help="List media items from SQLite")
    p_ls.add_argument("--group", help="Filter by source_group")
    p_ls.add_argument("--audio-status", help="Filter by audio_status")
    p_ls.add_argument("--asr-status", help="Filter by asr_status")
    p_ls.add_argument("--limit", type=int, default=50, help="Max rows (default: 50)")
    p_ls.add_argument("--json", action="store_true", help="Output JSON")
    p_ls.set_defaults(func=cmd_list)

    p_rs = sub.add_parser("reset", help="Reset items back to pending")
    p_rs.add_argument("--stage", choices=["audio", "asr"], required=True, help="Which stage to reset")
    p_rs.add_argument("--status", default="failed,running", help="Comma-separated statuses to reset (default: failed,running)")
    p_rs.add_argument("--group", help="Optional source_group filter")
    p_rs.set_defaults(func=cmd_reset)

    return parser


def main() -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
