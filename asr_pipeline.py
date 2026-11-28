#!/usr/bin/env python3
"""Simple MP4 -> Audio extraction & ASR transcription state pipeline backed by SQLite.

Zero third-party dependencies (Python 3.10+ stdlib + ffmpeg/ffprobe).
Stores audio & transcript files on the filesystem and tracks both audio extraction
and ASR transcription progress in SQLite.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import math
import json
import mimetypes
import os
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
from email.utils import parsedate_to_datetime
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


def stable_fingerprint(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_fingerprint(path: Path) -> str:
    st = path.stat()
    return stable_fingerprint({"size": st.st_size, "mtime_ns": st.st_mtime_ns})


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_base_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url.strip())
    return urllib.parse.urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), parts.query, ""))


def build_output_paths(root: Path, group: str, relative_path: str, save_json: bool = False) -> dict[str, Path]:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts or Path(group).is_absolute() or ".." in Path(group).parts:
        raise ValueError("Output paths must stay inside the configured root")
    stem = relative.stem
    folder = root / group / relative.parent
    result = {"txt": folder / (stem + ".txt"), "srt": folder / (stem + ".srt"),
              "marker": folder / ("." + stem + ".complete.json")}
    if save_json:
        result["json"] = folder / (stem + ".json")
    return result


class TranscriptStore:
    """Commit files first and a verifiable completion record last."""

    def __init__(self, paths: dict[str, Path]) -> None:
        self.paths = paths

    def read_marker(self) -> dict[str, Any] | None:
        try:
            marker = json.loads(self.paths["marker"].read_text(encoding="utf-8"))
            if marker.get("schema_version") != 1 or marker.get("producer") != "python":
                return None
            return marker
        except (OSError, ValueError, AttributeError):
            return None

    def valid(self, source: str | None, request: str | None) -> bool:
        marker = self.read_marker()
        if not marker or not source or not request:
            return False
        if marker.get("source_fingerprint") != source or marker.get("request_fingerprint") != request:
            return False
        files = marker.get("files")
        if not isinstance(files, dict) or not {"txt", "srt"}.issubset(files):
            return False
        try:
            for kind, dest in self.paths.items():
                if kind == "marker":
                    continue
                info = files.get(kind)
                if not isinstance(info, dict) or info.get("name") != dest.name:
                    return False
                if not dest.is_file() or dest.stat().st_size != info.get("size") or sha256_file(dest) != info.get("sha256"):
                    return False
            # Validate optional files recorded by a previous save-json run too.
            for kind, info in files.items():
                if kind not in ("txt", "srt", "json") or not isinstance(info, dict):
                    return False
                name = info.get("name")
                if not isinstance(name, str) or Path(name).name != name:
                    return False
                dest = self.paths["marker"].parent / name
                if not dest.is_file() or dest.stat().st_size != info.get("size") or sha256_file(dest) != info.get("sha256"):
                    return False
            return True
        except (OSError, TypeError, ValueError):
            return False

    def commit(self, text: str, subtitle: str, payload: Any, source: str, request: str, input_path: str) -> None:
        self.paths["marker"].parent.mkdir(parents=True, exist_ok=True)
        contents = {"txt": text + ("\n" if text and not text.endswith("\n") else ""), "srt": subtitle}
        if "json" in self.paths:
            contents["json"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        staged: list[tuple[Path, Path]] = []
        files: dict[str, Any] = {}
        try:
            for kind, content in contents.items():
                dest = self.paths[kind]
                temp = dest.with_name("." + dest.name + "." + uuid.uuid4().hex + ".tmp")
                staged.append((temp, dest))
                temp.write_text(content, encoding="utf-8")
                files[kind] = {"name": dest.name, "size": temp.stat().st_size, "sha256": sha256_file(temp)}
            # Remove the old commit point before replacing even the first file.
            self.paths["marker"].unlink(missing_ok=True)
            for temp, dest in staged:
                os.replace(temp, dest)
            marker = {"schema_version": 1, "producer": "python", "source_fingerprint": source,
                      "request_fingerprint": request, "input_path": input_path, "files": files}
            dest = self.paths["marker"]
            temp = dest.with_name(dest.name + "." + uuid.uuid4().hex + ".tmp")
            staged.append((temp, dest))
            temp.write_text(json.dumps(marker, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            os.replace(temp, dest)
        finally:
            for temp, _ in staged:
                temp.unlink(missing_ok=True)


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

    source_fingerprint TEXT,
    audio_fingerprint TEXT,
    asr_fingerprint   TEXT,
    asr_phase         TEXT,

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
        self.conn = sqlite3.connect(str(self.db_path), timeout=30.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        try:
            with self._lock:
                tables = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.public_snapshot = False
                if "snapshot_metadata" in tables:
                    entry = self.conn.execute("SELECT value FROM snapshot_metadata WHERE key='profile'").fetchone()
                    self.public_snapshot = bool(entry and entry[0] == "public")
                if not self.public_snapshot:
                    version = self.conn.execute("PRAGMA user_version").fetchone()[0]
                    if version > 1:
                        raise RuntimeError(f"Database schema {version} is newer than supported schema 1")
                    if "media_items" in tables:
                        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(media_items)")}
                        missing = {"source_fingerprint", "audio_fingerprint", "asr_fingerprint", "asr_phase"} - cols
                        if version < 1 or missing:
                            backup_dir = self.db_path.parent / ".migration-backups"
                            backup_dir.mkdir(exist_ok=True)
                            backup = backup_dir / (self.db_path.stem + ".pre-v1-" + uuid.uuid4().hex + ".db")
                            dest = sqlite3.connect(str(backup))
                            try:
                                self.conn.backup(dest)
                            finally:
                                dest.close()
                            with self.conn:
                                self.conn.execute("BEGIN IMMEDIATE")
                                # Another CLI may have migrated after our online backup.
                                locked_version = self.conn.execute("PRAGMA user_version").fetchone()[0]
                                if locked_version > 1:
                                    raise RuntimeError(f"Database schema {locked_version} is newer than supported schema 1")
                                locked_cols = {r[1] for r in self.conn.execute("PRAGMA table_info(media_items)")}
                                for name in sorted({"source_fingerprint", "audio_fingerprint", "asr_fingerprint", "asr_phase"} - locked_cols):
                                    self.conn.execute(f"ALTER TABLE media_items ADD COLUMN {name} TEXT")
                                self.conn.execute("PRAGMA user_version=1")
                    self.conn.executescript(SCHEMA_SQL)
                    self.conn.execute("PRAGMA user_version=1")
                    self.conn.commit()
        except BaseException:
            self.conn.close()
            raise

    def require_private(self) -> None:
        if self.public_snapshot:
            raise ValueError("Public snapshot is an archive index. Build local state with extract or restore a private backup to a different DB.")


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
                SET done_count = ?, failed_count = ?, skipped_count = ?, status = ?, finished_at = ?,
                    total_count = MAX(total_count, ?)
                WHERE id = ?
                """,
                (done_count, failed_count, skipped_count, status, utc_now(), done_count + failed_count + skipped_count, run_id),
            )
            self.conn.commit()

    def upsert_videos(self, source_group: str, records: list[tuple[str, str, str, str, int, float]]) -> int:
        """Record current inputs; invalidate downstream state only on observed changes."""
        self.require_private()
        now = utc_now()
        with self._lock, self.conn:
            for vid, title, vpath, vrel, size, mtime in records:
                try:
                    fingerprint = file_fingerprint(Path(vpath))
                except OSError:
                    fingerprint = stable_fingerprint({"size": size, "mtime_ns": int(mtime * 1_000_000_000)})
                old = self.conn.execute("SELECT * FROM media_items WHERE video_path=?", (vpath,)).fetchone()
                if old is None:
                    self.conn.execute("""INSERT INTO media_items
                        (source_group,video_id,title,video_path,video_rel_path,video_size_bytes,video_mtime,
                         source_fingerprint,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (source_group,vid,title,vpath,vrel,size,mtime,fingerprint,now,now))
                    continue
                changed = (old["source_fingerprint"] is not None and old["source_fingerprint"] != fingerprint) or old["video_size_bytes"] != size or old["video_mtime"] != mtime
                # Do not silently bless old completed artifacts whose provenance is unknown.
                source = fingerprint if changed or old["source_fingerprint"] or old["audio_status"] != "done" else None
                self.conn.execute("""UPDATE media_items SET source_group=?,video_id=?,title=?,video_rel_path=?,
                    video_size_bytes=?,video_mtime=?,source_fingerprint=?,updated_at=? WHERE id=?""",
                    (source_group,vid,title,vrel,size,mtime,source,now,old["id"]))
                if changed:
                    self.conn.execute("""UPDATE media_items SET audio_status='pending',audio_fingerprint=NULL,
                        asr_status='pending',asr_fingerprint=NULL,asr_phase=NULL,asr_task_id=NULL,
                        asr_progress=0,asr_message='input changed',asr_error=NULL,
                        txt_path=NULL,srt_path=NULL,json_path=NULL WHERE id=?""", (old["id"],))
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
            row = self.conn.execute("SELECT source_fingerprint FROM media_items WHERE id=?", (item_id,)).fetchone()
            fingerprint = stable_fingerprint({"source": row[0], "preset": preset, "rule": 1,
                                              "file": file_fingerprint(audio_path)})
            self.conn.execute("""UPDATE media_items SET audio_fingerprint=?,asr_status='pending',
                asr_fingerprint=NULL,asr_phase=NULL,asr_task_id=NULL,asr_progress=0,
                asr_message='audio rebuilt',asr_error=NULL,txt_path=NULL,srt_path=NULL,json_path=NULL WHERE id=?""",
                (fingerprint,item_id))
            self.conn.commit()

    def mark_audio_pending(self, item_id: int) -> None:
        now = utc_now()
        with self._lock:
            self.conn.execute(
                """
                UPDATE media_items
                SET audio_status = 'pending', audio_fingerprint = NULL,
                    audio_error = NULL, asr_status = 'pending', asr_fingerprint = NULL,
                    asr_task_id = NULL, asr_phase = NULL, asr_progress = 0,
                    asr_error = NULL, asr_message = 'audio unavailable',
                    txt_path = NULL, srt_path = NULL, json_path = NULL,
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
                    asr_task_id = ?,
                    asr_phase = CASE WHEN ? = 'async' THEN CASE WHEN ? IS NULL THEN 'submitting' ELSE 'polling' END ELSE 'writing' END,
                    asr_progress = ?,
                    asr_message = ?,
                    asr_error = NULL,
                    asr_attempts = asr_attempts + 1,
                    asr_started_at = COALESCE(asr_started_at, ?),
                    asr_updated_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, mode, server_url, task_id, mode, task_id, progress, message, now, now, now, item_id),
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
                    asr_phase = NULL,
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
        self.require_private()
        now = utc_now()
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [now]
        if stage == "audio":
            sql = f"""
                UPDATE media_items
                SET audio_status = 'pending', audio_error = NULL, audio_fingerprint = NULL,
                    asr_status = 'pending', asr_fingerprint = NULL, asr_task_id = NULL,
                    asr_phase = NULL, asr_progress = 0, asr_error = NULL,
                    asr_message = 'audio reset', txt_path = NULL, srt_path = NULL, json_path = NULL,
                    updated_at = ?
                WHERE audio_status IN ({placeholders})
            """
        else:
            sql = f"""
                UPDATE media_items
                SET asr_status = 'pending', asr_progress = 0.0, asr_task_id = NULL, asr_phase = NULL, asr_fingerprint = NULL,
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

    def get_item(self, item_id: int) -> sqlite3.Row:
        with self._lock:
            return self.conn.execute("SELECT * FROM media_items WHERE id=?", (item_id,)).fetchone()

    def all_items(self, group: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute("SELECT * FROM media_items" + (" WHERE source_group=?" if group else "") + " ORDER BY id", (group,) if group else ()).fetchall())

    def set_asr_phase(self, item_id: int, phase: str | None, *, error: str | None = None) -> None:
        with self._lock, self.conn:
            self.conn.execute("UPDATE media_items SET asr_phase=?,asr_error=?,asr_updated_at=?,updated_at=? WHERE id=?",
                              (phase,error,utc_now(),utc_now(),item_id))

    def set_asr_fingerprint(self, item_id: int, fingerprint: str) -> None:
        with self._lock, self.conn:
            self.conn.execute("UPDATE media_items SET asr_fingerprint=? WHERE id=?", (fingerprint,item_id))

    def invalidate_asr(self, item_id: int, reason: str, *, keep_task: bool = False) -> None:
        with self._lock, self.conn:
            self.conn.execute("""UPDATE media_items SET asr_status=?,asr_progress=0,asr_message=?,asr_error=NULL,
                asr_phase=?,asr_task_id=CASE WHEN ? THEN asr_task_id ELSE NULL END,
                txt_path=NULL,srt_path=NULL,json_path=NULL,updated_at=? WHERE id=?""",
                ("running" if keep_task else "pending",reason,"polling" if keep_task else None,keep_task,utc_now(),item_id))

    def invalidate_audio(self, item_id: int, reason: str) -> None:
        with self._lock, self.conn:
            self.conn.execute("UPDATE media_items SET audio_status='pending',audio_fingerprint=NULL,audio_error=?,updated_at=? WHERE id=?",
                              (reason,utc_now(),item_id))
        self.invalidate_asr(item_id, reason)

    def adopt_legacy(self, item_id: int) -> None:
        row = self.get_item(item_id)
        source = file_fingerprint(Path(row["video_path"]))
        audio = Path(row["audio_path"]) if row["audio_path"] else None
        fingerprint = stable_fingerprint({"source": source, "preset": row["audio_preset"], "rule": 1,
                                          "file": file_fingerprint(audio)}) if audio and audio.is_file() else None
        with self._lock, self.conn:
            self.conn.execute("UPDATE media_items SET source_fingerprint=?,audio_fingerprint=?,updated_at=? WHERE id=?",
                              (source,fingerprint,utc_now(),item_id))

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
    try:
        db.require_private()
    except ValueError:
        db.close()
        raise
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

        for item in db.get_all_group_items(source_group):
            if item["audio_status"] == "done" and item["audio_preset"] != preset.name:
                db.invalidate_audio(int(item["id"]), "audio preset changed")

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

        targets: dict[str, list[int]] = {}
        for item in all_rows:
            target = (audio_root / Path(item["video_rel_path"])).with_suffix(preset.ext)
            targets.setdefault(os.path.normcase(str(target)), []).append(int(item["id"]))
        conflicts = {i for ids in targets.values() if len(ids) > 1 for i in ids}
        for item_id in conflicts:
            db.mark_audio_failed(item_id, "Output path collision: multiple videos produce the same audio")
        todo_rows = [r for r in todo_rows if int(r["id"]) not in conflicts]
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
            return 2 if conflicts else 0

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
        counters = {"done": 0, "failed": len(conflicts), "skipped": 0, "audio_bytes": 0}
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
        retry_after = None
        try:
            retry_after = max(0.0, float(e.headers.get("Retry-After", "")))
        except (ValueError, TypeError, AttributeError):
            try:
                retry_after = max(0.0, parsedate_to_datetime(e.headers.get("Retry-After", "")).timestamp() - time.time())
            except (ValueError, TypeError, AttributeError, OverflowError):
                pass
        raise AsrHttpError(e.code, f"HTTP {e.code} {url}: {err_body}", retry_after) from e


def _fmt_srt_time(seconds: float) -> str:
    ms_total = max(0, int(round(seconds * 1000)))
    h, rem = divmod(ms_total, 3_600_000)
    m, rem = divmod(rem, 60_000)
    s, ms = divmod(rem, 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


class AsrProtocolError(RuntimeError):
    pass


class AsrHttpError(RuntimeError):
    def __init__(self, status: int, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


class RecoverableTaskError(RuntimeError):
    """The known remote identity must survive this local failure."""


class SubmissionUnknownError(RecoverableTaskError):
    pass


class RemoteTaskFailed(RuntimeError):
    pass


def _extract_segments_and_text(payload: Any) -> tuple[str, list[dict[str, Any]]]:
    root = payload
    completed = False
    for _ in range(8):
        if not isinstance(root, dict):
            raise AsrProtocolError("ASR result must be an object")
        completed = completed or str(root.get("status", root.get("state", ""))).lower() in DONE_ASR_STATUSES
        nested = next((root[k] for k in ("result", "data", "output") if isinstance(root.get(k), dict)), None)
        if nested is None:
            break
        root = nested
    else:
        raise AsrProtocolError("ASR result has too many nested wrappers")
    segments: list[dict[str, Any]] = []

    def segment(item: Any, word: bool = False) -> dict[str, Any]:
        if not isinstance(item, dict):
            raise AsrProtocolError("ASR segment must be an object")
        start = item.get("start", item.get("start_time"))
        end = item.get("end", item.get("end_time"))
        text = item.get("word", item.get("text", "")) if word else item.get("text", item.get("content", ""))
        if (isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, (int, float))
                or not isinstance(end, (int, float)) or not math.isfinite(start) or not math.isfinite(end)
                or start < 0 or end < start or not isinstance(text, str)):
            raise AsrProtocolError("ASR segment has invalid timestamps or text")
        return {"start": float(start), "end": float(end), "text": text.strip() if not word else text}

    recognized = False
    for key in ("segments", "utterances", "sentences", "chunks"):
        if key in root:
            recognized = True
            if not isinstance(root[key], list):
                raise AsrProtocolError(f"ASR {key} must be a list")
            segments = [segment(item) for item in root[key]]
            segments = [item for item in segments if item["text"]]
            break
    if not segments and "words" in root:
        recognized = True
        if not isinstance(root["words"], list):
            raise AsrProtocolError("ASR words must be a list")
        words = [segment(item, True) for item in root["words"]]
        text_parts: list[str] = []
        start: float | None = None
        end = 0.0
        for word in words:
            if not word["text"]:
                continue
            if start is None:
                start = word["start"]
            text_parts.append(word["text"])
            end = word["end"]
            joined = "".join(text_parts).strip()
            if len(joined) >= 24 or (joined and joined[-1] in "。！？.!?") or end - start >= 6:
                segments.append({"start": start, "end": end, "text": joined})
                text_parts, start = [], None
        if start is not None and text_parts:
            segments.append({"start": start, "end": end, "text": "".join(text_parts).strip()})
    if "text" in root and not isinstance(root["text"], str):
        raise AsrProtocolError("ASR text must be a string")
    text = root.get("text", "").strip()
    if not text and segments:
        text = "\n".join(item["text"] for item in segments)
    if not text and not segments and "text" not in root and not (recognized and completed):
        raise AsrProtocolError("Unrecognized or incomplete ASR result; no explicit speech result")
    return text, segments


def _build_srt(segments: list[dict[str, Any]], fallback_text: str = "", duration_sec: float | None = None) -> str:
    if not segments:
        return f"1\n00:00:00,000 --> 00:00:05,000\n{fallback_text.strip()}\n" if fallback_text.strip() else ""
    return "\n\n".join(f"{i}\n{_fmt_srt_time(s['start'])} --> {_fmt_srt_time(s['end'])}\n{s['text']}"
                       for i, s in enumerate(segments, 1)) + "\n"


class AsrClient:
    """The HTTP contract, error classification and result normalization in one place."""

    def __init__(self, endpoint: AsrEndpointConfig, args: argparse.Namespace, stop: threading.Event,
                 transport: Any = None, wait: Any = None) -> None:
        self.ep = endpoint
        self.args = args
        self.stop = stop
        self.transport = transport or _http_request
        self.wait = wait or stop.wait

    def request(self, method: str, route: str, **kwargs: Any) -> tuple[int, bytes, str]:
        return self.transport(method, self.ep.base_url + route, api_key=self.ep.api_key,
                              extra_headers=self.ep.headers, **kwargs)

    def submit(self, path: Path) -> str:
        fields = {"return_timestamps": "true"}
        if self.ep.include_words:
            fields["include_words"] = "true"
        if self.args.language:
            fields["language"] = self.args.language
        body, content_type = _encode_multipart(fields, "file", path)
        _, raw, _ = self.request("POST", "/v1/asr/tasks", body=body, content_type=content_type, timeout=self.args.timeout)
        created = json.loads(raw.decode("utf-8"))
        task_id = (created.get("task_id") or created.get("id") or (created.get("data") or {}).get("task_id")) if isinstance(created, dict) else None
        if not isinstance(task_id, (str, int)) or not str(task_id):
            raise AsrProtocolError("Server did not return a task_id; submission outcome needs manual verification")
        return str(task_id)

    def sync(self, path: Path) -> tuple[str, str, Any]:
        fields = {"response_format": "json", "return_timestamps": "true"}
        if self.ep.include_words:
            fields["include_words"] = "true"
        if self.args.language:
            fields["language"] = self.args.language
        body, content_type = _encode_multipart(fields, "file", path)
        _, raw, _ = self.request("POST", "/v1/asr/transcribe", body=body, content_type=content_type, timeout=self.args.timeout)
        payload = json.loads(raw.decode("utf-8"))
        text, segments = _extract_segments_and_text(payload)
        # Both entry points use the same local fallback instead of repeating inference for SRT.
        return text, _build_srt(segments, text), payload

    def poll(self, task_id: str, db: StateDB, item_id: int) -> tuple[str, str, Any]:
        route = "/v1/asr/tasks/" + urllib.parse.quote(task_id, safe="")
        deadline = time.monotonic() + self.args.task_timeout
        errors = 0
        while not self.stop.is_set():
            if time.monotonic() >= deadline:
                raise RecoverableTaskError("Local task wait elapsed; rerun to resume the same task")
            try:
                _, raw, _ = self.request("GET", route, timeout=min(60.0, max(0.1, deadline-time.monotonic())))
                payload = json.loads(raw.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise AsrProtocolError("Task status must be an object")
                errors = 0
            except Exception as exc:
                retryable = isinstance(exc, (TimeoutError, ConnectionError, OSError, urllib.error.URLError)) or (isinstance(exc, AsrHttpError) and (exc.status == 429 or exc.status >= 500))
                if not retryable or errors >= self.args.poll_retries:
                    raise RecoverableTaskError(f"Polling paused: {exc}; task identity retained. A missing/expired task requires explicit reset or overwrite.") from exc
                delay = min(30.0, 2.0 ** errors)
                if isinstance(exc, AsrHttpError) and exc.retry_after is not None:
                    delay = max(delay, exc.retry_after)
                errors += 1
                self.wait(min(delay, max(0.0, deadline-time.monotonic())))
                continue
            status, progress, message = _parse_progress(payload)
            if status in FAIL_ASR_STATUSES:
                raise RemoteTaskFailed(str(payload.get("error") or payload.get("message") or status))
            if status in DONE_ASR_STATUSES:
                db.set_asr_phase(item_id, "writing")
                text, segments = _extract_segments_and_text(payload)
                subtitle = _build_srt(segments, text)
                try:
                    _, raw, _ = self.request("GET", route + "/subtitle?format=srt", timeout=60.0)
                    if raw.strip():
                        subtitle = raw.decode("utf-8")
                except Exception:
                    pass
                return text, subtitle, payload
            db.update_asr_progress(item_id, status="queued" if status in ("queued", "pending") else "running",
                                   progress=progress, message=message or status, task_id=task_id)
            self.wait(min(max(0.0, self.args.poll_interval), max(0.0, deadline-time.monotonic())))
        raise RecoverableTaskError("Interrupted; remote task retained")

    def delete(self, task_id: str) -> None:
        try:
            self.request("DELETE", "/v1/asr/tasks/" + urllib.parse.quote(task_id, safe=""), timeout=30.0)
        except Exception as exc:
            print(f"[WARN] Remote cleanup failed for {task_id}: {exc}", file=sys.stderr)


def request_fingerprint(row: sqlite3.Row, audio: Path, ep: AsrEndpointConfig, args: argparse.Namespace) -> str:
    return stable_fingerprint({"version": 1, "audio": row["audio_fingerprint"], "file": file_fingerprint(audio),
        "input_path": str(audio.resolve()), "endpoint": normalize_base_url(ep.base_url), "language": args.language or None, "include_words": ep.include_words})


def ensure_remote_task(db: StateDB, row: sqlite3.Row, client: AsrClient, fingerprint: str, overwrite: bool) -> str:
    item_id = int(row["id"])
    if not overwrite and row["asr_phase"] in ("submitting", "submission_unknown"):
        db.set_asr_phase(item_id, "submission_unknown", error="Submission outcome unknown; verify before reset/overwrite (may duplicate remote work)")
        raise SubmissionUnknownError("Submission outcome unknown; verify before reset/overwrite (may duplicate remote work)")
    if not overwrite and row["asr_task_id"] and row["asr_status"] in ("running", "queued"):
        if normalize_base_url(row["asr_server_url"] or "") != client.ep.base_url:
            raise RecoverableTaskError("Remote endpoint changed; use explicit overwrite to create a replacement task")
        if row["asr_fingerprint"] is None:
            raise RecoverableTaskError("Legacy task request parameters are unknown; verify before explicit overwrite")
        if row["asr_fingerprint"] == fingerprint:
            db.set_asr_phase(item_id, "polling")
            return str(row["asr_task_id"])
    db.mark_asr_started(item_id, mode="async", server_url=client.ep.base_url, status="queued", message="submitting task")
    db.set_asr_fingerprint(item_id, fingerprint)
    try:
        task_id = client.submit(Path(row["audio_path"]))
    except AsrHttpError as exc:
        # A definite client rejection did not create a task. Ambiguous responses retain submission_unknown.
        if 400 <= exc.status < 500 and exc.status not in (408, 429):
            db.set_asr_phase(item_id, None)
            raise
        db.set_asr_phase(item_id, "submission_unknown", error=str(exc))
        raise SubmissionUnknownError(str(exc)) from exc
    except Exception as exc:
        db.set_asr_phase(item_id, "submission_unknown", error=str(exc))
        raise SubmissionUnknownError(str(exc)) from exc
    db.update_asr_progress(item_id, status="queued", progress=1.0, message="task created", task_id=task_id)
    db.set_asr_phase(item_id, "polling")
    return task_id


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
    ep = resolve_asr_endpoint(endpoint=args.endpoint, url=args.url, api_key=args.api_key,
        modal_key=args.modal_key, modal_secret=args.modal_secret, cli_headers=args.header, include_words=args.include_words)
    ep = AsrEndpointConfig(ep.name, normalize_base_url(ep.base_url), ep.api_key, ep.headers, ep.include_words)
    errors = _validate_endpoint_auth(ep)
    if errors:
        for error in errors:
            print(f"Error: {error}", file=sys.stderr)
        return 1
    if args.poll_retries < 0 or args.timeout <= 0 or args.task_timeout <= 0 or args.poll_interval < 0:
        raise ValueError("Retry count and poll interval must be nonnegative; timeouts must be positive")
    db = StateDB(Path(args.db))
    stop = threading.Event()
    previous_handlers: dict[int, Any] = {}

    def interrupt(_signum: int, _frame: Any) -> None:
        print("[Interrupt] Stopping; remote tasks will remain resumable.", flush=True)
        stop.set()

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", signal.SIGINT)):
        if sig not in previous_handlers and threading.current_thread() is threading.main_thread():
            previous_handlers[sig] = signal.signal(sig, interrupt)
    try:
        db.require_private()
        root = Path(args.output_dir).resolve()
        concurrency = max(1, args.concurrency)
        capacity = concurrency + max(0, args.prefetch)
        client = AsrClient(ep, args, stop)
        seen: set[int] = set()
        selected_ids = {int(i.strip()) for i in args.ids.split(",") if i.strip()} if args.ids else None
        counts = {"done": 0, "failed": 0, "skipped": 0, "total": 0}
        counter_lock = threading.Lock()
        run_id = db.create_run("transcribe", args.group, {"endpoint": ep.name, "base_url": ep.base_url,
            "mode": args.mode, "concurrency": concurrency, "prefetch": args.prefetch, "watch": args.watch,
            "language": args.language, "include_words": ep.include_words, "output_dir": str(root),
            "save_json": args.save_json, "delete_remote_task": not args.keep_remote_task}, 0)
        print(f"ASR Endpoint: {ep.name} ({ep.base_url}) | Concurrency: {concurrency} | Prefetch: {args.prefetch}", flush=True)

        def failure(row: sqlite3.Row, exc: Exception, preserve: bool = False) -> None:
            item_id = int(row["id"])
            current = db.get_item(item_id)
            if preserve:
                phase = current["asr_phase"]
                db.set_asr_phase(item_id, phase, error=str(exc))
            else:
                db.mark_asr_failed(item_id, str(exc))
                db.set_asr_phase(item_id, None, error=str(exc))
            with counter_lock:
                counts["failed"] += 1
                print(f"[FAILED] {row['video_rel_path']}: {exc}", file=sys.stderr, flush=True)

        def candidates() -> list[sqlite3.Row]:
            all_rows = db.all_items(args.group)
            target_ids: dict[str, list[int]] = {}
            for row in all_rows:
                paths = build_output_paths(root, row["source_group"], row["video_rel_path"], args.save_json)
                for target in paths.values():
                    owners = target_ids.setdefault(os.path.normcase(str(target)), [])
                    if int(row["id"]) not in owners:
                        owners.append(int(row["id"]))
            conflicts = {i for ids in target_ids.values() if len(ids) > 1 for i in ids}
            result: list[sqlite3.Row] = []
            for row in all_rows:
                item_id = int(row["id"])
                if item_id in seen or (selected_ids is not None and item_id not in selected_ids):
                    continue
                if args.limit and counts["total"] >= args.limit:
                    break
                if row["audio_status"] != "done" or not row["audio_path"]:
                    continue
                if item_id in conflicts:
                    seen.add(item_id)
                    counts["total"] += 1
                    failure(row, ValueError("Output path collision: multiple inputs produce the same transcript"))
                    continue
                audio = Path(row["audio_path"])
                if not audio.is_file() or audio.stat().st_size == 0:
                    seen.add(item_id)
                    counts["total"] += 1
                    failure(row, FileNotFoundError(f"Audio missing or empty: {audio}"))
                    continue
                if not args.overwrite and row["asr_phase"] in ("submitting", "submission_unknown"):
                    seen.add(item_id)
                    counts["total"] += 1
                    db.set_asr_phase(item_id, "submission_unknown")
                    failure(row, SubmissionUnknownError("Submission outcome unknown; verify before explicit reset/overwrite (may duplicate work)"), True)
                    continue
                if row["asr_status"] == "failed" and args.no_retry_failed and not args.overwrite:
                    continue
                fingerprint = request_fingerprint(row, audio, ep, args)
                if row["asr_status"] == "done" and not args.overwrite and row["asr_fingerprint"] is None:
                    print(f"[LEGACY] {row['video_rel_path']}: result provenance unverified; use reconcile or explicit overwrite.", flush=True)
                    seen.add(item_id)
                    counts["skipped"] += 1
                    continue
                if (not args.overwrite and row["asr_task_id"] and row["asr_status"] in ("running", "queued")
                        and (normalize_base_url(row["asr_server_url"] or "") != ep.base_url
                             or row["asr_fingerprint"] not in (None, fingerprint))):
                    seen.add(item_id)
                    counts["total"] += 1
                    failure(row, RecoverableTaskError("Active task endpoint or request differs; explicit overwrite is required to replace it"), True)
                    continue
                if not args.overwrite and row["asr_fingerprint"] and row["asr_fingerprint"] != fingerprint:
                    db.invalidate_asr(item_id, "audio or request parameters changed")
                    row = db.get_item(item_id)
                if row["asr_status"] == "done" and not args.overwrite:
                    store = TranscriptStore(build_output_paths(root, row["source_group"], row["video_rel_path"], args.save_json))
                    if store.valid(file_fingerprint(audio), fingerprint):
                        seen.add(item_id)
                        counts["skipped"] += 1
                        continue
                    if args.save_json and not row["json_path"]:
                        seen.add(item_id)
                        counts["total"] += 1
                        failure(row, ValueError("JSON was not saved; use explicit overwrite to regenerate raw response"), True)
                        continue
                    db.invalidate_asr(item_id, "output missing, changed, or not committed")
                    row = db.get_item(item_id)
                seen.add(item_id)
                counts["total"] += 1
                result.append(row)
            return result

        def process(row: sqlite3.Row, mode: str, fingerprint: str, task_id: str | None) -> None:
            if stop.is_set():
                return
            item_id = int(row["id"])
            audio = Path(row["audio_path"])
            try:
                if mode == "async":
                    task_id = task_id or ensure_remote_task(db, db.get_item(item_id), client, fingerprint, args.overwrite)
                    text, subtitle, payload = client.poll(task_id, db, item_id)
                else:
                    db.mark_asr_started(item_id, mode="sync", server_url=ep.base_url, message="transcribing")
                    db.set_asr_fingerprint(item_id, fingerprint)
                    text, subtitle, payload = client.sync(audio)
                source = file_fingerprint(audio)
                # Reject an audio file replaced while this request was in flight.
                if request_fingerprint(db.get_item(item_id), audio, ep, args) != fingerprint:
                    raise RecoverableTaskError("Audio changed during transcription; reconcile before retry")
                paths = build_output_paths(root, row["source_group"], row["video_rel_path"], args.save_json)
                db.set_asr_phase(item_id, "writing")
                TranscriptStore(paths).commit(text, subtitle, payload, source, fingerprint, str(audio.resolve()))
                db.mark_asr_done(item_id, txt_path=paths["txt"], srt_path=paths["srt"], json_path=paths.get("json"))
                if mode == "async" and not args.keep_remote_task and task_id:
                    client.delete(task_id)
                with counter_lock:
                    counts["done"] += 1
                    print(f"[DONE] ({mode}) {row['video_rel_path']}", flush=True)
            except RemoteTaskFailed as exc:
                failure(row, exc)
            except Exception as exc:
                failure(row, exc, bool(task_id) or isinstance(exc, RecoverableTaskError))

        ready = candidates()
        scheduled = 0
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures: set[concurrent.futures.Future[None]] = set()
                exhausted = False
                while not stop.is_set() and (ready or futures or not exhausted):
                    while ready and len(futures) < capacity and not stop.is_set():
                        row = ready.pop(0)
                        scheduled += 1
                        try:
                            mode, _duration = _resolve_transcribe_mode(row, Path(row["audio_path"]), args)
                            if not args.overwrite and row["asr_task_id"] and row["asr_status"] in ("running", "queued"):
                                mode = "async"
                            fingerprint = request_fingerprint(row, Path(row["audio_path"]), ep, args)
                            task_id = ensure_remote_task(db, row, client, fingerprint, args.overwrite) if args.prefetch > 0 and mode == "async" else None
                            futures.add(pool.submit(process, row, mode, fingerprint, task_id))
                        except Exception as exc:
                            failure(row, exc, isinstance(exc, RecoverableTaskError))
                    if futures:
                        done, futures = concurrent.futures.wait(futures, timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            future.result()
                    if not ready and not exhausted:
                        if not args.watch or (args.limit and counts["total"] >= args.limit):
                            exhausted = True
                        else:
                            ready = candidates()
                            if not ready:
                                if db.count_unfinished_audio(args.group) == 0:
                                    ready = candidates()  # final recheck after the last extraction commit
                                    exhausted = not ready
                                elif not futures:
                                    stop.wait(max(0.1, args.watch_interval))
                # Even on interruption, consume worker exceptions and let requests settle.
                for future in futures:
                    future.result()
        finally:
            db.finish_run(run_id, done_count=counts["done"], failed_count=counts["failed"], skipped_count=counts["skipped"],
                          status="interrupted" if stop.is_set() else "completed")
        print(f"Transcription finished: done={counts['done']} failed={counts['failed']} skipped={counts['skipped']}", flush=True)
        return 2 if counts["failed"] or stop.is_set() else 0
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        db.close()


def cmd_reconcile(args: argparse.Namespace) -> int:
    db = StateDB(Path(args.db))
    changes = 0
    try:
        if args.apply:
            db.require_private()
        if db.public_snapshot:
            print("Public archive index: no local execution state to reconcile.")
            return 0
        for row in db.all_items(args.group):
            item_id = int(row["id"])
            action = None
            reason = None
            video = Path(row["video_path"])
            audio = Path(row["audio_path"]) if row["audio_path"] else None
            if video.is_file() and row["source_fingerprint"] and file_fingerprint(video) != row["source_fingerprint"]:
                action, reason = "audio", "source changed"
            elif row["audio_status"] == "done" and (not audio or not audio.is_file() or audio.stat().st_size == 0):
                action, reason = "audio", "audio missing or empty"
            elif row["source_fingerprint"] is None and row["audio_status"] == "done":
                print(f"[LEGACY] {item_id} {row['video_rel_path']}: provenance unverified")
                if args.apply and args.adopt_legacy and video.is_file():
                    db.adopt_legacy(item_id)
                    print("  Adopted current source/audio as a baseline; historical ASR parameters remain unverified.")
                    changes += 1
            if action is None and row["asr_status"] in ("done", "running", "queued", "failed") and audio and audio.is_file():
                if row["txt_path"] and row["srt_path"]:
                    txt, srt = Path(row["txt_path"]), Path(row["srt_path"])
                else:
                    paths = build_output_paths(Path(args.output_dir).resolve(), row["source_group"], row["video_rel_path"], False)
                    txt, srt = paths["txt"], paths["srt"]
                paths = {"txt": txt, "srt": srt, "marker": txt.with_name("." + txt.stem + ".complete.json")}
                if row["json_path"]:
                    paths["json"] = Path(row["json_path"])
                store = TranscriptStore(paths)
                if row["asr_fingerprint"] and store.valid(file_fingerprint(audio), row["asr_fingerprint"]):
                    if row["asr_status"] != "done":
                        action, reason = "complete", "valid committed files found"
                elif row["asr_status"] == "done":
                    if row["asr_fingerprint"] or not txt.is_file() or not srt.is_file():
                        action, reason = "asr", "transcript missing, changed, or incomplete"
                    else:
                        print(f"[LEGACY] {item_id}: historical transcript has no completion marker; explicit overwrite verifies it.")
            if action:
                changes += 1
                print(f"[{'APPLY' if args.apply else 'PLAN'}] {item_id} {row['video_rel_path']}: {reason}")
                if args.apply:
                    if action == "audio":
                        db.invalidate_audio(item_id, reason)
                    elif action == "asr":
                        db.invalidate_asr(item_id, reason)
                    else:
                        marker = store.read_marker()
                        json_file = marker.get("files", {}).get("json") if marker else None
                        json_path = txt.parent / json_file["name"] if json_file else None
                        db.mark_asr_done(item_id, txt_path=txt, srt_path=srt, json_path=json_path)
        print(f"Reconcile: {changes} action(s); {'applied' if args.apply else 'dry run, use --apply to write'}.")
        return 0
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
    p_tr.add_argument("--poll-retries", type=int, default=3, help="Consecutive transient poll retries; retain task identity when exhausted")
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

    p_rec = sub.add_parser("reconcile", help="Check local files and state (dry run unless --apply)")
    p_rec.add_argument("--group", help="Filter source group")
    p_rec.add_argument("--apply", action="store_true", help="Apply reported state repairs")
    p_rec.add_argument("--adopt-legacy", action="store_true", help="With --apply, adopt current source/audio metadata as an unverified legacy baseline")
    p_rec.add_argument("--output-dir", default=str(DEFAULT_TRANSCRIPT_DIR), help="Output root for finding commits not yet stored in DB")
    p_rec.set_defaults(func=cmd_reconcile)

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
    try:
        return int(args.func(args))
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
