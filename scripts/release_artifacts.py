#!/usr/bin/env python3
"""Package, publish, and restore ASR transcript artifacts for GitHub Releases.

Keeps Git repositories code-only while organizing transcript outputs (.txt/.srt,
word-level .json, and a clean SQLite pipeline.db snapshot) into versioned
GitHub Release assets (< 2 GiB each) with SHA-256 verification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
TRANSCRIPTS_DIR = DATA_DIR / "transcripts"
DB_PATH = DATA_DIR / "pipeline.db"
DEFAULT_OUT_DIR = REPO_ROOT / "release_dist"
DEFAULT_MAX_PART_RAW_MB = 450  # ~80-90 MB compressed for JSON/text, avoids long TCP resets
GITHUB_ASSET_LIMIT_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB hard GitHub limit


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def format_size(num_bytes: int) -> str:
    units = ("B", "KB", "MB", "GB")
    val = float(num_bytes)
    for u in units:
        if val < 1024.0 or u == units[-1]:
            return f"{val:.2f} {u}" if u != "B" else f"{int(val)} B"
        val /= 1024.0
    return f"{num_bytes} B"


def discover_groups(explicit_groups: list[str] | None) -> list[str]:
    if explicit_groups:
        return explicit_groups
    groups: list[str] = []
    if DB_PATH.is_file():
        conn = sqlite3.connect(str(DB_PATH))
        try:
            rows = conn.execute(
                "SELECT DISTINCT source_group FROM media_items ORDER BY source_group"
            ).fetchall()
            groups = [r[0] for r in rows if r[0] and (TRANSCRIPTS_DIR / r[0]).is_dir()]
        finally:
            conn.close()
    if not groups and TRANSCRIPTS_DIR.is_dir():
        groups = sorted(p.name for p in TRANSCRIPTS_DIR.iterdir() if p.is_dir())
    return groups


def build_tar_gz(
    archive_path: Path,
    files: Iterable[tuple[Path, str]],
    compresslevel: int = 6,
) -> tuple[int, int]:
    """Write a PAX-format .tar.gz archive; returns (file_count, raw_bytes)."""
    count = 0
    raw_bytes = 0
    tmp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    try:
        with tarfile.open(
            tmp_path,
            mode="w:gz",
            format=tarfile.PAX_FORMAT,
            compresslevel=compresslevel,
        ) as tf:
            for src, arcname in files:
                tf.add(src, arcname=arcname, recursive=False)
                count += 1
                raw_bytes += src.stat().st_size
        tmp_path.replace(archive_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    return count, raw_bytes


PUBLIC_MEDIA_FIELDS = frozenset({
    "id", "source_group", "video_id", "title", "video_size_bytes", "video_mtime",
    "duration_sec", "audio_status", "audio_preset", "audio_format", "audio_size_bytes",
    "asr_status", "asr_mode", "asr_progress", "asr_attempts", "created_at", "updated_at",
    "audio_started_at", "audio_finished_at", "asr_started_at", "asr_updated_at", "asr_finished_at",
})
PUBLIC_RUN_FIELDS = frozenset({
    "id", "run_type", "source_group", "total_count", "done_count", "failed_count",
    "skipped_count", "status", "started_at", "finished_at",
})
PUBLIC_PATH_FIELDS = frozenset({"video_path", "video_rel_path", "audio_path", "txt_path", "srt_path", "json_path"})


def _relative_public_path(value: str | None, fallback: str = "") -> str:
    if not value:
        return fallback
    text = str(value).replace("\\", "/")
    parts = [p for p in text.split("/") if p not in ("", ".", "..") and not p.endswith(":")]
    if "data" in parts:
        parts = parts[parts.index("data"):]
    elif PureWindowsPath(text).is_absolute() or text.startswith("/"):
        parts = parts[-1:]
    return "/".join(parts) or fallback


def _db_profile(sanitize: bool | None, db_profile: str | None) -> str:
    if db_profile not in (None, "public", "private"):
        raise ValueError("db_profile must be public or private")
    if sanitize is False and db_profile == "public":
        raise ValueError("--no-sanitize-db conflicts with --db-profile public")
    return db_profile or ("private" if sanitize is False else "public")


def export_clean_db(dest_db: Path, sanitize: bool | None = None, *, db_profile: str | None = None) -> None:
    """Online WAL backup; public snapshots retain table schemas but only allowlisted values."""
    profile = _db_profile(sanitize, db_profile)
    if dest_db.resolve() == DB_PATH.resolve():
        raise ValueError("Snapshot destination must differ from the live database")
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".snapshot-", dir=dest_db.parent) as td:
        staged = Path(td) / "pipeline.db"
        _export_db_snapshot(staged, profile)
        os.replace(staged, dest_db)


def _export_db_snapshot(dest_db: Path, profile: str) -> None:
    # Snapshot first: the source may have active WAL writers or an older schema.
    with tempfile.TemporaryDirectory() as td:
        snapshot = sqlite3.connect(str(Path(td) / "snapshot.db"))
        source = sqlite3.connect(DB_PATH.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            source.backup(snapshot)
            dest = sqlite3.connect(str(dest_db))
            try:
                if profile == "private":
                    snapshot.backup(dest)
                else:
                    snapshot.row_factory = sqlite3.Row
                    for table, allowed in (("media_items", PUBLIC_MEDIA_FIELDS), ("pipeline_runs", PUBLIC_RUN_FIELDS)):
                        schema = snapshot.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                        if not schema:
                            continue
                        dest.execute(schema[0])
                        columns = snapshot.execute(f'PRAGMA table_info("{table}")').fetchall()
                        names = [c[1] for c in columns]
                        quoted = ",".join('"' + n.replace('"', '""') + '"' for n in names)
                        for row in snapshot.execute(f'SELECT * FROM "{table}"'):
                            values = []
                            for col in columns:
                                name = col[1]
                                if name in allowed:
                                    value = row[name]
                                elif table == "media_items" and name in PUBLIC_PATH_FIELDS:
                                    value = _relative_public_path(row[name])
                                    if name == "video_path":
                                        value = _relative_public_path(row["video_rel_path"] if "video_rel_path" in names else None, value)
                                        value = f"{row['id']}/{value}" if "id" in names else value
                                else:
                                    value = None
                                if value is None and col[3]:
                                    value = 0 if "INT" in col[2].upper() or "REAL" in col[2].upper() else ""
                                values.append(value)
                            dest.execute(f'INSERT INTO "{table}" ({quoted}) VALUES ({",".join("?" for _ in names)})', values)
                dest.execute("CREATE TABLE IF NOT EXISTS snapshot_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                dest.executemany("INSERT OR REPLACE INTO snapshot_metadata VALUES (?, ?)", [("profile", profile), ("sanitization_version", "1" if profile == "public" else "0")])
                dest.commit()
                dest.execute("PRAGMA journal_mode=DELETE")
                dest.execute("VACUUM")
                dest.commit()
            finally:
                dest.close()
        finally:
            source.close()
            snapshot.close()


def collect_group_files(group: str, exts: set[str]) -> list[tuple[Path, str]]:
    _safe_name(group)
    group_dir = TRANSCRIPTS_DIR / group
    if group_dir.is_symlink():
        raise ValueError(f"Symlink transcript group rejected: {group}")
    if not group_dir.is_dir():
        return []
    result: list[tuple[Path, str]] = []
    for p in sorted(group_dir.rglob("*")):
        if p.is_file() and not p.is_symlink() and not p.name.startswith(".") and p.suffix.lower() in exts:
            rel = p.relative_to(REPO_ROOT).as_posix()
            result.append((p, rel))
    return result


def partition_files(
    files: list[tuple[Path, str]],
    max_raw_bytes: int,
) -> list[list[tuple[Path, str]]]:
    """Split files into standalone chunks so each archive stays fast and upload-safe."""
    if not files:
        return []
    sized = [(src, arc, src.stat().st_size) for src, arc in files]
    total_raw = sum(sz for _, _, sz in sized)
    if max_raw_bytes <= 0 or total_raw <= max_raw_bytes:
        return [files]

    chunks: list[list[tuple[Path, str]]] = []
    cur: list[tuple[Path, str]] = []
    cur_bytes = 0
    for src, arc, sz in sized:
        if cur and cur_bytes + sz > max_raw_bytes:
            chunks.append(cur)
            cur = []
            cur_bytes = 0
        cur.append((src, arc))
        cur_bytes += sz
    if cur:
        chunks.append(cur)
    return chunks


def _package_batch(
    out_dir: Path,
    groups: list[str],
    include_json: bool = True,
    include_db: bool = True,
    sanitize_db: bool | None = None,
    compresslevel: int = 6,
    max_part_raw_mb: int = DEFAULT_MAX_PART_RAW_MB,
    db_profile: str | None = None,
) -> dict:
    profile = _db_profile(sanitize_db, db_profile)
    out_dir.mkdir(parents=True, exist_ok=True)
    max_raw_bytes = max_part_raw_mb * 1024 * 1024 if max_part_raw_mb > 0 else 0
    started = time.time()
    assets: list[dict] = []

    print(
        f"Packaging groups: {', '.join(groups)} -> {out_dir} "
        f"(max_part_raw={max_part_raw_mb} MB)"
    )

    def pack_bucket(
        group: str,
        stem: str,
        kind: str,
        bucket_files: list[tuple[Path, str]],
    ) -> None:
        chunks = partition_files(bucket_files, max_raw_bytes)
        multi = len(chunks) > 1
        for i, chunk in enumerate(chunks, 1):
            arc_name = f"{stem}-part{i:02d}.tar.gz" if multi else f"{stem}.tar.gz"
            arc = out_dir / arc_name
            t0 = time.time()
            cnt, raw_b = build_tar_gz(arc, chunk, compresslevel=compresslevel)
            comp_b = arc.stat().st_size
            digest = sha256_file(arc)
            print(
                f"  [OK] {arc.name}: {cnt} files, {format_size(raw_b)} -> {format_size(comp_b)} "
                f"({time.time() - t0:.1f}s)"
            )
            assets.append(
                {
                    "name": arc.name,
                    "group": group,
                    "kind": kind,
                    "part": f"{i}/{len(chunks)}" if multi else None,
                    "files": cnt,
                    "raw_bytes": raw_b,
                    "archive_bytes": comp_b,
                    "sha256": digest,
                }
            )

    for group in groups:
        # 1. Text & subtitles (.txt, .srt)
        text_files = collect_group_files(group, {".txt", ".srt"})
        if text_files:
            pack_bucket(group, f"{group}-text", "text+srt", text_files)

        # 2. Word-level JSON (.json)
        if include_json:
            json_files = collect_group_files(group, {".json"})
            if json_files:
                pack_bucket(group, f"{group}-json", "word-json", json_files)

    # 3. Clean SQLite snapshot
    if include_db and DB_PATH.is_file():
        with tempfile.TemporaryDirectory() as td:
            clean_db = Path(td) / "pipeline.db"
            export_clean_db(clean_db, db_profile=profile)
            arc = out_dir / "pipeline-db.tar.gz"
            t0 = time.time()
            cnt, raw_b = build_tar_gz(
                arc, [(clean_db, "data/pipeline.db")], compresslevel=compresslevel
            )
            comp_b = arc.stat().st_size
            digest = sha256_file(arc)
            print(
                f"  [OK] {arc.name}: {cnt} file, {format_size(raw_b)} -> {format_size(comp_b)} "
                f"({time.time() - t0:.1f}s)"
            )
            assets.append(
                {
                    "name": arc.name,
                    "group": "_meta",
                    "kind": "sqlite-db",
                    "files": cnt,
                    "raw_bytes": raw_b,
                    "archive_bytes": comp_b,
                    "sha256": digest,
                }
            )

    # Validate GitHub 2 GiB single-asset limit
    oversized = [a for a in assets if a["archive_bytes"] >= GITHUB_ASSET_LIMIT_BYTES]
    if oversized:
        names = ", ".join(f"{a['name']} ({format_size(a['archive_bytes'])})" for a in oversized)
        raise RuntimeError(f"Assets exceed GitHub 2 GiB limit: {names}")

    # Write MANIFEST.json and SHA256SUMS.txt
    manifest = {
        "schema_version": 2,
        "release_id": uuid.uuid4().hex,
        "db_profile": profile,
        "sanitization_version": 1 if profile == "public" else 0,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groups": groups,
        "total_archive_bytes": sum(a["archive_bytes"] for a in assets),
        "total_raw_bytes": sum(a["raw_bytes"] for a in assets),
        "assets": assets,
    }
    manifest_path = out_dir / "MANIFEST.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    sums_lines = [f"{a['sha256']}  {a['name']}" for a in assets]
    sums_lines.append(f"{sha256_file(manifest_path)}  {manifest_path.name}")
    sums_path = out_dir / "SHA256SUMS.txt"
    sums_path.write_text("\n".join(sums_lines) + "\n", encoding="utf-8")

    print(
        f"Done in {time.time() - started:.1f}s | {len(assets)} archives | "
        f"raw={format_size(manifest['total_raw_bytes'])} -> "
        f"packed={format_size(manifest['total_archive_bytes'])}"
    )
    return manifest


def package_artifacts(out_dir: Path, groups: list[str], include_json: bool = True,
                      include_db: bool = True, sanitize_db: bool | None = None,
                      compresslevel: int = 6, max_part_raw_mb: int = DEFAULT_MAX_PART_RAW_MB,
                      db_profile: str | None = None) -> dict:
    """Build a complete sibling batch before replacing the current delivery directory."""
    out_dir = out_dir.resolve()
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    previous = out_dir.with_name(out_dir.name + ".previous")
    if not out_dir.exists() and previous.exists():
        previous.replace(out_dir)  # Recover a crash between the two directory renames.
    with tempfile.TemporaryDirectory(prefix=out_dir.name + ".staging-", dir=out_dir.parent) as td:
        stage = Path(td) / "batch"
        manifest = _package_batch(stage, groups, include_json, include_db, sanitize_db,
                                  compresslevel, max_part_raw_mb, db_profile)
        validate_release(stage)
        if previous.exists():
            shutil.rmtree(previous)
        if out_dir.exists():
            out_dir.replace(previous)
        try:
            stage.replace(out_dir)
        except BaseException:
            if previous.exists() and not out_dir.exists():
                previous.replace(out_dir)
            raise
    return manifest


def render_release_notes(tag: str, manifest: dict) -> str:
    lines = [
        f"## ASR Transcripts & Metadata Snapshot (`{tag}`)",
        "",
        "All transcript artifacts are packaged by source group with UTF-8 PAX `.tar.gz` archives.",
        "Pause all destination readers/writers, then run `python scripts/release_artifacts.py restore <dir>` to verify and restore `data/transcripts/...` and `data/pipeline.db`.",
        f"Database profile: **{manifest.get('db_profile', 'legacy')}**. Public snapshots are queryable archive indexes; create local state before extracting or transcribing.",
        "",
        "| Asset | Group | Type | Files | Raw Size | Archive Size |",
        "|---|---|---|---:|---:|---:|",
    ]
    for a in manifest["assets"]:
        lines.append(
            f"| `{a['name']}` | `{a['group']}` | {a['kind']} | {a['files']:,} | "
            f"{format_size(a['raw_bytes'])} | {format_size(a['archive_bytes'])} |"
        )
    lines.extend(
        [
            "",
            f"**Total**: {format_size(manifest['total_raw_bytes'])} raw -> "
            f"**{format_size(manifest['total_archive_bytes'])}** compressed.",
            "",
            "Verify integrity with `SHA256SUMS.txt` (`sha256sum -c SHA256SUMS.txt`).",
        ]
    )
    return "\n".join(lines) + "\n"


def publish_release(
    out_dir: Path,
    tag: str,
    title: str | None,
    manifest: dict,
    draft: bool = False,
    max_retries: int = 5,
) -> None:
    if not shutil.which("gh"):
        raise RuntimeError("GitHub CLI (`gh`) not found in PATH.")

    validated = validate_release(out_dir)
    if validated != manifest:
        raise ValueError("Manifest argument differs from packaged MANIFEST")
    upload_files = [out_dir / a["name"] for a in manifest["assets"]]
    upload_files.extend([out_dir / "MANIFEST.json", out_dir / "SHA256SUMS.txt"])
    missing = [str(p) for p in upload_files if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing release files: {missing}")

    notes = render_release_notes(tag, manifest)
    notes_file = out_dir / "RELEASE_NOTES.md"
    notes_file.write_text(notes, encoding="utf-8")

    # 1. Ensure release exists (create metadata-only first so partial upload never rolls back)
    check = subprocess.run(
        ["gh", "release", "view", tag, "--json", "assets,isDraft"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    existing_assets: dict[str, dict] = {}
    release_is_draft = True
    if check.returncode == 0:
        data = json.loads(check.stdout or "{}")
        release_is_draft = bool(data.get("isDraft", False))
        for asset in data.get("assets", []):
            existing_assets[asset["name"]] = asset
        print(f"Release {tag} exists ({len(existing_assets)} assets already uploaded); updating notes...")
        subprocess.run(
            ["gh", "release", "edit", tag, "--notes-file", str(notes_file)],
            cwd=str(REPO_ROOT),
            check=True,
        )
    else:
        print(f"Creating release {tag}...")
        cmd = [
            "gh",
            "release",
            "create",
            tag,
            "--title",
            title or f"ASR Transcripts {tag}",
            "--notes-file",
            str(notes_file),
        ]
        cmd.append("--draft")
        subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)

    # 2. Upload assets sequentially (smallest first) with digest-verified resume + retry
    ordered_files = sorted(upload_files[:-2], key=lambda p: p.stat().st_size) + [out_dir / "SHA256SUMS.txt", out_dir / "MANIFEST.json"]
    total = len(ordered_files)
    for idx, path in enumerate(ordered_files, 1):
        size = path.stat().st_size
        remote = existing_assets.get(path.name)
        if remote:
            remote_digest = (remote.get("digest") or "").lower()
            local_digest = f"sha256:{sha256_file(path)}"
            if remote_digest and remote_digest == local_digest:
                print(f"  [{idx}/{total}] SKIP (digest verified) {path.name} ({format_size(size)})")
                continue
        for attempt in range(1, max_retries + 1):
            t0 = time.time()
            print(
                f"  [{idx}/{total}] Uploading {path.name} ({format_size(size)}) "
                f"[attempt {attempt}/{max_retries}]..."
            )
            res = subprocess.run(
                ["gh", "release", "upload", tag, str(path), "--clobber"],
                cwd=str(REPO_ROOT),
            )
            if res.returncode == 0:
                print(f"  [{idx}/{total}] OK {path.name} ({time.time() - t0:.1f}s)")
                break
            if attempt == max_retries:
                raise RuntimeError(f"Failed to upload {path.name} after {max_retries} attempts.")
            wait_sec = min(30, 5 * attempt)
            print(f"  [{idx}/{total}] Upload failed (rc={res.returncode}); retrying in {wait_sec}s...")
            time.sleep(wait_sec)

    verified = subprocess.run(["gh", "release", "view", tag, "--json", "assets,isDraft"], cwd=str(REPO_ROOT), capture_output=True, text=True, encoding="utf-8", check=True)
    remote = {a["name"]: a for a in json.loads(verified.stdout).get("assets", [])}
    for path in upload_files:
        asset = remote.get(path.name)
        if not asset or asset.get("size") != path.stat().st_size:
            raise RuntimeError(f"Remote asset missing/size mismatch: {path.name}")
        digest = asset.get("digest")
        if digest and digest.lower() != "sha256:" + sha256_file(path):
            raise RuntimeError(f"Remote digest mismatch: {path.name}")
    if release_is_draft and not draft:
        subprocess.run(["gh", "release", "edit", tag, "--draft=false"], cwd=str(REPO_ROOT), check=True)


def _safe_name(name: str) -> str:
    if (not isinstance(name, str) or not name or name in (".", "..")
            or "/" in name or "\\" in name or PureWindowsPath(name).drive or ":" in name
            or name.endswith((".", " ")) or PureWindowsPath(name).is_reserved()):
        raise ValueError(f"Unsafe asset name: {name!r}")
    return name


def validate_release(src_dir: Path, skip_verify: bool = False) -> dict:
    """Validate the complete declared set before callers write any destination files."""
    manifest_path = src_dir / "MANIFEST.json"
    sums_path = src_dir / "SHA256SUMS.txt"
    if not manifest_path.is_file() or not sums_path.is_file():
        raise FileNotFoundError("MANIFEST.json and SHA256SUMS.txt required; repackage legacy directories")
    expected = {}
    for line in sums_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        fields = line.split(None, 1)
        if len(fields) != 2 or not re.fullmatch(r"[0-9a-fA-F]{64}", fields[0]):
            raise ValueError("Malformed SHA256SUMS.txt")
        digest, name = fields
        name = _safe_name(name.strip())
        if name in expected:
            raise ValueError(f"Duplicate checksum name: {name}")
        expected[name] = digest.lower()
    if "MANIFEST.json" not in expected:
        raise ValueError("Missing MANIFEST checksum")
    if not skip_verify and sha256_file(manifest_path) != expected["MANIFEST.json"]:
        raise RuntimeError("SHA-256 mismatch for MANIFEST.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("Manifest must be an object")
    version = manifest.get("schema_version", 1)
    if type(version) is not int or version not in (1, 2):
        raise ValueError("Unsupported manifest schema_version")
    if version == 2:
        if not isinstance(manifest.get("release_id"), str) or not manifest["release_id"]:
            raise ValueError("Missing release_id")
        profile = manifest.get("db_profile")
        if profile not in ("public", "private"):
            raise ValueError("Missing or invalid db_profile")
        if manifest.get("sanitization_version") != (1 if profile == "public" else 0):
            raise ValueError("Invalid sanitization_version for db_profile")
    if not isinstance(manifest.get("assets"), list):
        raise ValueError("Manifest assets must be a list")
    seen = set()
    seen_keys = set()
    for asset in manifest["assets"]:
        if not isinstance(asset, dict):
            raise ValueError("Manifest asset must be an object")
        name = _safe_name(asset.get("name"))
        if name.casefold() in seen_keys or not name.endswith(".tar.gz"):
            raise ValueError(f"Duplicate or invalid archive: {name}")
        seen.add(name)
        seen_keys.add(name.casefold())
        if name not in expected or not (src_dir / name).is_file() or (src_dir / name).is_symlink():
            raise FileNotFoundError(f"Missing declared asset/checksum: {name}")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", str(asset.get("sha256", ""))):
            raise ValueError(f"Missing or invalid manifest digest: {name}")
        if "archive_bytes" in asset and (src_dir / name).stat().st_size != asset["archive_bytes"]:
            raise RuntimeError(f"Size mismatch for {name}")
        if not skip_verify:
            if expected[name] != asset["sha256"].lower() or sha256_file(src_dir / name) != expected[name]:
                raise RuntimeError(f"SHA-256 mismatch for {name}")
    if set(expected) != seen | {"MANIFEST.json"}:
        raise ValueError("Checksum asset set differs from MANIFEST")
    extras = {p.name for p in src_dir.glob("*.tar.gz")} - seen
    if extras:
        print("Ignoring undeclared archives: " + ", ".join(sorted(extras)))
    return manifest


def _safe_target(root: Path, name: str) -> Path:
    # Check both POSIX and Windows syntax even when running on the other OS.
    path = PurePosixPath(name)
    if (not name or "\\" in name or path.is_absolute() or PureWindowsPath(name).drive
            or ".." in path.parts or any(":" in p or p.endswith((".", " "))
                                        or PureWindowsPath(p).is_reserved() for p in path.parts)):
        raise RuntimeError(f"Unsafe archive path: {name}")
    target = root.joinpath(*path.parts)
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError(f"Symlink destination rejected: {name}")
    try:
        target.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"Unsafe destination path: {name}") from exc
    return target


def _write_journal(path: Path, journal: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as stream:
        json.dump(journal, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def _recover_restore(root: Path) -> None:
    work = root / ".release-restore"
    log = work / "journal.json"
    if work.is_symlink() or log.is_symlink():
        raise RuntimeError("Restore workspace/journal must not be a symlink")
    if not log.exists():
        if work.exists():
            # A crash before the first journal write cannot have replaced targets.
            shutil.rmtree(work)
        return
    journal = json.loads(log.read_text(encoding="utf-8"))
    if journal["state"] != "complete":
        recovery = []
        for entry in reversed(journal["entries"]):
            target = _safe_target(root, entry["path"])
            backup = _safe_target(work / "backup", entry["path"])
            _check_db_sidecars(target)
            if entry["existed"] and not backup.is_file():
                raise RuntimeError(f"Restore backup missing; journal retained: {entry['path']}")
            recovery.append((entry, target, backup))
        for entry, target, backup in recovery:
            if entry["existed"]:
                target.parent.mkdir(parents=True, exist_ok=True)
                # Keep backup intact until the entire rollback succeeds (idempotent recovery).
                shutil.copy2(backup, target)
            elif target.exists():
                target.unlink()
        print("Recovered interrupted restore by rolling back target files")
    shutil.rmtree(work)


def _check_db_sidecars(target: Path) -> None:
    if target.suffix.lower() == ".db" and any(
        Path(str(target) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")
    ):
        raise RuntimeError("Close database users and checkpoint/remove WAL sidecars before restore")


def restore_artifacts(src_dir: Path, dest_root: Path, skip_verify: bool = False) -> None:
    if not hasattr(tarfile, "data_filter"):
        raise RuntimeError("tarfile data filter required; upgrade to patched Python 3.10.12+ or 3.12+")
    manifest = validate_release(src_dir, skip_verify)
    dest_root = dest_root.resolve()
    dest_root.mkdir(parents=True, exist_ok=True)
    _recover_restore(dest_root)
    work = dest_root / ".release-restore"
    work.mkdir()
    stage = work / "stage"
    stage.mkdir()
    log = work / "journal.json"
    journal = {"state": "staging", "entries": []}
    _write_journal(log, journal)
    try:
        seen = set()
        seen_keys = set()
        for asset in manifest["assets"]:
            with tarfile.open(src_dir / asset["name"], "r:gz") as tf:
                for member in tf.getmembers():
                    target = _safe_target(stage, member.name)
                    _safe_target(dest_root, member.name)
                    relative = target.relative_to(stage)
                    if relative.parts and relative.parts[0].casefold() == ".release-restore":
                        raise RuntimeError("Archive conflicts with restore journal")
                    if not (member.isfile() or member.isdir()):
                        raise RuntimeError(f"Archive links/special files rejected: {member.name}")
                    if member.isfile():
                        rel = relative.as_posix()
                        if rel.casefold() in seen_keys:
                            raise RuntimeError(f"Duplicate archive destination: {rel}")
                        seen_keys.add(rel.casefold())
                        seen.add(rel)
                tf.extractall(path=stage, filter="data")
        for rel in seen:
            target = _safe_target(dest_root, rel)
            if target.exists() and not target.is_file():
                raise RuntimeError(f"Destination is not a regular file: {rel}")
            _check_db_sidecars(target)
        journal["state"] = "committing"
        _write_journal(log, journal)
        for rel in sorted(seen):
            target = _safe_target(dest_root, rel)
            existed = target.exists()
            if existed:
                backup = _safe_target(work / "backup", rel)
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup)
            journal["entries"].append({"path": rel, "existed": existed})
            _write_journal(log, journal)  # Write intent before replacing, including new files.
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(stage / rel, target)
        journal["state"] = "complete"
        _write_journal(log, journal)
    except BaseException:
        _recover_restore(dest_root)
        raise
    else:
        shutil.rmtree(work)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Package, publish, or restore ASR transcript release artifacts."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_pack_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
        p.add_argument("--groups", nargs="+", help="Source groups to include (default: all in DB)")
        p.add_argument("--no-json", action="store_true", help="Exclude word-level .json files")
        p.add_argument("--no-db", action="store_true", help="Exclude pipeline.db snapshot")
        p.add_argument(
            "--no-sanitize-db",
            action="store_true",
            help="Compatibility alias for --db-profile private",
        )
        p.add_argument("--db-profile", choices=("public", "private"), default=None)
        p.add_argument("--level", type=int, default=6, choices=range(1, 10))
        p.add_argument(
            "--max-part-raw-mb",
            type=int,
            default=DEFAULT_MAX_PART_RAW_MB,
            help=f"Split large buckets into standalone archives <= N MB raw (default: {DEFAULT_MAX_PART_RAW_MB}, 0=disable)",
        )

    p_pack = sub.add_parser("package", help="Package transcript artifacts locally")
    add_pack_args(p_pack)

    p_pub = sub.add_parser("publish", help="Package (unless --skip-package) and publish via gh")
    add_pack_args(p_pub)
    p_pub.add_argument("--tag", required=True, help="Release tag, e.g. v2026.11")
    p_pub.add_argument("--title", help="Release title")
    p_pub.add_argument("--draft", action="store_true", help="Create as draft release")
    p_pub.add_argument(
        "--skip-package",
        action="store_true",
        help="Upload existing archives from --out-dir without re-packing",
    )
    p_pub.add_argument("--clean", action="store_true", help="Delete --out-dir after upload")

    p_res = sub.add_parser("restore", help="Verify and extract downloaded release archives")
    p_res.add_argument("src_dir", type=Path, help="Directory containing downloaded .tar.gz files")
    p_res.add_argument("--dest", type=Path, default=REPO_ROOT, help="Destination repo root")
    p_res.add_argument("--skip-verify", action="store_true")

    args = parser.parse_args()
    if args.cmd in ("package", "publish"):
        requested_profile = args.db_profile or ("private" if args.no_sanitize_db else None)
        try:
            args.db_profile = _db_profile(False if args.no_sanitize_db else None, args.db_profile)
        except ValueError as exc:
            parser.error(str(exc))

    if args.cmd == "package":
        groups = discover_groups(args.groups)
        if not groups:
            print("No transcript groups found.", file=sys.stderr)
            return 1
        package_artifacts(
            out_dir=args.out_dir,
            groups=groups,
            include_json=not args.no_json,
            include_db=not args.no_db,
            db_profile=args.db_profile,
            compresslevel=args.level,
            max_part_raw_mb=args.max_part_raw_mb,
        )
        return 0

    if args.cmd == "publish":
        if args.skip_package:
            manifest_path = args.out_dir / "MANIFEST.json"
            if not manifest_path.is_file():
                raise FileNotFoundError(f"Missing {manifest_path}; run without --skip-package.")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if requested_profile and manifest.get("db_profile") != requested_profile:
                parser.error("Existing batch db_profile differs; repackage with the requested profile")
        else:
            groups = discover_groups(args.groups)
            if not groups:
                print("No transcript groups found.", file=sys.stderr)
                return 1
            manifest = package_artifacts(
                out_dir=args.out_dir,
                groups=groups,
                include_json=not args.no_json,
                include_db=not args.no_db,
                db_profile=args.db_profile,
                compresslevel=args.level,
                max_part_raw_mb=args.max_part_raw_mb,
            )
        publish_release(
            out_dir=args.out_dir,
            tag=args.tag,
            title=args.title,
            manifest=manifest,
            draft=args.draft,
        )
        if args.clean:
            shutil.rmtree(args.out_dir, ignore_errors=True)
            print(f"Cleaned temporary directory: {args.out_dir}")
        return 0

    if args.cmd == "restore":
        restore_artifacts(args.src_dir, args.dest, skip_verify=args.skip_verify)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
