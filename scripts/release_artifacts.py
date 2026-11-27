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
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
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


def export_clean_db(dest_db: Path, sanitize: bool = True) -> None:
    """Create a consistent WAL-merged snapshot of pipeline.db via SQLite backup API."""
    src_conn = sqlite3.connect(str(DB_PATH))
    dst_conn = sqlite3.connect(str(dest_db))
    try:
        src_conn.backup(dst_conn)
        dst_conn.commit()
        if sanitize:
            cols = {
                r[1]
                for r in dst_conn.execute("PRAGMA table_info(media_items)").fetchall()
            }
            if "asr_server_url" in cols:
                dst_conn.execute(
                    "UPDATE media_items SET asr_server_url = NULL WHERE asr_server_url IS NOT NULL"
                )
            tables = {
                r[0]
                for r in dst_conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "pipeline_runs" in tables:
                run_cols = {
                    r[1]
                    for r in dst_conn.execute("PRAGMA table_info(pipeline_runs)").fetchall()
                }
                for col in ("config_json", "params_json"):
                    if col in run_cols:
                        dst_conn.execute(
                            f"UPDATE pipeline_runs SET {col} = NULL WHERE {col} IS NOT NULL"
                        )
            dst_conn.commit()
        dst_conn.execute("PRAGMA journal_mode=DELETE")
        dst_conn.execute("VACUUM")
        dst_conn.commit()
    finally:
        dst_conn.close()
        src_conn.close()


def collect_group_files(group: str, exts: set[str]) -> list[tuple[Path, str]]:
    group_dir = TRANSCRIPTS_DIR / group
    if not group_dir.is_dir():
        return []
    result: list[tuple[Path, str]] = []
    for p in sorted(group_dir.rglob("*")):
        if p.is_file() and p.suffix.lower() in exts:
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


def package_artifacts(
    out_dir: Path,
    groups: list[str],
    include_json: bool = True,
    include_db: bool = True,
    sanitize_db: bool = True,
    compresslevel: int = 6,
    max_part_raw_mb: int = DEFAULT_MAX_PART_RAW_MB,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in list(out_dir.glob("*.tar.gz")) + [
        out_dir / "MANIFEST.json",
        out_dir / "SHA256SUMS.txt",
        out_dir / "RELEASE_NOTES.md",
    ]:
        stale.unlink(missing_ok=True)

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
            export_clean_db(clean_db, sanitize=sanitize_db)
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


def render_release_notes(tag: str, manifest: dict) -> str:
    lines = [
        f"## ASR Transcripts & Metadata Snapshot (`{tag}`)",
        "",
        "All transcript artifacts are packaged by source group with UTF-8 PAX `.tar.gz` archives.",
        "Extract at the repository root (or run `python scripts/release_artifacts.py restore <dir>`) to restore `data/transcripts/...` and `data/pipeline.db`.",
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
        ["gh", "release", "view", tag, "--json", "assets"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    existing_assets: dict[str, dict] = {}
    if check.returncode == 0:
        data = json.loads(check.stdout or "{}")
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
        if draft:
            cmd.append("--draft")
        subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)

    # 2. Upload assets sequentially (smallest first) with digest-verified resume + retry
    ordered_files = sorted(upload_files, key=lambda p: p.stat().st_size)
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


def restore_artifacts(src_dir: Path, dest_root: Path, skip_verify: bool = False) -> None:
    sums_path = src_dir / "SHA256SUMS.txt"
    expected: dict[str, str] = {}
    if sums_path.is_file():
        for line in sums_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            digest, name = line.split(None, 1)
            expected[name.strip()] = digest.strip().lower()

    archives = sorted(src_dir.glob("*.tar.gz"))
    if not archives:
        raise FileNotFoundError(f"No .tar.gz archives found in {src_dir}")

    for arc in archives:
        if not skip_verify and arc.name in expected:
            actual = sha256_file(arc)
            if actual != expected[arc.name]:
                raise RuntimeError(
                    f"SHA-256 mismatch for {arc.name}: expected {expected[arc.name]}, got {actual}"
                )
            print(f"  [VERIFIED] {arc.name}")

        with tarfile.open(arc, "r:gz") as tf:
            for member in tf.getmembers():
                target = (dest_root / member.name).resolve()
                if not str(target).startswith(str(dest_root.resolve())):
                    raise RuntimeError(f"Unsafe path in archive {arc.name}: {member.name}")
            tf.extractall(path=dest_root, filter="data")
        print(f"  [EXTRACTED] {arc.name} -> {dest_root}")


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
            help="Keep server URLs / run params inside exported pipeline.db",
        )
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
            sanitize_db=not args.no_sanitize_db,
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
                sanitize_db=not args.no_sanitize_db,
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
