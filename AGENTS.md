# AGENTS.md

Coding-agent reference for `qwen3-asr-pipeline`. Read `README.md` for user-facing usage and run `--help` on any script for full flag lists; this file records only repository constraints, invariants, and gotchas.

## Repository Map & Runtime Constraints

- `asr_pipeline.py`: Single-file CLI for `ffmpeg` audio extraction, SQLite WAL state tracking, and Qwen3-ASR sync/async transcription.
- `.pi/extensions/qwen-asr.ts`: Single-file Pi Coding Agent extension exposing `/asr` and the `asr_batch` tool (standalone; does not touch SQLite).
- `scripts/release_artifacts.py`: Packages, publishes (via `gh`), and restores transcript archives + sanitized `pipeline.db` snapshots for GitHub Releases.

Constraints:
- Keep `asr_pipeline.py` and `scripts/release_artifacts.py` **Python 3.10+ stdlib-only** (no `pip` dependencies). They must run on both Windows (Python 3.12) and Ubuntu 22.04 ECS (Python 3.10.12).
- `scripts/release_artifacts.py` uses `tarfile.extractall(..., filter="data")` (PEP 706, available on Ubuntu 22.04's patched 3.10.12 and Python 3.12+). Preserve both the explicit path-traversal guard and `filter="data"`.

## Architecture & Invariants

### SQLite state (`asr_pipeline.py`)
- DB lives at `data/pipeline.db` in WAL mode (`media_items`, `pipeline_runs`). `StateDB` opens with `check_same_thread=False` and serializes all queries behind `self._lock`; every new DB method must acquire `self._lock`.
- `media_items.video_path` is `UNIQUE` and stores resolved absolute OS paths. A `pipeline.db` created on Windows is **not** directly runnable on Linux without remapping paths.
- Schema v1 adds `source_fingerprint`, `audio_fingerprint`, `asr_fingerprint`, `asr_phase`. Migration backs up the old DB under `.migration-backups/`; `BEGIN IMMEDIATE` and a lock-time column recheck serialize concurrent `extract`/`transcribe` startup.
- Input/preset changes invalidate downstream state. Fingerprints use size/mtime plus parameters, not complete input hashes; legacy provenance remains explicitly unverified.
- `snapshot_metadata.profile=public` identifies an archive index: allow `status`/`list`, reject execution/reset/state repair. Public snapshots skip migration.
- `extract` writes to a hidden sibling temp file before `os.replace` so partial extractions never look complete. Preflight all output paths for collisions.
- `TranscriptStore` stages outputs, invalidates the previous marker, replaces files, then commits `.<stem>.complete.json` with SHA-256 records. Mark DB done before deleting remote tasks. `reconcile` is local-only and dry-run by default; `--apply` repairs state.

### ASR HTTP contract & endpoint resolution
- Expected Qwen3-ASR HTTP routes:
  - `GET /health`
  - `POST /v1/asr/transcribe` (multipart form `file`, `return_timestamps=true`, optional `include_words=true`, `language`)
  - `POST /v1/asr/tasks` -> `GET /v1/asr/tasks/{task_id}` -> `DELETE /v1/asr/tasks/{task_id}` (auto-deleted on completion unless `--keep-remote-task`).
- Auto mode threshold: duration `<= 150s` (or file size `<= 3 MiB` when `ffprobe` cannot read duration) uses sync; longer uses async. Async resume requires a matching normalized URL and request fingerprint with `asr_status IN ('queued', 'running')`. A known active async task resumes even if sync routing is selected; explicit overwrite creates a replacement.
- `ensure_remote_task` is shared by ordinary/prefetch paths. Clear old ID before submission; persist `submitting` before POST, then ID and `polling`. Ambiguous submission becomes `submission_unknown` and never auto-resubmits. Poll/transport/local-write failures retain known IDs; only explicit remote failure clears resumability. Active-task option changes require explicit overwrite.
- `tests/fixtures/asr_cases.json` defines the shared result contract. Reject invalid structure/timestamps, accept explicit silence, use a 5-second SRT fallback for text-only results without another inference POST.
- Endpoint discovery scans `ASR_<NAME>_BASE_URL` in `.env` / environment (`ASR_ENDPOINT` sets default; `local` falls back to `ASR_BASE_URL`). When endpoint is `modal` or host ends with `.modal.run`, `_validate_endpoint_auth` requires `Modal-Key` and `Modal-Secret` headers and warns if `Authorization: Bearer` (`ASR_MODAL_API_KEY`) is empty.
- Intentional difference between Python CLI and Pi extension:
  - `asr_pipeline.py` loads `<repo>/.env` only and fails fast if `base_url` is empty.
  - `.pi/extensions/qwen-asr.ts` merges `<repo>/.env` -> `<cwd>/.env` -> `process.env` (later wins) and falls back to `http://127.0.0.1:8000` for `local`.

### Pi extension (`.pi/extensions/qwen-asr.ts`)
- Keep the dual `execute` argument adapter (`(id, params, signal, onUpdate, ctx)` vs `(id, params, onUpdate, ctx, signal)`) so the extension works across Pi versions.
- When changing endpoint env-var names or HTTP request headers in `asr_pipeline.py`, mirror the change in `.pi/extensions/qwen-asr.ts`.

### Release packaging (`scripts/release_artifacts.py`)
- `export_clean_db` uses SQLite online `backup()`, then defaults to a public allowlist export with relative paths and `snapshot_metadata.profile=public`; clear task IDs, endpoints, errors/messages and run configuration. `--db-profile private` (legacy `--no-sanitize-db`) preserves full state for private backup only. Finish with `journal_mode=DELETE` and `VACUUM`.
- Restore requires MANIFEST + SHA table, validates the complete declared set before writing targets, ignores extra archives, and preserves explicit path checks plus data filter. `--skip-verify` skips digests only. Use staging plus `.release-restore` journal/backups; rollback incomplete operations before another restore. Pause target writers; reject SQLite sidecars.
- Packaging preserves the last complete batch in the `.previous` sibling directory. Manifest v2 records release ID and DB profile. New releases stay draft until remote asset validation succeeds; update SHA table and MANIFEST after archive uploads.
- Buckets larger than `--max-part-raw-mb` (default `450` MB raw, ~80–90 MB gzipped) split into independent `-partXX.tar.gz` archives so each part extracts standalone and avoids GitHub upload TCP resets (hard asset ceiling: 2 GiB).
- `publish` uploads assets smallest-first via `gh release upload --clobber`, skipping assets whose remote `sha256:` digest matches local.

## Verification Commands

Run after any edit to Python scripts:

```bash
python -m py_compile asr_pipeline.py scripts/release_artifacts.py
python asr_pipeline.py endpoints
python asr_pipeline.py status
```

Use `--db <temporary-path>` for status/smoke checks to avoid the user's live DB. Run `python -m unittest discover -s tests -v` for state/output/recovery/release regressions and `node --test tests/test_pi_contract.mjs` (Node 24+) for the offline Pi contract. Tests use stdlib, temporary files and mocked network; Pi uses built-in TypeScript stripping, not a full `tsc` check.

## Secrets, Billing & Deployment Guardrails

- Keep `.env`, `data/`, `release_dist/`, and `*.db*` untracked; use placeholders in docs and commit messages instead of real keys, Modal tokens, private URLs, or personal local paths.
- Skip `asr_pipeline.py endpoints --endpoint modal --check` during routine checks unless explicitly verifying live Modal health, because HTTP requests to `.modal.run` wake the remote GPU container and incur billing.
- When deploying to Linux servers without direct GitHub access, transfer a `git bundle` with `core.fileMode false`, set `.env` to mode `0600`, and symlink `asr_pipeline.py` onto `PATH` (and `.pi/extensions/qwen-asr.ts` to `~/.pi/agent/extensions/qwen-asr.ts` if using Pi).
