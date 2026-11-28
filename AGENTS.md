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
- `extract` writes to a hidden sibling temp file before `os.replace` so partial extractions never look complete.

### ASR HTTP contract & endpoint resolution
- Expected Qwen3-ASR HTTP routes:
  - `GET /health`
  - `POST /v1/asr/transcribe` (multipart form `file`, `return_timestamps=true`, optional `include_words=true`, `language`)
  - `POST /v1/asr/tasks` -> `GET /v1/asr/tasks/{task_id}` -> `DELETE /v1/asr/tasks/{task_id}` (auto-deleted on completion unless `--keep-remote-task`).
- Auto mode threshold: duration `<= 150s` (or file size `<= 3 MiB` when `ffprobe` cannot read duration) uses sync; longer uses async. Async resume reuses `asr_task_id` only when `asr_server_url` matches the current endpoint and `asr_status IN ('queued', 'running')`.
- Endpoint discovery scans `ASR_<NAME>_BASE_URL` in `.env` / environment (`ASR_ENDPOINT` sets default; `local` falls back to `ASR_BASE_URL`). When endpoint is `modal` or host ends with `.modal.run`, `_validate_endpoint_auth` requires `Modal-Key` and `Modal-Secret` headers and warns if `Authorization: Bearer` (`ASR_MODAL_API_KEY`) is empty.
- Intentional difference between Python CLI and Pi extension:
  - `asr_pipeline.py` loads `<repo>/.env` only and fails fast if `base_url` is empty.
  - `.pi/extensions/qwen-asr.ts` merges `<repo>/.env` -> `<cwd>/.env` -> `process.env` (later wins) and falls back to `http://127.0.0.1:8000` for `local`.

### Pi extension (`.pi/extensions/qwen-asr.ts`)
- Keep the dual `execute` argument adapter (`(id, params, signal, onUpdate, ctx)` vs `(id, params, onUpdate, ctx, signal)`) so the extension works across Pi versions.
- When changing endpoint env-var names or HTTP request headers in `asr_pipeline.py`, mirror the change in `.pi/extensions/qwen-asr.ts`.

### Release packaging (`scripts/release_artifacts.py`)
- `export_clean_db` uses SQLite's online `backup()` API, strips `media_items.asr_server_url` and `pipeline_runs.{config_json,params_json}` by default, switches `journal_mode=DELETE`, and runs `VACUUM`. Never skip sanitization for public releases.
- Buckets larger than `--max-part-raw-mb` (default `450` MB raw, ~80–90 MB gzipped) split into independent `-partXX.tar.gz` archives so each part extracts standalone and avoids GitHub upload TCP resets (hard asset ceiling: 2 GiB).
- `publish` uploads assets smallest-first via `gh release upload --clobber`, skipping assets whose remote `sha256:` digest matches local.

## Verification Commands

Run after any edit to Python scripts:

```bash
python -m py_compile asr_pipeline.py scripts/release_artifacts.py
python asr_pipeline.py endpoints
python asr_pipeline.py status
```

There is no automated test suite yet; for behavioral changes, test against a throwaway SQLite DB (`--db /tmp/test.db`) and a sample audio file.

## Secrets, Billing & Deployment Guardrails

- Keep `.env`, `data/`, `release_dist/`, and `*.db*` untracked; use placeholders in docs and commit messages instead of real keys, Modal tokens, private URLs, or personal local paths.
- Skip `asr_pipeline.py endpoints --endpoint modal --check` during routine checks unless explicitly verifying live Modal health, because HTTP requests to `.modal.run` wake the remote GPU container and incur billing.
- When deploying to Linux servers without direct GitHub access, transfer a `git bundle` with `core.fileMode false`, set `.env` to mode `0600`, and symlink `asr_pipeline.py` onto `PATH` (and `.pi/extensions/qwen-asr.ts` to `~/.pi/agent/extensions/qwen-asr.ts` if using Pi).
