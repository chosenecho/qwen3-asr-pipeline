# qwen3-asr-pipeline

面向 [Qwen3-ASR](https://github.com/QwenLM/Qwen3-ASR) HTTP 服务的批量视频转写工具链：提取音轨、并发调用 ASR 生成 `.txt` / `.srt`（可选词级 `.json`），并用 SQLite 记录每条任务的实时进度。附带 Pi Coding Agent 扩展和 GitHub Releases 产物分卷打包脚本。

## 为什么用这套脚本

- **零 Python 第三方依赖**：`asr_pipeline.py` 和 `scripts/release_artifacts.py` 只用 Python 3.10+ 标准库和 `ffmpeg` / `ffprobe`，在 Windows 和 Linux 服务器上直接跑，不用配虚拟环境。
- **SQLite WAL 断点续跑**：中断后重跑自动跳过已完成项；长音频异步任务会持久化 `asr_task_id`，重启后直接接着轮询同一个远端任务，不会重复上传。
- **边提边转 + 预上传**：`extract` 和 `transcribe --watch` 可同时跑；`--prefetch` 会在当前推理结束前预先提交下一条异步任务，减少网络等待空窗。
- **多端点与 Modal 双层鉴权**：同一份 `.env` 管理本地服务与 Modal 端点（`Modal-Key` / `Modal-Secret` + `Authorization: Bearer`），命令行加 `--endpoint modal` 即可切换。

## 环境要求与快速开始

依赖：**Python 3.10+**、`ffmpeg` / `ffprobe`（需在 `PATH` 中）。按需可选：`gh` CLI（发布 GitHub Releases）、[Pi Coding Agent](https://github.com/badlogic/pi-mono)（使用 `.pi/extensions/qwen-asr.ts`）。

```bash
git clone https://github.com/chosenecho/qwen3-asr-pipeline.git
cd qwen3-asr-pipeline
cp .env.example .env
# 编辑 .env，填入 ASR_BASE_URL（或 ASR_MODAL_*）

# 1. 检查端点配置
python asr_pipeline.py endpoints

# 2. 提取音频（默认 16kHz 单声道 48kbps MP3，写入 data/audio/<group>/）
python asr_pipeline.py extract ./videos/my_channel --workers 8

# 3. 批量转写（输出 .txt + .srt 到 data/transcripts/<group>/）
python asr_pipeline.py transcribe --group my_channel --concurrency 4

# 4. 查看进度
python asr_pipeline.py status
```

`status` 输出示例：

```text
SQLite DB: /path/to/qwen3-asr-pipeline/data/pipeline.db

Group: my_channel (total=105)
  Audio : done=105/105 (100.0%) | pending=0 running=0 failed=0
  ASR   : done=105/105 (100.0%) | in_progress=0 pending=0 failed=0 (avg_progress=100.0%)
  Volume: video=54.90 GB -> audio=3.10 GB | total_duration=154h17m23s
```

## 核心用法（`asr_pipeline.py`）

默认数据全部落在 `data/`（已被 `.gitignore` 忽略）：状态库 `data/pipeline.db`（WAL 模式）、音频 `data/audio/<group>/`、转写结果 `data/transcripts/<group>/`。

```bash
# 提取音频：--preset 支持 asr-mp3（默认）/ copy（无损 .m4a）/ opus / wav
python asr_pipeline.py extract ./videos/my_channel --preset asr-mp3 --workers 8

# 走 Modal 端点转写，同时保存原始词级 JSON，并监听 extract 新产出的音频
python asr_pipeline.py transcribe --endpoint modal --group my_channel --concurrency 4 --save-json --watch

# 校验端点 /health 连通性（注意：对 Modal 端点执行会唤醒远端容器）
python asr_pipeline.py endpoints --endpoint modal --check

# 查看指定状态条目，或重置失败/卡住的任务
python asr_pipeline.py list --group my_channel --asr-status failed --limit 20
python asr_pipeline.py reset --stage asr --status failed,running --group my_channel
```

转写路由规则：`--mode auto`（默认）将时长 `<= 150s`（或无法探测时长且体积 `<= 3 MiB`）的音频发往同步接口 `POST /v1/asr/transcribe`，更长音频发往异步接口 `POST /v1/asr/tasks`，完成后自动 `DELETE` 远端任务（传 `--keep-remote-task` 可保留）。完整参数见 `python asr_pipeline.py <子命令> --help`。

## 端点配置（`.env`）

`asr_pipeline.py` 和 Pi 扩展共用项目根目录的 `.env`（参考 [`.env.example`](.env.example)）：

| 变量 | 说明 |
| --- | --- |
| `ASR_ENDPOINT` | 默认端点名，未设时为 `local` |
| `ASR_BASE_URL` / `ASR_API_KEY` | `local` 端点地址与 Bearer Key |
| `ASR_MODAL_BASE_URL` / `ASR_MODAL_API_KEY` | `modal` 端点地址与应用层 Bearer Key |
| `ASR_MODAL_KEY` / `ASR_MODAL_SECRET` | Modal 网关鉴权头（`wk-...` / `ws-...`） |
| `ASR_MODAL_INCLUDE_WORDS` | 是否默认请求词级时间戳（`true` / `false`） |

支持自定义任意命名端点：按 `ASR_<NAME>_BASE_URL`、`ASR_<NAME>_API_KEY`、`ASR_<NAME>_HEADERS`、`ASR_<NAME>_INCLUDE_WORDS` 配置后，用 `--endpoint <name>` 调用；也可用 `--url` / `--api-key` / `--modal-key` / `--modal-secret` / `--header "K: V"` 在命令行临时覆盖。

## Pi 扩展（`.pi/extensions/qwen-asr.ts`）

给 Pi Coding Agent 用的单文件扩展，不依赖 SQLite，适合对任意本地文件或目录直接跑转写（默认并发 3，结果默认输出在音频同目录，已有 `.txt` + `.srt` 自动跳过）。

- **当前目录启用**：在仓库根目录运行 `pi` 自动加载。
- **全局启用**：软链或复制到 `~/.pi/agent/extensions/qwen-asr.ts`。

支持两种调用方式：

```text
# 1. 斜杠命令（不消耗 LLM token）
/asr ./data/audio/my_channel --endpoint modal --out ./outputs --concurrency 4 --json

# 2. 自然语言（触发 asr_batch 工具）
把 ./data/audio/my_channel 下的音频批量转成 txt 和 srt，输出到 ./data/transcripts/my_channel，并发 4。
```

`/asr` 参数：`--endpoint`、`--out`、`--url`、`--api-key`、`--modal-key`、`--modal-secret`、`--header`、`--include-words` / `--no-include-words`、`--mode auto|sync|async`、`--concurrency`、`--lang`、`--json`、`--overwrite`、`--no-recursive`。未配置 `.env` 时回退地址为 `http://127.0.0.1:8000`。

## 产物发布与恢复（`scripts/release_artifacts.py`）

源码仓库不提交大体积数据，转写文本与数据库快照通过 [GitHub Releases](https://github.com/chosenecho/qwen3-asr-pipeline/releases) 分发（原始音频留在本地）：

- `<group>-text.tar.gz`：`.txt` 和 `.srt`
- `<group>-json.tar.gz`：词级 `.json`（原始体积超过 450 MB 时自动按 `--max-part-raw-mb` 拆成独立的 `<group>-json-partXX.tar.gz`）
- `pipeline-db.tar.gz`：经 SQLite `backup` + `VACUUM` 导出的快照，默认抹除服务端 URL 与运行参数（传 `--no-sanitize-db` 保留）
- `MANIFEST.json`、`SHA256SUMS.txt`：文件清单与 SHA-256 校验和

```bash
# 本地打包到 release_dist/
python scripts/release_artifacts.py package

# 打包并顺序上传到 GitHub Release（按 SHA-256 断点续传与重试）
python scripts/release_artifacts.py publish --tag v2026.11 --title "Transcripts Release v2026.11"

# 从 Release 下载压缩包与 SHA256SUMS.txt 到 release_dist/ 后校验并恢复到 data/
python scripts/release_artifacts.py restore release_dist
```

## 项目状态与开发

- **维护状态**：个人自用工具，随实际转写与部署需求维护。API 契约绑定 Qwen3-ASR HTTP 服务（`/health`、`/v1/asr/transcribe`、`/v1/asr/tasks`）。
- **代码约定与 Agent 指南**：见 [`AGENTS.md`](AGENTS.md)。
- **许可证**：仓库尚未添加 `LICENSE` 文件。
