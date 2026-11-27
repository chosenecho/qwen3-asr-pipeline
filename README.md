# ASR Pipeline & Pi Extension

本仓库只保留源码与工具脚本，转录产物与数据库快照统一通过 **GitHub Releases** 分包发布：
1. **[`asr_pipeline.py`](asr_pipeline.py)**：基于 Python 标准库 + `ffmpeg` + SQLite 的视频转音频与转录进度管理程序（音频/字幕落盘在文件系统，提取与转录状态持久化在 SQLite）。
2. **[`.pi/extensions/qwen-asr.ts`](.pi/extensions/qwen-asr.ts)**：单文件 Pi 扩展，支持通过 `/asr` 命令或 `asr_batch` 工具直接批量调用 Qwen3-ASR 服务。
3. **[`scripts/release_artifacts.py`](scripts/release_artifacts.py)**：一键按分组打包转录产物、生成 SHA-256 校验和、发布或恢复 GitHub Release 产物。

首次使用请先复制模板并填写端点配置：

```bash
cp .env.example .env
```

---

## 一、视频转音频 & SQLite 状态管理（`asr_pipeline.py`）

默认存储位置（已在 `.gitignore` 中忽略）：
- SQLite 状态库：`data/pipeline.db`（WAL 模式，包含 `media_items` 与 `pipeline_runs` 两张表）
- 音频文件目录：`data/audio/<source_group>/...`
- 转录输出目录：`data/transcripts/<source_group>/...`

### 1. 批量视频转音频（断点续跑）

```bash
# 默认转成 16kHz 单声道 48kbps MP3（体积小、适合 ASR 与上传）
python asr_pipeline.py extract "D:\bili_yt_dl\bilibili\bili_23191782" --workers 10

# 如需无损拷贝原始音轨为 .m4a（--preset 可选 asr-mp3 / copy / opus / wav）
python asr_pipeline.py extract "D:\bili_yt_dl\bilibili\bili_23191782" --preset copy --workers 8
```

### 2. 查看音频提取与转录进度

```bash
python asr_pipeline.py status
python asr_pipeline.py status --group bili_23191782 --json
python asr_pipeline.py list --group bili_23191782 --asr-status pending --limit 20
```

### 3. 批量调用 Qwen3-ASR 转录（自动保存每条进度到 SQLite）

```bash
# 默认（local）端点
python asr_pipeline.py transcribe --group bili_23191782 --concurrency 8

# 使用 Modal 端点（读取 .env 中的 ASR_MODAL_* 配置，携带 Modal-Key / Modal-Secret + Bearer 双层鉴权）
python asr_pipeline.py transcribe --endpoint modal --group bili_23191782 --concurrency 4

# 查看所有已配置端点及脱敏鉴权状态，或校验 /health 连通性
python asr_pipeline.py endpoints
python asr_pipeline.py endpoints --endpoint modal --check
```

- 在 `.env` 中支持多端点配置：
  - 默认 `local`：`ASR_BASE_URL`、`ASR_API_KEY`
  - 新增 `modal`：`ASR_MODAL_BASE_URL`、`ASR_MODAL_API_KEY`、`ASR_MODAL_KEY`（`wk-...`）、`ASR_MODAL_SECRET`（`ws-...`）、`ASR_MODAL_INCLUDE_WORDS=true`
  - 可在 `.env` 中设置 `ASR_ENDPOINT=modal` 切换默认端点，或通过 `--endpoint` / `--modal-key` / `--modal-secret` / `--header "Key: Value"` / `--include-words` 临时覆盖。
- 短音频（`<= 150s`）自动走 `POST /v1/asr/transcribe`；长音频（`> 150s`）自动走 `POST /v1/asr/tasks` 并实时写入 `asr_task_id`、`asr_status`、`asr_progress`、`asr_message` 到 SQLite，支持断点续轮询。
- 输出同名 `.txt` 和 `.srt`（加 `--save-json` 可同时保存原始 JSON）。

### 4. 重置失败/卡住的任务

```bash
python asr_pipeline.py reset --stage audio --status failed,running --group bili_23191782
python asr_pipeline.py reset --stage asr --status failed,running --group bili_23191782
```

---

## 二、Qwen3-ASR Pi Extension（`.pi/extensions/qwen-asr.ts`）

### 启用方式

- **当前目录使用**：直接在本目录运行 `pi`，Pi 会自动加载 `.pi/extensions/qwen-asr.ts`。
- **全局使用**：将该文件复制到 `~/.pi/agent/extensions/qwen-asr.ts` 即可在任意目录使用。

### 环境变量配置（自动读取项目 `.env`）

```bash
# 默认端点（local）
export ASR_BASE_URL="http://127.0.0.1:6006"   # 你的 ASR 服务地址
export ASR_API_KEY=""                         # 如有鉴权再填，无则留空
export ASR_CONCURRENCY="3"                    # 并发数（默认 3）

# Modal 端点（--endpoint modal）
export ASR_MODAL_BASE_URL="https://<your-workspace>--qwen3-asr-server-serve.us-east.modal.run"
export ASR_MODAL_API_KEY=""                   # 应用层 Bearer API Key
export ASR_MODAL_KEY=""                       # Modal-Key (wk-...)
export ASR_MODAL_SECRET=""                    # Modal-Secret (ws-...)
export ASR_MODAL_INCLUDE_WORDS="true"
```

### 使用方式

#### 1. 斜杠命令（不消耗 LLM token）

```text
/asr ./data/audio/bili_23191782
/asr ./audios --endpoint modal --out ./outputs --concurrency 4 --json
/asr ./long_meeting.mp3 --mode async --url http://10.0.0.5:8000
```

常用参数：
- `--endpoint local|modal`：选择 `.env` 中配置的命名端点（默认 `ASR_ENDPOINT` 或 `local`）
- `--out <目录>`：指定输出目录（默认与音频同目录）
- `--url <地址>`：覆盖端点地址
- `--api-key <KEY>` / `--modal-key <wk-...>` / `--modal-secret <ws-...>` / `--header "K: V"`：临时覆盖鉴权与自定义 Header
- `--include-words` / `--no-include-words`：是否请求词级时间戳（`include_words=true`）
- `--mode auto|sync|async`：默认 `auto`（短音频走同步 `/v1/asr/transcribe`，长音频自动走异步 `/v1/asr/tasks`）
- `--concurrency <N>`：并发文件数（默认 `3`）
- `--json`：同时保存原始时间戳 JSON
- `--overwrite`：覆盖已存在的 `.txt` / `.srt`（默认断点跳过）
- `--no-recursive`：不递归子目录

#### 2. 自然语言（通过 `asr_batch` 工具）

直接在 Pi 里说：
> 把 `./data/audio/bili_23191782` 目录下的音频批量转成 txt 和 srt，输出到 `./data/transcripts/bili_23191782`，并发 4。

---

## 三、转录产物与 GitHub Releases（`scripts/release_artifacts.py`）

为保持 Git 仓库轻量清晰，所有二进制与大体积产物均不提交到 Git，而是按分组打包并挂载到 [GitHub Releases](../../releases)：

- `<group>-text.tar.gz`：该分组的 `.txt` 纯文本与 `.srt` 字幕
- `<group>-json.tar.gz`：该分组的词级时间戳原始 `.json`（如该分组启用了 `--save-json`）
- `pipeline-db.tar.gz`：经过 WAL checkpoint 的干净 `data/pipeline.db` 快照
- `MANIFEST.json` 与 `SHA256SUMS.txt`：各分组文件计数、体积清单与 SHA-256 校验和

> 注：原始音频（`data/audio/`）体积较大，仅保留在本地或外部存储，不上传至 GitHub。

### 打包与发布新版本

```bash
# 1. 仅本地打包到 release_dist/（默认自动读取 pipeline.db 中的分组）
python scripts/release_artifacts.py package

# 2. 打包并直接创建/更新 GitHub Release
python scripts/release_artifacts.py publish --tag v2026.11 --title "Transcripts Release v2026.11"
```

### 下载并校验恢复到本地 `data/`

```bash
# 从 Release 下载所需压缩包与 SHA256SUMS.txt 到 release_dist/ 后执行：
python scripts/release_artifacts.py restore release_dist
```

