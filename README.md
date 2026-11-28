# qwen3-asr-pipeline

面向 [Qwen3-ASR](https://github.com/QwenLM/Qwen3-ASR) HTTP 服务的批量视频转写工具链：提取音轨、并发调用 ASR 生成 `.txt` / `.srt`（可选词级 `.json`），并用 SQLite 记录每条任务的实时进度。附带 Pi Coding Agent 扩展和 GitHub Releases 产物分卷打包脚本。

## 为什么用这套脚本

- **零 Python 第三方依赖**：`asr_pipeline.py` 和 `scripts/release_artifacts.py` 只用 Python 3.10+ 标准库和 `ffmpeg` / `ffprobe`，在 Windows 和 Linux 服务器上直接跑，不用配虚拟环境。
- **SQLite WAL 断点续跑**：任务 ID、请求指纹与执行阶段持久化；轮询临时失败后重跑继续原任务。提交响应丢失时标记为“结果未知”，待核实后显式重建，避免自动重复上传。
- **结果有效性检查**：输入与参数指纹检测过期状态，转写产物以摘要和完成标记确认整套写入；多点文件名保留完整，同名目标在发起请求前报告冲突。
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

### 状态核对、迁移与失败恢复

```bash
# 只核对本地状态和文件，不发网络请求
python asr_pipeline.py reconcile --group my_channel

# 应用缺失/过期产物的状态修复
python asr_pipeline.py reconcile --group my_channel --apply

# 自定义转写输出目录时，也用该目录查找尚未记入数据库的完成标记
python asr_pipeline.py reconcile --output-dir ./outputs --apply

# 轮询临时失败可直接重跑，仍使用原 task ID
python asr_pipeline.py transcribe --group my_channel --poll-retries 3
```

首次打开旧数据库会先在同级 `.migration-backups/` 生成私有备份，再迁移到 schema v1。新字段记录视频、音频与请求指纹及 ASR 阶段；指纹使用大小、修改时间和处理参数，不扫描整个视频内容。内容变化但大小和修改时间都保持的情况，需要显式重建。

重新扫描发现视频变化或切换提取 preset 时，下游状态失效。转写参数改变会使已完成的新格式结果失效；仍在运行的任务则保留原身份，替换前使用显式 `--overwrite`。轮询暂时超时、429/5xx、等待时间耗尽及本地写入失败都会保留已知任务，重跑继续取结果。上传响应丢失或提交过程中退出会进入 `submission_unknown`，先核实远端，再用 `reset` 或 `--overwrite` 重建；这两种显式操作可能产生重复远端计算。没有服务端幂等键时，不承诺严格一次执行。

每条转写增加隐藏的 `.<stem>.complete.json`，记录产物摘要；标记最后写入，数据库完成后才清理远端任务。`lecture.part1.mp4` 对应 `lecture.part1.txt`；多个输入生成同一路径时，在请求前报告冲突。完成标记有效但数据库未记入时，`reconcile --apply` 可补记；缺失或改动产物则重置为待处理。已有结果未保存 JSON 时，新增 `--save-json` 会报告缺口，需显式 `--overwrite` 重建。

历史结果没有指纹和完成标记，CLI 会标注未验证并保留它们。可用 `reconcile --apply --adopt-legacy` 将当前视频/音频建立为基线；这不验证历史 ASR 参数。显式 `transcribe --overwrite` 可生成可校验的新结果。历史远端任务参数未记录时也需核实后显式重建。

同一数据库的同一阶段/组按单活动进程使用；允许一个 `extract` 与一个 `transcribe --watch` 配合。线程锁不承担多个转写进程的任务分配。

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

给 Pi Coding Agent 用的单文件扩展，不依赖 SQLite，适合对任意本地文件或目录直接跑转写（默认并发 3，结果默认输出在音频同目录）。只有输入/请求指纹和全部产物摘要匹配完成标记时才自动跳过；旧产物或标记失配会提示用 `--overwrite` 重建。异步任务在本地提交成功后默认删除，`--keep-remote-task` 可保留；Pi 的异步任务暂不支持跨进程持久化恢复。

- **当前目录启用**：在仓库根目录运行 `pi` 自动加载。
- **全局启用**：软链或复制到 `~/.pi/agent/extensions/qwen-asr.ts`。

支持两种调用方式：

```text
# 1. 斜杠命令（不消耗 LLM token）
/asr ./data/audio/my_channel --endpoint modal --out ./outputs --concurrency 4 --json

# 2. 自然语言（触发 asr_batch 工具）
把 ./data/audio/my_channel 下的音频批量转成 txt 和 srt，输出到 ./data/transcripts/my_channel，并发 4。
```

`/asr` 参数：`--endpoint`、`--out`、`--url`、`--api-key`、`--modal-key`、`--modal-secret`、`--header`、`--include-words` / `--no-include-words`、`--mode auto|sync|async`、`--concurrency`、`--lang`、`--json`、`--overwrite`、`--keep-remote-task`、`--no-recursive`。未配置 `.env` 时回退地址为 `http://127.0.0.1:8000`。

## 产物发布与恢复（`scripts/release_artifacts.py`）

源码仓库不提交大体积数据，转写文本与数据库快照通过 [GitHub Releases](https://github.com/chosenecho/qwen3-asr-pipeline/releases) 分发（原始音频留在本地）：

- `<group>-text.tar.gz`：`.txt` 和 `.srt`
- `<group>-json.tar.gz`：词级 `.json`（原始体积超过 450 MB 时自动按 `--max-part-raw-mb` 拆成独立的 `<group>-json-partXX.tar.gz`）
- `pipeline-db.tar.gz`：经 SQLite `backup` + `VACUUM` 导出的快照；默认 `--db-profile public` 按字段白名单导出，路径相对化，清除任务 ID、URL、错误消息和运行配置。它是归档索引，仅供 `status` / `list` 查询。`--db-profile private` 保存完整执行状态用于私有备份；旧 `--no-sanitize-db` 为 private 的兼容别名，与显式 public 冲突
- `MANIFEST.json`、`SHA256SUMS.txt`：文件清单与 SHA-256 校验和

```bash
# 本地打包到 release_dist/
python scripts/release_artifacts.py package

# 打包并顺序上传到 GitHub Release（按 SHA-256 断点续传与重试）
python scripts/release_artifacts.py publish --tag v2026.11 --title "Transcripts Release v2026.11"

# 从 Release 下载压缩包与 SHA256SUMS.txt 到 release_dist/ 后校验并恢复到 data/
python scripts/release_artifacts.py restore release_dist
```

恢复默认要求 `MANIFEST.json` 和 `SHA256SUMS.txt`，先校验清单及全部声明资产，再暂存解包并替换目标；缺少任一分卷会在覆盖前终止，额外旧归档会忽略。`--skip-verify` 只跳摘要，仍检查清单、文件集合、大小与路径。恢复需要带 `tarfile.data_filter` 的 Python（3.12+ 或发行版补丁 3.10.12+），并保留显式路径检查与 `filter="data"`。

打包成功后保留上一批到输出目录的 `.previous` 兄弟目录。恢复使用 `.release-restore/` 日志与备份，中断后下次恢复会先回滚未完成操作。多文件替换没有全局原子性，恢复期间应暂停目标目录的读写进程；数据库存在 WAL/SHM/journal sidecar 时会停止。私有快照中的绝对路径仍依赖原机器，跨 Windows/Linux 使用需要路径映射。公共快照的正文、标题和相对文件名保留原内容，字段脱敏不代表内容匿名化。

新 GitHub Release 先创建草稿，资产验证完成后再发布；已有公开 Release 更新时清单最后上传，短暂不一致应校验失败后重新下载。常规本地验证不会调用发布操作。

## 项目状态与开发

- **维护状态**：个人自用工具，随实际转写与部署需求维护。API 契约绑定 Qwen3-ASR HTTP 服务（`/health`、`/v1/asr/transcribe`、`/v1/asr/tasks`）。
- **代码约定与 Agent 指南**：见 [`AGENTS.md`](AGENTS.md)。
- **许可证**：仓库尚未添加 `LICENSE` 文件。

本地回归测试使用临时数据库和模拟网络：

```bash
python -m unittest discover -s tests -v
python -m py_compile asr_pipeline.py scripts/release_artifacts.py
node --test tests/test_pi_contract.mjs
```

Pi 离线测试使用 Node.js 24 内置 TypeScript 类型擦除与测试器，不新增 npm 依赖；它验证运行行为，完整 `tsc` 类型检查仍需 Pi 的开发依赖环境。Python 与 Pi 共用 `tests/fixtures/asr_cases.json`，统一结果包装、时间段与词级结果解析；纯文本字幕回退为 5 秒，不再次发起推理请求。
