import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const EXTENSION_REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

const AUDIO_EXTENSIONS = new Set([
	".wav",
	".mp3",
	".m4a",
	".flac",
	".ogg",
	".opus",
	".aac",
	".wma",
	".webm",
	".mp4",
	".mkv",
	".mov",
]);

const DONE_STATUSES = new Set(["completed", "succeeded", "success", "done", "finished"]);
const FAIL_STATUSES = new Set(["failed", "error", "cancelled", "canceled"]);

type RunMode = "auto" | "sync" | "async";

interface TimestampSegment {
	start: number;
	end: number;
	text: string;
}

interface ParsedAsrResult {
	text: string;
	segments: TimestampSegment[];
	raw: unknown;
}

interface BatchOptions {
	inputPath: string;
	outputDir?: string;
	endpoint?: string;
	baseUrl: string;
	apiKey?: string;
	modalKey?: string;
	modalSecret?: string;
	headers?: Record<string, string>;
	includeWords: boolean;
	mode: RunMode;
	concurrency: number;
	overwrite: boolean;
	recursive: boolean;
	saveJson: boolean;
	language?: string;
	asyncDurationSec: number;
	asyncSizeMb: number;
	pollIntervalMs: number;
	taskTimeoutMs: number;
}

interface FileOutcome {
	input: string;
	status: "done" | "skipped" | "failed";
	mode?: "sync" | "async";
	txtPath?: string;
	srtPath?: string;
	jsonPath?: string;
	error?: string;
}

function cleanPath(p: string): string {
	const trimmed = p.trim().replace(/^['"]|['"]$/g, "");
	return trimmed.startsWith("@") ? trimmed.slice(1) : trimmed;
}

function trimTrailingSlash(url: string): string {
	return url.replace(/\/+$/, "");
}

function getMimeType(filePath: string): string {
	const ext = path.extname(filePath).toLowerCase();
	const map: Record<string, string> = {
		".wav": "audio/wav",
		".mp3": "audio/mpeg",
		".m4a": "audio/mp4",
		".flac": "audio/flac",
		".ogg": "audio/ogg",
		".opus": "audio/opus",
		".aac": "audio/aac",
		".webm": "audio/webm",
		".mp4": "video/mp4",
		".mkv": "video/x-matroska",
		".mov": "video/quicktime",
	};
	return map[ext] ?? "application/octet-stream";
}

function parseBoolEnv(raw?: string): boolean {
	if (!raw) return false;
	return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
}

function normalizeEndpointName(name: string): string {
	return name.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function parseHeadersSpec(raw?: string): Record<string, string> {
	const out: Record<string, string> = {};
	if (!raw?.trim()) return out;
	const text = raw.trim();
	if (text.startsWith("{")) {
		try {
			const parsed = JSON.parse(text);
			if (parsed && typeof parsed === "object") {
				for (const [k, v] of Object.entries(parsed)) {
					const ks = k.trim();
					const vs = String(v ?? "").trim();
					if (ks && vs) out[ks] = vs;
				}
				return out;
			}
		} catch {
			// Fall through to Key: Value parsing
		}
	}
	for (const part of text.split(/[;\r\n]+/)) {
		const idx = part.indexOf(":");
		if (idx <= 0) continue;
		const k = part.slice(0, idx).trim();
		const v = part.slice(idx + 1).trim();
		if (k && v) out[k] = v;
	}
	return out;
}

function loadMergedEnv(cwd?: string): Record<string, string> {
	const merged: Record<string, string> = {};
	const candidates = [
		path.join(EXTENSION_REPO_ROOT, ".env"),
		...(cwd ? [path.resolve(cwd, ".env")] : []),
	];
	const seen = new Set<string>();
	for (const envFile of candidates) {
		const resolved = path.resolve(envFile);
		if (seen.has(resolved)) continue;
		seen.add(resolved);
		try {
			const content = readFileSync(resolved, "utf8");
			for (const rawLine of content.split(/\r?\n/)) {
				const line = rawLine.trim();
				if (!line || line.startsWith("#") || !line.includes("=")) continue;
				const eqIdx = line.indexOf("=");
				const key = line.slice(0, eqIdx).trim();
				const val = line.slice(eqIdx + 1).trim().replace(/^['"]|['"]$/g, "");
				if (key) merged[key] = val;
			}
		} catch {
			// Ignore missing .env
		}
	}
	for (const [k, v] of Object.entries(process.env)) {
		if (v !== undefined) merged[k] = v;
	}
	return merged;
}

function isModalUrl(url: string): boolean {
	try {
		return new URL(url).hostname.toLowerCase().endsWith(".modal.run");
	} catch {
		return false;
	}
}

function buildRequestHeaders(opts: BatchOptions): Record<string, string> {
	const headers: Record<string, string> = { ...(opts.headers ?? {}) };
	if (opts.modalKey?.trim()) {
		headers["Modal-Key"] = opts.modalKey.trim();
	}
	if (opts.modalSecret?.trim()) {
		headers["Modal-Secret"] = opts.modalSecret.trim();
	}
	if (opts.apiKey?.trim()) {
		headers.Authorization = `Bearer ${opts.apiKey.trim()}`;
	}
	return headers;
}

function validateEndpointAuth(opts: BatchOptions): void {
	const headers = buildRequestHeaders(opts);
	const needsModal = opts.endpoint?.toLowerCase() === "modal" || isModalUrl(opts.baseUrl);
	if (!needsModal) return;
	const mk = headers["Modal-Key"]?.trim() ?? "";
	const ms = headers["Modal-Secret"]?.trim() ?? "";
	const missing: string[] = [];
	if (!mk || mk === "wk-..." || mk === "<Modal-Key>") missing.push("Modal-Key (ASR_MODAL_KEY)");
	if (!ms || ms === "ws-..." || ms === "<Modal-Secret>") missing.push("Modal-Secret (ASR_MODAL_SECRET)");
	if (missing.length > 0) {
		throw new Error(`Modal 端点缺少鉴权 Header: ${missing.join(", ")}。请在 .env 中填入或通过 --modal-key / --modal-secret 传入。`);
	}
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal?.aborted) {
			reject(new Error("Aborted"));
			return;
		}
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		const onAbort = () => {
			clearTimeout(timer);
			reject(new Error("Aborted"));
		};
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

async function probeDurationSec(filePath: string): Promise<number | null> {
	try {
		const { stdout } = await execFileAsync(
			"ffprobe",
			[
				"-v",
				"error",
				"-show_entries",
				"format=duration",
				"-of",
				"default=noprint_wrappers=1:nokey=1",
				filePath,
			],
			{ timeout: 10_000 },
		);
		const value = Number.parseFloat(stdout.trim());
		return Number.isFinite(value) ? value : null;
	} catch {
		return null;
	}
}

async function resolveMode(filePath: string, opts: BatchOptions): Promise<"sync" | "async"> {
	if (opts.mode === "sync" || opts.mode === "async") {
		return opts.mode;
	}
	const duration = await probeDurationSec(filePath);
	if (duration !== null) {
		return duration > opts.asyncDurationSec ? "async" : "sync";
	}
	const info = await stat(filePath);
	return info.size > opts.asyncSizeMb * 1024 * 1024 ? "async" : "sync";
}

function normalizeSegment(item: any): TimestampSegment | null {
	if (!item || typeof item !== "object") return null;
	const text = String(item.text ?? item.word ?? item.token ?? "").trim();
	if (!text) return null;

	let start = item.start ?? item.start_time ?? item.begin ?? item.offset;
	let end = item.end ?? item.end_time ?? item.stop;
	if (Array.isArray(item.timestamp) && item.timestamp.length >= 2) {
		start = item.timestamp[0];
		end = item.timestamp[1];
	}

	const startNum = Number(start);
	const endNum = Number(end);
	if (!Number.isFinite(startNum)) return null;
	const safeEnd = Number.isFinite(endNum) && endNum >= startNum ? endNum : startNum + 0.5;
	return { start: Math.max(0, startNum), end: Math.max(0, safeEnd), text };
}

function groupWordsToSegments(words: TimestampSegment[]): TimestampSegment[] {
	if (words.length === 0) return [];
	const segments: TimestampSegment[] = [];
	let currentText = "";
	let currentStart = words[0].start;
	let currentEnd = words[0].end;

	const flush = () => {
		const trimmed = currentText.trim();
		if (trimmed) {
			segments.push({ start: currentStart, end: currentEnd, text: trimmed });
		}
		currentText = "";
	};

	const needsSpace = (prev: string, next: string) =>
		/[A-Za-z0-9]$/.test(prev) && /^[A-Za-z0-9]/.test(next);

	for (let i = 0; i < words.length; i++) {
		const w = words[i];
		const gap = i > 0 ? w.start - words[i - 1].end : 0;
		const shouldSplitBefore =
			currentText.length > 0 && (gap > 0.8 || currentText.length >= 28 || w.end - currentStart >= 8);

		if (shouldSplitBefore) {
			flush();
			currentStart = w.start;
		}

		currentText += currentText && needsSpace(currentText, w.text) ? ` ${w.text}` : w.text;
		currentEnd = w.end;

		if (/[。！？!?；;.\n]$/.test(w.text)) {
			flush();
			if (i + 1 < words.length) {
				currentStart = words[i + 1].start;
			}
		}
	}
	flush();
	return segments;
}

function parseAsrJson(data: any): ParsedAsrResult {
	const payload = data?.result && typeof data.result === "object" ? data.result : data ?? {};

	const rawSegments = Array.isArray(payload.segments)
		? payload.segments
		: Array.isArray(payload.chunks)
			? payload.chunks
			: [];
	let segments = rawSegments.map(normalizeSegment).filter((s): s is TimestampSegment => s !== null);

	if (segments.length === 0) {
		const rawWords = Array.isArray(payload.words) ? payload.words : [];
		const words = rawWords.map(normalizeSegment).filter((w): w is TimestampSegment => w !== null);
		segments = groupWordsToSegments(words);
	}

	const directText = typeof payload.text === "string" ? payload.text.trim() : "";
	const fallbackText = segments.map((s) => s.text).join("\n");

	return {
		text: directText || fallbackText,
		segments,
		raw: data,
	};
}

function formatSrtTime(seconds: number): string {
	const totalMs = Math.max(0, Math.round(seconds * 1000));
	const hours = Math.floor(totalMs / 3_600_000);
	const minutes = Math.floor((totalMs % 3_600_000) / 60_000);
	const secs = Math.floor((totalMs % 60_000) / 1000);
	const ms = totalMs % 1000;
	const pad2 = (n: number) => String(n).padStart(2, "0");
	const pad3 = (n: number) => String(n).padStart(3, "0");
	return `${pad2(hours)}:${pad2(minutes)}:${pad2(secs)},${pad3(ms)}`;
}

function segmentsToSrt(segments: TimestampSegment[], fallbackText: string): string {
	if (segments.length === 0) {
		if (!fallbackText.trim()) return "";
		return `1\n00:00:00,000 --> 00:00:05,000\n${fallbackText.trim()}\n`;
	}
	return (
		segments
			.map((seg, idx) => `${idx + 1}\n${formatSrtTime(seg.start)} --> ${formatSrtTime(seg.end)}\n${seg.text}`)
			.join("\n\n") + "\n"
	);
}

async function buildUploadForm(
	filePath: string,
	fields: Record<string, string | undefined>,
): Promise<FormData> {
	const bytes = await readFile(filePath);
	const form = new FormData();
	const blob = new Blob([bytes], { type: getMimeType(filePath) });
	form.append("file", blob, path.basename(filePath));
	for (const [k, v] of Object.entries(fields)) {
		if (v !== undefined && v !== "") {
			form.append(k, v);
		}
	}
	return form;
}

async function transcribeSync(
	filePath: string,
	opts: BatchOptions,
	signal?: AbortSignal,
): Promise<{ txt: string; srt: string; raw: unknown }> {
	const url = `${trimTrailingSlash(opts.baseUrl)}/v1/asr/transcribe`;
	const headers = buildRequestHeaders(opts);
	const form = await buildUploadForm(filePath, {
		response_format: "json",
		return_timestamps: "true",
		include_words: opts.includeWords ? "true" : undefined,
		language: opts.language,
	});

	const res = await fetch(url, {
		method: "POST",
		headers,
		body: form,
		signal,
	});
	if (!res.ok) {
		const body = await res.text().catch(() => "");
		throw new Error(`POST /v1/asr/transcribe failed (${res.status}): ${body.slice(0, 300)}`);
	}

	const json = await res.json();
	const parsed = parseAsrJson(json);
	let srt = segmentsToSrt(parsed.segments, parsed.text);

	// Fallback: if server returned no segments/words in JSON, request SRT directly once
	if (parsed.segments.length === 0 && parsed.text) {
		try {
			const srtForm = await buildUploadForm(filePath, {
				response_format: "srt",
				return_timestamps: "true",
				language: opts.language,
			});
			const srtRes = await fetch(url, {
				method: "POST",
				headers,
				body: srtForm,
				signal,
			});
			if (srtRes.ok) {
				const serverSrt = (await srtRes.text()).trim();
				if (serverSrt) srt = `${serverSrt}\n`;
			}
		} catch {
			// Keep locally generated fallback SRT
		}
	}

	return {
		txt: parsed.text ? `${parsed.text}\n` : "",
		srt,
		raw: parsed.raw,
	};
}

async function transcribeAsync(
	filePath: string,
	opts: BatchOptions,
	signal?: AbortSignal,
): Promise<{ txt: string; srt: string; raw: unknown }> {
	const base = trimTrailingSlash(opts.baseUrl);
	const headers = buildRequestHeaders(opts);

	// 1. Submit async task
	const form = await buildUploadForm(filePath, {
		return_timestamps: "true",
		include_words: opts.includeWords ? "true" : undefined,
		language: opts.language,
	});
	const createRes = await fetch(`${base}/v1/asr/tasks`, {
		method: "POST",
		headers,
		body: form,
		signal,
	});
	if (!createRes.ok) {
		const body = await createRes.text().catch(() => "");
		throw new Error(`POST /v1/asr/tasks failed (${createRes.status}): ${body.slice(0, 300)}`);
	}

	const createJson: any = await createRes.json();
	const taskId = createJson?.task_id ?? createJson?.id ?? createJson?.data?.task_id;
	if (!taskId) {
		throw new Error(`POST /v1/asr/tasks returned no task_id: ${JSON.stringify(createJson).slice(0, 200)}`);
	}

	// 2. Poll task status until finished
	const deadline = Date.now() + opts.taskTimeoutMs;
	let taskJson: any = createJson;
	while (true) {
		if (signal?.aborted) throw new Error("Aborted");
		if (Date.now() > deadline) {
			throw new Error(`Task ${taskId} timed out after ${Math.round(opts.taskTimeoutMs / 1000)}s`);
		}

		const pollRes = await fetch(`${base}/v1/asr/tasks/${encodeURIComponent(String(taskId))}`, {
			headers,
			signal,
		});
		if (!pollRes.ok) {
			const body = await pollRes.text().catch(() => "");
			throw new Error(`GET /v1/asr/tasks/${taskId} failed (${pollRes.status}): ${body.slice(0, 300)}`);
		}
		taskJson = await pollRes.json();

		const status = String(taskJson?.status ?? taskJson?.state ?? "").toLowerCase();
		if (DONE_STATUSES.has(status)) {
			break;
		}
		if (FAIL_STATUSES.has(status)) {
			const errMsg = taskJson?.error ?? taskJson?.message ?? status;
			throw new Error(`Task ${taskId} failed: ${String(errMsg)}`);
		}

		await sleep(opts.pollIntervalMs, signal);
	}

	const parsed = parseAsrJson(taskJson);

	// 3. Prefer server-exported SRT (exact server formatting), fallback to local SRT from JSON
	let srt = "";
	try {
		const subRes = await fetch(
			`${base}/v1/asr/tasks/${encodeURIComponent(String(taskId))}/subtitle?format=srt`,
			{ headers, signal },
		);
		if (subRes.ok) {
			const text = (await subRes.text()).trim();
			if (text) srt = `${text}\n`;
		}
	} catch {
		// Fallback below
	}
	if (!srt) {
		srt = segmentsToSrt(parsed.segments, parsed.text);
	}

	return {
		txt: parsed.text ? `${parsed.text}\n` : "",
		srt,
		raw: parsed.raw,
	};
}

async function collectAudioFiles(targetPath: string, recursive: boolean): Promise<string[]> {
	const info = await stat(targetPath);
	if (info.isFile()) {
		return [targetPath];
	}
	const results: string[] = [];
	const walk = async (dir: string) => {
		const entries = await readdir(dir, { withFileTypes: true });
		entries.sort((a, b) => a.name.localeCompare(b.name));
		for (const entry of entries) {
			const full = path.join(dir, entry.name);
			if (entry.isDirectory()) {
				if (recursive) await walk(full);
			} else if (entry.isFile()) {
				const ext = path.extname(entry.name).toLowerCase();
				if (AUDIO_EXTENSIONS.has(ext)) {
					results.push(full);
				}
			}
		}
	};
	await walk(targetPath);
	return results;
}

async function fileHasContent(filePath: string): Promise<boolean> {
	try {
		const info = await stat(filePath);
		return info.isFile() && info.size > 0;
	} catch {
		return false;
	}
}

async function runBatch(
	opts: BatchOptions,
	cwd: string,
	onProgress?: (msg: string) => void,
	signal?: AbortSignal,
): Promise<FileOutcome[]> {
	validateEndpointAuth(opts);
	const resolvedInput = path.resolve(cwd, cleanPath(opts.inputPath));
	const inputStat = await stat(resolvedInput);
	const baseInputDir = inputStat.isDirectory() ? resolvedInput : path.dirname(resolvedInput);
	const resolvedOutputDir = opts.outputDir ? path.resolve(cwd, cleanPath(opts.outputDir)) : undefined;

	const files = await collectAudioFiles(resolvedInput, opts.recursive);
	if (files.length === 0) {
		return [];
	}

	const outcomes: FileOutcome[] = new Array(files.length);
	let nextIdx = 0;
	let finished = 0;

	const worker = async () => {
		while (true) {
			if (signal?.aborted) return;
			const idx = nextIdx++;
			if (idx >= files.length) return;

			const filePath = files[idx];
			const relPath = path.relative(baseInputDir, filePath);
			const targetDir = resolvedOutputDir
				? path.join(resolvedOutputDir, path.dirname(relPath))
				: path.dirname(filePath);
			const stem = path.basename(filePath, path.extname(filePath));

			const txtPath = path.join(targetDir, `${stem}.txt`);
			const srtPath = path.join(targetDir, `${stem}.srt`);
			const jsonPath = path.join(targetDir, `${stem}.json`);

			try {
				if (!opts.overwrite && (await fileHasContent(txtPath)) && (await fileHasContent(srtPath))) {
					outcomes[idx] = { input: filePath, status: "skipped", txtPath, srtPath };
				} else {
					await mkdir(targetDir, { recursive: true });
					const chosenMode = await resolveMode(filePath, opts);
					onProgress?.(`[${finished}/${files.length}] 正在转写 (${chosenMode}): ${relPath}`);

					const result =
						chosenMode === "async"
							? await transcribeAsync(filePath, opts, signal)
							: await transcribeSync(filePath, opts, signal);

					await writeFile(txtPath, result.txt, "utf8");
					await writeFile(srtPath, result.srt, "utf8");
					if (opts.saveJson) {
						await writeFile(jsonPath, JSON.stringify(result.raw, null, 2), "utf8");
					}

					outcomes[idx] = {
						input: filePath,
						status: "done",
						mode: chosenMode,
						txtPath,
						srtPath,
						jsonPath: opts.saveJson ? jsonPath : undefined,
					};
				}
			} catch (err) {
				outcomes[idx] = {
					input: filePath,
					status: "failed",
					error: err instanceof Error ? err.message : String(err),
				};
			} finally {
				finished++;
				onProgress?.(`[${finished}/${files.length}] 已处理: ${relPath}`);
			}
		}
	};

	const workers = Array.from({ length: Math.max(1, Math.min(opts.concurrency, files.length)) }, () =>
		worker(),
	);
	await Promise.all(workers);
	return outcomes.filter(Boolean);
}

function formatSummary(outcomes: FileOutcome[], baseUrl: string): string {
	if (outcomes.length === 0) {
		return "未找到可处理的音频文件。";
	}
	const done = outcomes.filter((o) => o.status === "done");
	const skipped = outcomes.filter((o) => o.status === "skipped");
	const failed = outcomes.filter((o) => o.status === "failed");

	const lines = [
		`ASR 批处理完成（服务：${baseUrl}）`,
		`总计: ${outcomes.length} | 成功: ${done.length} | 跳过(已存在): ${skipped.length} | 失败: ${failed.length}`,
	];

	if (done.length > 0) {
		lines.push("", "已生成：");
		for (const item of done.slice(0, 30)) {
			lines.push(`- [${item.mode}] ${path.basename(item.input)} -> .txt, .srt${item.jsonPath ? ", .json" : ""}`);
		}
		if (done.length > 30) {
			lines.push(`- ... 其余 ${done.length - 30} 条已省略`);
		}
	}

	if (failed.length > 0) {
		lines.push("", "失败项：");
		for (const item of failed) {
			lines.push(`- ${item.input}: ${item.error}`);
		}
	}

	return lines.join("\n");
}

function resolveEndpointDefaults(
	env: Record<string, string>,
	endpoint?: string,
	cliUrl?: string,
): {
	endpoint: string;
	baseUrl: string;
	apiKey?: string;
	modalKey?: string;
	modalSecret?: string;
	headers: Record<string, string>;
	includeWords: boolean;
} {
	const explicitEndpoint = endpoint?.trim() ?? "";
	let chosen = explicitEndpoint || env.ASR_ENDPOINT?.trim() || "local";

	if (!explicitEndpoint && cliUrl?.trim()) {
		const normCli = trimTrailingSlash(cliUrl.trim());
		for (const [k, v] of Object.entries(env)) {
			const m = k.match(/^ASR_([A-Z0-9_]+)_BASE_URL$/);
			if (m && v && trimTrailingSlash(v.trim()) === normCli) {
				chosen = m[1].toLowerCase();
				break;
			}
		}
	}

	const lower = chosen.toLowerCase();
	if (!lower || lower === "local" || lower === "default") {
		if (env.ASR_LOCAL_BASE_URL?.trim()) {
			return {
				endpoint: "local",
				baseUrl: trimTrailingSlash(env.ASR_LOCAL_BASE_URL.trim()),
				apiKey: env.ASR_LOCAL_API_KEY?.trim() || undefined,
				modalKey: env.ASR_LOCAL_MODAL_KEY?.trim() || undefined,
				modalSecret: env.ASR_LOCAL_MODAL_SECRET?.trim() || undefined,
				headers: parseHeadersSpec(env.ASR_LOCAL_HEADERS),
				includeWords: parseBoolEnv(env.ASR_LOCAL_INCLUDE_WORDS),
			};
		}
		return {
			endpoint: "local",
			baseUrl: trimTrailingSlash(env.ASR_BASE_URL?.trim() || "http://127.0.0.1:8000"),
			apiKey: env.ASR_API_KEY?.trim() || undefined,
			headers: parseHeadersSpec(env.ASR_HEADERS || env.ASR_EXTRA_HEADERS),
			includeWords: parseBoolEnv(env.ASR_INCLUDE_WORDS),
		};
	}

	const norm = normalizeEndpointName(lower);
	const prefix = `ASR_${norm}_`;
	const modalKey =
		env[`${prefix}MODAL_KEY`]?.trim() ||
		(norm === "MODAL" ? env.ASR_MODAL_KEY?.trim() : undefined) ||
		undefined;
	const modalSecret =
		env[`${prefix}MODAL_SECRET`]?.trim() ||
		(norm === "MODAL" ? env.ASR_MODAL_SECRET?.trim() : undefined) ||
		undefined;

	return {
		endpoint: lower,
		baseUrl: trimTrailingSlash(env[`${prefix}BASE_URL`]?.trim() || ""),
		apiKey: env[`${prefix}API_KEY`]?.trim() || undefined,
		modalKey,
		modalSecret,
		headers: parseHeadersSpec(env[`${prefix}HEADERS`]),
		includeWords: parseBoolEnv(env[`${prefix}INCLUDE_WORDS`]),
	};
}

function defaultOptions(overrides: Partial<BatchOptions> & { inputPath: string }, cwd?: string): BatchOptions {
	const env = loadMergedEnv(cwd);
	const ep = resolveEndpointDefaults(env, overrides.endpoint, overrides.baseUrl);
	const envConcurrency = Number.parseInt(env.ASR_CONCURRENCY ?? "3", 10);
	const envAsyncSec = Number.parseInt(env.ASR_ASYNC_DURATION_SEC ?? "150", 10);
	const envAsyncMb = Number.parseFloat(env.ASR_ASYNC_SIZE_MB ?? "3");
	const envPollMs = Number.parseInt(env.ASR_POLL_INTERVAL_MS ?? "2000", 10);
	const envTimeoutMs = Number.parseInt(env.ASR_TASK_TIMEOUT_MS ?? "1800000", 10);

	const baseUrl = trimTrailingSlash(overrides.baseUrl?.trim() || ep.baseUrl);
	if (!baseUrl) {
		throw new Error(`未找到端点 "${ep.endpoint}" 的地址，请在 .env 中配置 ASR_${normalizeEndpointName(ep.endpoint)}_BASE_URL 或传入 --url。`);
	}

	return {
		inputPath: overrides.inputPath,
		outputDir: overrides.outputDir,
		endpoint: ep.endpoint,
		baseUrl,
		apiKey: overrides.apiKey ?? ep.apiKey,
		modalKey: overrides.modalKey ?? ep.modalKey,
		modalSecret: overrides.modalSecret ?? ep.modalSecret,
		headers: { ...ep.headers, ...(overrides.headers ?? {}) },
		includeWords: overrides.includeWords ?? ep.includeWords,
		mode: overrides.mode ?? "auto",
		concurrency: overrides.concurrency ?? (Number.isFinite(envConcurrency) ? envConcurrency : 3),
		overwrite: overrides.overwrite ?? false,
		recursive: overrides.recursive ?? true,
		saveJson: overrides.saveJson ?? false,
		language: overrides.language ?? env.ASR_LANGUAGE,
		asyncDurationSec: Number.isFinite(envAsyncSec) ? envAsyncSec : 150,
		asyncSizeMb: Number.isFinite(envAsyncMb) ? envAsyncMb : 3,
		pollIntervalMs: Number.isFinite(envPollMs) ? envPollMs : 2000,
		taskTimeoutMs: Number.isFinite(envTimeoutMs) ? envTimeoutMs : 1_800_000,
	};
}

function parseCommandArgs(rawArgs: string): Partial<BatchOptions> & { inputPath?: string } {
	const tokens = rawArgs.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) ?? [];
	const parsed: Partial<BatchOptions> & { inputPath?: string } = {};

	const addHeader = (spec: string) => {
		const idx = spec.indexOf(":");
		if (idx <= 0) return;
		const k = spec.slice(0, idx).trim();
		const v = spec.slice(idx + 1).trim();
		if (k) {
			parsed.headers = { ...(parsed.headers ?? {}), [k]: v };
		}
	};

	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i].replace(/^['"]|['"]$/g, "");
		if (token === "--out" && tokens[i + 1]) {
			parsed.outputDir = tokens[++i].replace(/^['"]|['"]$/g, "");
		} else if (token.startsWith("--out=")) {
			parsed.outputDir = token.slice(6);
		} else if (token === "--endpoint" && tokens[i + 1]) {
			parsed.endpoint = tokens[++i].replace(/^['"]|['"]$/g, "");
		} else if (token.startsWith("--endpoint=")) {
			parsed.endpoint = token.slice(11);
		} else if (token === "--url" && tokens[i + 1]) {
			parsed.baseUrl = tokens[++i].replace(/^['"]|['"]$/g, "");
		} else if (token.startsWith("--url=")) {
			parsed.baseUrl = token.slice(6);
		} else if (token === "--api-key" && tokens[i + 1]) {
			parsed.apiKey = tokens[++i].replace(/^['"]|['"]$/g, "");
		} else if (token.startsWith("--api-key=")) {
			parsed.apiKey = token.slice(10);
		} else if (token === "--modal-key" && tokens[i + 1]) {
			parsed.modalKey = tokens[++i].replace(/^['"]|['"]$/g, "");
		} else if (token.startsWith("--modal-key=")) {
			parsed.modalKey = token.slice(12);
		} else if (token === "--modal-secret" && tokens[i + 1]) {
			parsed.modalSecret = tokens[++i].replace(/^['"]|['"]$/g, "");
		} else if (token.startsWith("--modal-secret=")) {
			parsed.modalSecret = token.slice(15);
		} else if (token === "--header" && tokens[i + 1]) {
			addHeader(tokens[++i].replace(/^['"]|['"]$/g, ""));
		} else if (token.startsWith("--header=")) {
			addHeader(token.slice(9));
		} else if (token === "--include-words") {
			parsed.includeWords = true;
		} else if (token === "--no-include-words") {
			parsed.includeWords = false;
		} else if (token === "--mode" && tokens[i + 1]) {
			parsed.mode = tokens[++i] as RunMode;
		} else if (token.startsWith("--mode=")) {
			parsed.mode = token.slice(7) as RunMode;
		} else if (token === "--concurrency" && tokens[i + 1]) {
			parsed.concurrency = Number.parseInt(tokens[++i], 10);
		} else if (token.startsWith("--concurrency=")) {
			parsed.concurrency = Number.parseInt(token.slice(14), 10);
		} else if (token === "--lang" && tokens[i + 1]) {
			parsed.language = tokens[++i];
		} else if (token.startsWith("--lang=")) {
			parsed.language = token.slice(7);
		} else if (token === "--overwrite") {
			parsed.overwrite = true;
		} else if (token === "--no-recursive") {
			parsed.recursive = false;
		} else if (token === "--json") {
			parsed.saveJson = true;
		} else if (!token.startsWith("--") && !parsed.inputPath) {
			parsed.inputPath = token;
		}
	}
	return parsed;
}

export default function qwenAsrExtension(pi: ExtensionAPI) {
	pi.registerTool({
		name: "asr_batch",
		label: "Qwen3 ASR Batch",
		description:
			"Batch transcribe a local audio file or directory via a configured Qwen3-ASR HTTP service (e.g. local or modal endpoint from .env), generating .txt and .srt files (and optionally raw .json). Automatically routes short audio to POST /v1/asr/transcribe and long audio (>150s) to async POST /v1/asr/tasks.",
		parameters: Type.Object({
			inputPath: Type.String({ description: "Path to an audio file or directory containing audio files" }),
			outputDir: Type.Optional(
				Type.String({ description: "Optional output directory; defaults to alongside each audio file" }),
			),
			endpoint: Type.Optional(
				Type.String({ description: "Named ASR endpoint from .env / env vars (e.g. 'local' or 'modal')" }),
			),
			baseUrl: Type.Optional(
				Type.String({ description: "ASR server base URL override (defaults to endpoint config or ASR_BASE_URL)" }),
			),
			apiKey: Type.Optional(
				Type.String({ description: "Optional Bearer token override" }),
			),
			modalKey: Type.Optional(
				Type.String({ description: "Optional Modal-Key header override" }),
			),
			modalSecret: Type.Optional(
				Type.String({ description: "Optional Modal-Secret header override" }),
			),
			headers: Type.Optional(
				Type.Record(Type.String(), Type.String(), { description: "Optional extra HTTP headers" }),
			),
			includeWords: Type.Optional(
				Type.Boolean({ description: "Include word-level timestamps (include_words=true)" }),
			),
			mode: Type.Optional(
				Type.Union([Type.Literal("auto"), Type.Literal("sync"), Type.Literal("async")], {
					description: "auto (default: <=150s sync, >150s async), sync (/v1/asr/transcribe), or async (/v1/asr/tasks)",
				}),
			),
			concurrency: Type.Optional(Type.Number({ description: "Max concurrent files (default 3)" })),
			overwrite: Type.Optional(Type.Boolean({ description: "Overwrite existing .txt/.srt (default false)" })),
			recursive: Type.Optional(Type.Boolean({ description: "Scan subdirectories recursively (default true)" })),
			saveJson: Type.Optional(Type.Boolean({ description: "Also save raw timestamp JSON (default false)" })),
			language: Type.Optional(Type.String({ description: "Optional language hint" })),
		}),
		async execute(_toolCallId: string, params: any, arg3: any, arg4: any, arg5: any) {
			// Compatible with both Pi execute(id, params, signal, onUpdate, ctx) and execute(id, params, onUpdate, ctx, signal)
			const signal: AbortSignal | undefined =
				typeof arg3 === "function" ? arg5 : arg3;
			const onUpdate: ((update: { content: { type: "text"; text: string }[]; details?: unknown }) => void) | undefined =
				typeof arg3 === "function" ? arg3 : arg4;
			const ctx: any = typeof arg3 === "function" ? arg4 : arg5;

			const cwd = ctx?.cwd ?? process.cwd();
			const opts = defaultOptions(
				{
					inputPath: params.inputPath,
					outputDir: params.outputDir,
					endpoint: params.endpoint,
					baseUrl: params.baseUrl,
					apiKey: params.apiKey,
					modalKey: params.modalKey,
					modalSecret: params.modalSecret,
					headers: params.headers,
					includeWords: params.includeWords,
					mode: params.mode,
					concurrency: params.concurrency,
					overwrite: params.overwrite,
					recursive: params.recursive,
					saveJson: params.saveJson,
					language: params.language,
				},
				cwd,
			);

			const outcomes = await runBatch(
				opts,
				cwd,
				(msg) => {
					onUpdate?.({ content: [{ type: "text", text: msg }] });
				},
				signal,
			);

			const summary = formatSummary(outcomes, opts.baseUrl);
			return {
				content: [{ type: "text", text: summary }],
				details: { endpoint: opts.endpoint, baseUrl: opts.baseUrl, outcomes },
			};
		},
	});

	pi.registerCommand("asr", {
		description: "批量转写音频为 .txt + .srt：/asr <文件或目录> [--endpoint local|modal] [--out DIR] [--url URL] [--mode auto|sync|async] [--concurrency N] [--json] [--overwrite]",
		handler: async (args: string, ctx: any) => {
			const parsed = parseCommandArgs(args ?? "");
			if (!parsed.inputPath) {
				ctx.ui.notify(
					"用法: /asr <文件或目录> [--endpoint local|modal] [--out 输出目录] [--url http://ip:port] [--mode auto|sync|async] [--concurrency 3] [--json] [--overwrite]",
					"info",
				);
				return;
			}

			const cwd = ctx?.cwd ?? process.cwd();
			let opts: BatchOptions;
			try {
				opts = defaultOptions(
					{
						...parsed,
						inputPath: parsed.inputPath,
					},
					cwd,
				);
			} catch (err) {
				ctx.ui.notify(`ASR 配置错误: ${err instanceof Error ? err.message : String(err)}`, "error");
				return;
			}

			ctx.ui.setStatus("qwen-asr", `ASR 启动中 (${opts.endpoint}: ${opts.baseUrl})...`);
			try {
				const outcomes = await runBatch(opts, cwd, (msg) => {
					ctx.ui.setStatus("qwen-asr", msg);
				});
				const summary = formatSummary(outcomes, opts.baseUrl);
				const hasFail = outcomes.some((o) => o.status === "failed");
				ctx.ui.notify(summary, hasFail ? "warning" : "info");
			} catch (err) {
				ctx.ui.notify(`ASR 执行失败: ${err instanceof Error ? err.message : String(err)}`, "error");
			} finally {
				ctx.ui.setStatus("qwen-asr", "");
			}
		},
	});
}
