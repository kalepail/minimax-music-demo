export type MusicInput = {
	prompt: string;
	is_instrumental: boolean;
	format: "mp3" | "wav";
	lyrics?: string;
};

export type JobState = "queued" | "running" | "complete" | "failed";

export type AttemptLog = {
	attempt: number;
	started_at: number;
	ended_at: number;
	duration_ms: number;
	error?: string;
};

export type JobRecord = {
	state: JobState;
	input: MusicInput;
	audio_url?: string;
	error?: string;
	attempts: number;
	attempt_log: AttemptLog[];
	created_at: number;
	started_at?: number;
	completed_at?: number;
	expires_at?: number;
};

export type PublicJobRecord = Omit<JobRecord, "audio_url" | "input"> & {
	ready: boolean;
};

export type RateLimitRecord = {
	window_start: number;
	count: number;
};

export type RateLimitResult = {
	allowed: boolean;
	record: RateLimitRecord;
	remaining: number;
	retry_after_ms: number;
};

export const PROMPT_MAX_CHARS = 2000;
export const LYRICS_MAX_CHARS = 3500;
export const FORMATS = new Set(["mp3", "wav"]);
export const ATTEMPT_TIMEOUT_MS = 14 * 60 * 1000;
export const STALE_JOB_MS = ATTEMPT_TIMEOUT_MS + 30 * 1000;
export const JOB_TTL_MS = 60 * 60 * 1000;
export const RATE_LIMIT_WINDOW_MS = 60 * 60 * 1000;
export const RATE_LIMIT_MAX_JOBS = 3;

const INPUT_FIELDS = new Set(["prompt", "lyrics", "format", "is_instrumental"]);

export function parseInput(body: unknown): MusicInput | { error: string } {
	if (!body || typeof body !== "object") return { error: "body must be a JSON object" };
	const raw = body as Record<string, unknown>;
	const unknownField = Object.keys(raw).find((key) => !INPUT_FIELDS.has(key));
	if (unknownField) return { error: `unsupported field: ${unknownField}` };

	const prompt = typeof raw.prompt === "string" ? raw.prompt.trim() : "";
	if (!prompt) return { error: "prompt is required" };
	if (prompt.length > PROMPT_MAX_CHARS) return { error: `prompt must be <= ${PROMPT_MAX_CHARS} chars` };

	if (raw.format !== undefined && (typeof raw.format !== "string" || !FORMATS.has(raw.format))) {
		return { error: "format must be mp3 or wav" };
	}
	const format = typeof raw.format === "string" ? (raw.format as "mp3" | "wav") : "mp3";
	const is_instrumental = raw.is_instrumental === true;
	const lyricsRaw = typeof raw.lyrics === "string" ? raw.lyrics.trim() : "";
	if (!is_instrumental && lyricsRaw.length > LYRICS_MAX_CHARS) {
		return { error: `lyrics must be <= ${LYRICS_MAX_CHARS} chars` };
	}
	const lyrics = lyricsRaw || undefined;

	const input: MusicInput = { prompt, is_instrumental, format };
	if (lyrics && !is_instrumental) input.lyrics = lyrics;
	return input;
}

export function publicStatus(record: JobRecord): PublicJobRecord {
	const { audio_url: _audioUrl, input: _input, ...publicRecord } = record;
	return { ...publicRecord, ready: record.state === "complete" };
}

export function shouldCleanUp(job: JobRecord, now = Date.now()): boolean {
	return (job.state === "complete" || job.state === "failed") && (!job.expires_at || now >= job.expires_at);
}

export function isStaleRunningJob(job: JobRecord, now = Date.now()): boolean {
	return job.state === "running" && typeof job.started_at === "number" && now - job.started_at >= STALE_JOB_MS;
}

export function applyRateLimit(
	record: RateLimitRecord | undefined,
	now = Date.now(),
	limit = RATE_LIMIT_MAX_JOBS,
	windowMs = RATE_LIMIT_WINDOW_MS,
): RateLimitResult {
	if (!record || now - record.window_start >= windowMs) {
		const next = { window_start: now, count: 1 };
		return {
			allowed: true,
			record: next,
			remaining: Math.max(0, limit - next.count),
			retry_after_ms: windowMs,
		};
	}

	const retryAfterMs = Math.max(0, record.window_start + windowMs - now);
	if (record.count >= limit) {
		return {
			allowed: false,
			record,
			remaining: 0,
			retry_after_ms: retryAfterMs,
		};
	}

	const next = { ...record, count: record.count + 1 };
	return {
		allowed: true,
		record: next,
		remaining: Math.max(0, limit - next.count),
		retry_after_ms: retryAfterMs,
	};
}

export function isExpiredRateLimit(record: RateLimitRecord, now = Date.now(), windowMs = RATE_LIMIT_WINDOW_MS): boolean {
	return now - record.window_start >= windowMs;
}

export function clientRateLimitKey(request: Request): string {
	const forwarded = request.headers.get("CF-Connecting-IP") ?? request.headers.get("X-Forwarded-For") ?? "local";
	const ip = forwarded.split(",")[0]?.trim() || "local";
	return `rate:${ip}`;
}

export function json(data: unknown, status = 200, extraHeaders?: HeadersInit): Response {
	const headers = new Headers(extraHeaders);
	headers.set("Content-Type", "application/json");
	return new Response(JSON.stringify(data), {
		status,
		headers,
	});
}

export function extractAudioUrl(value: unknown): string | undefined {
	if (!value || typeof value !== "object") return undefined;
	const v = value as { audio?: unknown; result?: { audio?: unknown } };
	if (typeof v.audio === "string" && v.audio) return v.audio;
	if (v.result && typeof v.result === "object" && typeof v.result.audio === "string" && v.result.audio) {
		return v.result.audio;
	}
	return undefined;
}

export function snippet(value: unknown): string | undefined {
	if (value === undefined || value === null) return undefined;
	try {
		const str = typeof value === "string" ? value : JSON.stringify(value);
		return str.length > 400 ? `${str.slice(0, 400)}...` : str;
	} catch {
		return String(value).slice(0, 400);
	}
}

export function audioResponseHeaders(record: JobRecord, upstream: Response): Headers {
	const headers = new Headers();
	headers.set(
		"Content-Type",
		upstream.headers.get("content-type") ?? (record.input.format === "wav" ? "audio/wav" : "audio/mpeg"),
	);
	headers.set("Cache-Control", "no-store");

	for (const name of ["content-length", "content-range", "accept-ranges"]) {
		const value = upstream.headers.get(name);
		if (value) headers.set(name, value);
	}
	if (!headers.has("Accept-Ranges")) headers.set("Accept-Ranges", "none");
	return headers;
}
