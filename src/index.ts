import { DurableObject } from "cloudflare:workers";

type MusicInput = {
	prompt: string;
	is_instrumental: boolean;
	format: "mp3" | "wav";
	lyrics?: string;
};

type JobState = "queued" | "running" | "complete" | "failed";

type AttemptLog = {
	attempt: number;
	started_at: number;
	ended_at: number;
	duration_ms: number;
	log_id?: string;
	cache_hit: boolean;
	error?: string;
	raw_response_snippet?: string;
};

type JobRecord = {
	state: JobState;
	input: MusicInput;
	audio_url?: string;
	error?: string;
	attempts: number;
	attempt_log: AttemptLog[];
	created_at: number;
	started_at?: number;
	completed_at?: number;
};

const FORMATS = new Set(["mp3", "wav"]);
const ATTEMPT_TIMEOUT_MS = 30 * 60 * 1000;

export class MusicJob extends DurableObject<Env> {
	async start(input: MusicInput): Promise<void> {
		const existing = await this.ctx.storage.get<JobRecord>("job");
		if (existing) return;

		const job: JobRecord = {
			state: "queued",
			input,
			attempts: 0,
			attempt_log: [],
			created_at: Date.now(),
		};
		await this.ctx.storage.put("job", job);
		await this.ctx.storage.setAlarm(Date.now() + 50);
	}

	async status(): Promise<JobRecord | null> {
		return (await this.ctx.storage.get<JobRecord>("job")) ?? null;
	}

	async alarm(): Promise<void> {
		const job = await this.ctx.storage.get<JobRecord>("job");
		if (!job) return;
		if (job.state === "complete" || job.state === "failed") return;
		// Already attempted — the previous alarm firing owns this generation.
		// This guards against duplicate runs if an alarm ever re-fires.
		if (job.attempts > 0) return;

		const gatewayId = this.env.AI_GATEWAY_ID;
		if (!gatewayId) {
			await this.finalize({ ...job, state: "failed", error: "AI_GATEWAY_ID not configured", completed_at: Date.now() });
			return;
		}

		const cacheKey = this.ctx.id.toString();
		const aiInput: Record<string, unknown> = {
			prompt: job.input.prompt,
			is_instrumental: job.input.is_instrumental,
			format: job.input.format,
			lyrics_optimizer: !job.input.is_instrumental && !job.input.lyrics,
		};
		if (job.input.lyrics && !job.input.is_instrumental) aiInput.lyrics = job.input.lyrics;

		const attemptStarted = Date.now();
		let current: JobRecord = {
			...job,
			state: "running",
			started_at: attemptStarted,
			attempts: 1,
		};
		await this.ctx.storage.put("job", current);

		let result: unknown;
		let errorMsg: string | undefined;
		try {
			result = await this.env.AI.run(
				"minimax/music-2.6" as never,
				aiInput as never,
				{
					gateway: {
						id: gatewayId,
						cacheKey,
						cacheTtl: 86400,
						requestTimeoutMs: ATTEMPT_TIMEOUT_MS,
						retries: { maxAttempts: 1 },
					},
					signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
				} as never,
			);
		} catch (err) {
			errorMsg = err instanceof Error ? err.message : "AI run failed";
		}

		const attemptEnded = Date.now();
		const logId = this.env.AI.aiGatewayLogId ?? undefined;
		const cacheHit = logId ? await this.inspectCacheStatus(logId) : false;
		const audio = extractAudioUrl(result);
		const hasAudio = typeof audio === "string" && audio.length > 0;

		const attemptLog: AttemptLog = {
			attempt: 1,
			started_at: attemptStarted,
			ended_at: attemptEnded,
			duration_ms: attemptEnded - attemptStarted,
			log_id: logId,
			cache_hit: cacheHit,
			error: hasAudio ? undefined : errorMsg ?? snippet(result),
			raw_response_snippet: hasAudio ? undefined : snippet(result),
		};
		current = { ...current, attempt_log: [...current.attempt_log, attemptLog] };
		console.log("MusicJob attempt", {
			cacheKey,
			logId,
			cacheHit,
			duration_ms: attemptLog.duration_ms,
			error: attemptLog.error,
		});

		if (hasAudio) {
			await this.finalize({
				...current,
				state: "complete",
				audio_url: audio as string,
				completed_at: attemptEnded,
			});
			return;
		}

		await this.finalize({
			...current,
			state: "failed",
			error: attemptLog.error ?? "Model returned no audio URL",
			completed_at: attemptEnded,
		});
	}

	private async finalize(job: JobRecord): Promise<void> {
		await this.ctx.storage.put("job", job);
		// Auto-clean after 1 hour; audio URL is already delivered to client by then.
		await this.ctx.storage.setAlarm(Date.now() + 60 * 60 * 1000);
	}

	private async inspectCacheStatus(logId: string): Promise<boolean> {
		try {
			const gateway = this.env.AI.gateway(this.env.AI_GATEWAY_ID);
			const log = await gateway.getLog(logId);
			// AI Gateway log exposes `cached: boolean`
			return Boolean((log as unknown as { cached?: boolean }).cached);
		} catch {
			return false;
		}
	}
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === "/api/generate" && request.method === "POST") {
			return handleGenerate(request, env, ctx);
		}
		const statusMatch = url.pathname.match(/^\/api\/status\/([A-Za-z0-9_-]+)$/);
		if (statusMatch && request.method === "GET") {
			return handleStatus(statusMatch[1], env);
		}
		const audioMatch = url.pathname.match(/^\/api\/audio\/([A-Za-z0-9_-]+)$/);
		if (audioMatch && request.method === "GET") {
			return handleAudio(audioMatch[1], env);
		}

		return env.ASSETS.fetch(request);
	},
} satisfies ExportedHandler<Env>;

async function handleGenerate(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
	let body: unknown;
	try {
		body = await request.json();
	} catch {
		return json({ error: "invalid JSON body" }, 400);
	}

	const input = parseInput(body);
	if ("error" in input) return json(input, 400);

	if (!env.AI_GATEWAY_ID) {
		return json({ error: "AI_GATEWAY_ID not configured" }, 500);
	}

	const jobId = crypto.randomUUID();
	const stub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(jobId));
	ctx.waitUntil(stub.start(input));
	return json({ jobId }, 202);
}

async function handleStatus(jobId: string, env: Env): Promise<Response> {
	const stub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(jobId));
	const record = await stub.status();
	if (!record) return json({ error: "job not found" }, 404);
	const { audio_url: _audio, ...publicRecord } = record;
	return json({ ...publicRecord, ready: record.state === "complete" });
}

async function handleAudio(jobId: string, env: Env): Promise<Response> {
	const stub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(jobId));
	const record = await stub.status();
	if (!record) return json({ error: "job not found" }, 404);
	if (record.state !== "complete" || !record.audio_url) {
		return json({ error: "job not ready", state: record.state }, 409);
	}

	const upstream = await fetch(record.audio_url);
	if (!upstream.ok || !upstream.body) {
		const text = await upstream.text().catch(() => "");
		return json(
			{ error: `upstream audio fetch failed: ${upstream.status}`, body: text.slice(0, 500) },
			502,
		);
	}

	const headers = new Headers();
	headers.set("Content-Type", record.input.format === "wav" ? "audio/wav" : "audio/mpeg");
	const len = upstream.headers.get("content-length");
	if (len) headers.set("Content-Length", len);
	headers.set("Accept-Ranges", "bytes");
	headers.set("Cache-Control", "no-store");

	return new Response(upstream.body, { status: 200, headers });
}

function parseInput(body: unknown): MusicInput | { error: string } {
	if (!body || typeof body !== "object") return { error: "body must be a JSON object" };
	const raw = body as Record<string, unknown>;
	const prompt = typeof raw.prompt === "string" ? raw.prompt.trim() : "";
	if (!prompt) return { error: "prompt is required" };
	if (prompt.length > 2000) return { error: "prompt must be <= 2000 chars" };

	const format = typeof raw.format === "string" && FORMATS.has(raw.format) ? (raw.format as "mp3" | "wav") : "mp3";
	const is_instrumental = raw.is_instrumental === true;
	const lyricsRaw = typeof raw.lyrics === "string" ? raw.lyrics.trim() : "";
	const lyrics = lyricsRaw ? lyricsRaw.slice(0, 3500) : undefined;

	const input: MusicInput = { prompt, is_instrumental, format };
	if (lyrics && !is_instrumental) input.lyrics = lyrics;
	return input;
}

function json(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

function extractAudioUrl(value: unknown): string | undefined {
	if (!value || typeof value !== "object") return undefined;
	const v = value as { audio?: unknown; result?: { audio?: unknown } };
	if (typeof v.audio === "string" && v.audio) return v.audio;
	if (v.result && typeof v.result === "object" && typeof v.result.audio === "string" && v.result.audio) {
		return v.result.audio;
	}
	return undefined;
}

function snippet(value: unknown): string | undefined {
	if (value === undefined || value === null) return undefined;
	try {
		const str = typeof value === "string" ? value : JSON.stringify(value);
		return str.length > 400 ? `${str.slice(0, 400)}…` : str;
	} catch {
		return String(value).slice(0, 400);
	}
}
