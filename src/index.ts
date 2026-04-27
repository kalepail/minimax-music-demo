import { DurableObject } from "cloudflare:workers";
import {
	ATTEMPT_TIMEOUT_MS,
	JOB_TTL_MS,
	RATE_LIMIT_WINDOW_MS,
	STALE_JOB_MS,
	applyRateLimit,
	audioObjectKey,
	audioResponseHeaders,
	clientRateLimitKey,
	extractAudioUrl,
	isExpiredRateLimit,
	json,
	parseRangeHeader,
	rangeNotSatisfiableHeaders,
	isStaleRunningJob,
	parseInput,
	publicStatus,
	shouldCleanUp,
	snippet,
	storedAudioResponseHeaders,
	storedAudioStatus,
	type AttemptLog,
	type JobRecord,
	type MusicInput,
	type RateLimitRecord,
	type StoredAudioRange,
} from "./lib";

export class MusicJob extends DurableObject<Env> {
	async start(input: MusicInput, jobId: string): Promise<void> {
		const existing = await this.ctx.storage.get<JobRecord>("job");
		if (existing) return;

		const job: JobRecord = {
			state: "queued",
			input,
			audio_object_key: audioObjectKey(jobId, input.format),
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

	async checkRateLimit(): Promise<{ allowed: boolean; remaining: number; retry_after_ms: number }> {
		const result = applyRateLimit(await this.ctx.storage.get<RateLimitRecord>("rate_limit"));
		await this.ctx.storage.put("rate_limit", result.record);
		await this.ctx.storage.setAlarm(result.record.window_start + RATE_LIMIT_WINDOW_MS);
		return {
			allowed: result.allowed,
			remaining: result.remaining,
			retry_after_ms: result.retry_after_ms,
		};
	}

	async alarm(): Promise<void> {
		const rateLimit = await this.ctx.storage.get<RateLimitRecord>("rate_limit");
		if (rateLimit && isExpiredRateLimit(rateLimit)) {
			await this.ctx.storage.deleteAll();
			return;
		}

		const job = await this.ctx.storage.get<JobRecord>("job");
		if (!job) return;
		if (job.state === "complete" || job.state === "failed") {
			if (shouldCleanUp(job)) {
				await this.deletePersistedAudio(job);
				await this.ctx.storage.deleteAll();
				return;
			}
			await this.ctx.storage.setAlarm(job.expires_at ?? Date.now());
			return;
		}
		if (isStaleRunningJob(job)) {
			const now = Date.now();
			const attemptLog: AttemptLog = {
				attempt: job.attempts,
				started_at: job.started_at ?? now,
				ended_at: now,
				duration_ms: job.started_at ? now - job.started_at : 0,
				error: "Generation timed out before completing",
			};
			await this.finalize({
				...job,
				state: "failed",
				error: attemptLog.error,
				attempt_log: job.attempt_log.length < job.attempts ? [...job.attempt_log, attemptLog] : job.attempt_log,
				completed_at: now,
			});
			return;
		}
		// One generation attempt per demo job. A duplicate alarm while the attempt
		// is still active should not start another model request.
		if (job.attempts > 0) return;

		const gatewayId = this.env.AI_GATEWAY_ID;
		if (!gatewayId) {
			await this.finalize({
				...job,
				state: "failed",
				error: "AI_GATEWAY_ID not configured",
				completed_at: Date.now(),
			});
			return;
		}

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
		await this.ctx.storage.setAlarm(attemptStarted + STALE_JOB_MS);

		let result: unknown;
		let errorMsg: string | undefined;
		try {
			result = await this.env.AI.run(
				"minimax/music-2.6",
				aiInput,
				{
					gateway: {
						id: gatewayId,
						requestTimeoutMs: ATTEMPT_TIMEOUT_MS,
						retries: { maxAttempts: 1 },
					},
					signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
				},
			);
		} catch (err) {
			errorMsg = err instanceof Error ? err.message : "AI run failed";
		}

		const attemptEnded = Date.now();
		const audio = extractAudioUrl(result);
		const hasAudio = typeof audio === "string" && audio.length > 0;
		let persistedAudio: { contentType: string; key: string } | undefined;
		if (hasAudio) {
			try {
				persistedAudio = await this.persistAudio(current, audio as string);
			} catch (err) {
				errorMsg = err instanceof Error ? err.message : "Audio persistence failed";
			}
		}

		const attemptLog: AttemptLog = {
			attempt: 1,
			started_at: attemptStarted,
			ended_at: attemptEnded,
			duration_ms: attemptEnded - attemptStarted,
			error: persistedAudio ? undefined : errorMsg ?? snippet(result),
		};
		current = { ...current, attempt_log: [...current.attempt_log, attemptLog] };
		console.log("MusicJob attempt", {
			duration_ms: attemptLog.duration_ms,
			error: attemptLog.error,
		});

		if (persistedAudio) {
			await this.finalize({
				...current,
				state: "complete",
				audio_url: audio as string,
				audio_object_key: persistedAudio.key,
				audio_content_type: persistedAudio.contentType,
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
		const expiresAt = Date.now() + JOB_TTL_MS;
		await this.ctx.storage.put("job", { ...job, expires_at: expiresAt });
		await this.ctx.storage.setAlarm(expiresAt);
	}

	private async persistAudio(job: JobRecord, audioUrl: string): Promise<{ contentType: string; key: string }> {
		const upstream = await fetch(audioUrl);
		if (!upstream.ok || !upstream.body) {
			throw new Error(`upstream audio fetch failed before persistence: ${upstream.status}`);
		}

		const contentType = upstream.headers.get("content-type") ?? (job.input.format === "wav" ? "audio/wav" : "audio/mpeg");
		const key = job.audio_object_key ?? audioObjectKey(crypto.randomUUID(), job.input.format);
		await this.env.AUDIO_BUCKET.put(key, upstream.body, {
			httpMetadata: {
				cacheControl: "no-store",
				contentType,
			},
		});
		return { contentType, key };
	}

	private async deletePersistedAudio(job: JobRecord): Promise<void> {
		if (!job.audio_object_key) return;
		try {
			await this.env.AUDIO_BUCKET.delete(job.audio_object_key);
		} catch (err) {
			console.warn("Failed to delete persisted audio", {
				error: err instanceof Error ? err.message : String(err),
				key: job.audio_object_key,
			});
		}
	}
}

export default {
	async fetch(request, env): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === "/api/generate" && request.method === "POST") {
			return handleGenerate(request, env);
		}
		const statusMatch = url.pathname.match(/^\/api\/status\/([A-Za-z0-9_-]+)$/);
		if (statusMatch && request.method === "GET") {
			return handleStatus(statusMatch[1], env);
		}
		const audioMatch = url.pathname.match(/^\/api\/audio\/([A-Za-z0-9_-]+)$/);
		if (audioMatch && (request.method === "GET" || request.method === "HEAD")) {
			return handleAudio(request, audioMatch[1], env);
		}

		return env.ASSETS.fetch(request);
	},
} satisfies ExportedHandler<Env>;

async function handleGenerate(request: Request, env: Env): Promise<Response> {
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

	const rateStub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(clientRateLimitKey(request)));
	const rateLimit = await rateStub.checkRateLimit();
	if (!rateLimit.allowed) {
		const retryAfterSeconds = Math.ceil(rateLimit.retry_after_ms / 1000);
		return json(
			{
				error: "rate limit exceeded",
				retry_after_seconds: retryAfterSeconds,
			},
			429,
			{ "Retry-After": String(retryAfterSeconds) },
		);
	}

	const jobId = crypto.randomUUID();
	const stub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(jobId));
	await stub.start(input, jobId);
	return json({ jobId }, 202);
}

async function handleStatus(jobId: string, env: Env): Promise<Response> {
	const stub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(jobId));
	const record = await stub.status();
	if (!record) return json({ error: "job not found" }, 404);
	return json(publicStatus(record));
}

async function handleAudio(request: Request, jobId: string, env: Env): Promise<Response> {
	const stub = env.MUSIC_JOB.get(env.MUSIC_JOB.idFromName(jobId));
	const record = await stub.status();
	if (!record) return json({ error: "job not found" }, 404);
	if (record.state !== "complete") {
		return json({ error: "job not ready", state: record.state }, 409);
	}

	if (record.audio_object_key) {
		const rangeHeader = request.headers.get("Range");
		let range: StoredAudioRange | undefined;
		if (rangeHeader) {
			const head = await env.AUDIO_BUCKET.head(record.audio_object_key);
			if (!head) return json({ error: "audio not found" }, 404);
			const parsedRange = parseRangeHeader(rangeHeader, head.size);
			if (parsedRange && "error" in parsedRange) {
				return new Response(null, {
					status: 416,
					headers: rangeNotSatisfiableHeaders(head.size),
				});
			}
			range = parsedRange;
		}

		const object = await env.AUDIO_BUCKET.get(record.audio_object_key, range ? { range: range.r2Range } : undefined);
		if (object?.body) {
			return new Response(request.method === "HEAD" ? null : object.body, {
				status: storedAudioStatus(range),
				headers: storedAudioResponseHeaders(record, object, range),
			});
		}
	}

	if (!record.audio_url) return json({ error: "audio not found" }, 404);

	const upstreamHeaders = new Headers();
	const range = request.headers.get("Range");
	if (range) upstreamHeaders.set("Range", range);

	const upstream = await fetch(record.audio_url, { headers: upstreamHeaders });
	if (upstream.status === 416) {
		return new Response(request.method === "HEAD" ? null : upstream.body, {
			status: 416,
			headers: audioResponseHeaders(record, upstream),
		});
	}
	if (!upstream.ok || !upstream.body) {
		const text = await upstream.text().catch(() => "");
		return json(
			{ error: `upstream audio fetch failed: ${upstream.status}`, body: text.slice(0, 500) },
			502,
		);
	}

	return new Response(request.method === "HEAD" ? null : upstream.body, {
		status: upstream.status === 206 ? 206 : 200,
		headers: audioResponseHeaders(record, upstream),
	});
}
