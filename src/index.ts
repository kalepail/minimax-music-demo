import { DurableObject } from "cloudflare:workers";
import {
	ATTEMPT_TIMEOUT_MS,
	JOB_TTL_MS,
	RATE_LIMIT_WINDOW_MS,
	RADIO_IN_FLIGHT_STALE_MS,
	RADIO_MAX_QUEUE_ATTEMPTS,
	RADIO_MAX_FULFILLED_REQUESTS,
	RADIO_MAX_PLAYLIST,
	RADIO_MAX_REQUESTS,
	RADIO_STATION_ID,
	RADIO_TARGET_BACKLOG,
	RADIO_TEXT_MODEL,
	STALE_JOB_MS,
	applyRateLimit,
	audioObjectKey,
	audioResponseHeaders,
	clientRateLimitKey,
	extractAudioUrl,
	extractTextResponse,
	isExpiredRateLimit,
	json,
	parseRangeHeader,
	rangeNotSatisfiableHeaders,
	isStaleRunningJob,
	normalizeBoundedInt,
	normalizeFacet,
	normalizeTags,
	parseLibraryQuery,
	parseInput,
	parseRadioRequest,
	parseStationParams,
	publicStatus,
	radioAudioObjectKey,
	radioMetadataObjectKey,
	shouldCleanUp,
	snippet,
	stationName,
	storedAudioResponseHeaders,
	storedAudioStatus,
	type AttemptLog,
	type JobRecord,
	type LibraryQuery,
	type MusicInput,
	type RateLimitRecord,
	type RadioGenerateMessage,
	type FulfilledRadioRequest,
	type RadioInFlight,
	type RadioPromptPlan,
	type RadioRequest,
	type RadioSong,
	type RadioStationRecord,
	type RadioStatus,
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

export class RadioStation extends DurableObject<Env> {
	async status(): Promise<RadioStatus> {
		const { playlist, requests, fulfilled } = await this.reconcileRequests();
		const inFlight = await this.inFlight();
		return {
			playlist,
			fulfilled_requests: fulfilled,
			requests,
			in_flight: inFlight,
			target_backlog: RADIO_TARGET_BACKLOG,
		};
	}

	async request(text: string, stationId = RADIO_STATION_ID, genre?: string): Promise<{ request: RadioRequest; queued: number }> {
		const request: RadioRequest = {
			id: crypto.randomUUID(),
			text,
			created_at: Date.now(),
		};
		const requests = [request, ...(await this.requests())].slice(0, RADIO_MAX_REQUESTS);
		await this.ctx.storage.put("requests", requests);
		const queued = await this.fill(RADIO_TARGET_BACKLOG, stationId, genre);
		return { request, queued };
	}

	async fill(target = RADIO_TARGET_BACKLOG, stationId = RADIO_STATION_ID, genre?: string): Promise<number> {
		const now = Date.now();
		const requests = [...(await this.requests())];
		const freshInFlight = (await this.inFlight()).filter((item) => now - item.queued_at < RADIO_IN_FLIGHT_STALE_MS);
		const needed = Math.max(0, target - freshInFlight.length);
		if (needed === 0) {
			if (freshInFlight.length !== (await this.inFlight()).length) {
				await this.ctx.storage.put("in_flight", freshInFlight);
			}
			return 0;
		}

		const nextInFlight = [...freshInFlight];
		const messages: MessageSendRequest<RadioGenerateMessage>[] = [];
		for (let i = 0; i < needed; i++) {
			const request = requests.shift();
			const songId = crypto.randomUUID();
			const queuedAt = Date.now();
			const creative = creativeDirection(songId, i, genre, request?.text);
			const body: RadioGenerateMessage = {
				song_id: songId,
				station_id: stationId,
				format: "mp3",
				request_id: request?.id,
				request_created_at: request?.created_at,
				request_text: request?.text,
				genre,
				creative_seed: creative.seed,
				creative_axis: creative.axis,
				creative_bpm: creative.bpm,
				queued_at: queuedAt,
			};
			messages.push({ body });
			nextInFlight.push({
				song_id: songId,
				queued_at: queuedAt,
				creative_seed: creative.seed,
				request_id: request?.id,
				request_created_at: request?.created_at,
				request_text: request?.text,
			});
		}

		await this.env.RADIO_QUEUE.sendBatch(messages);
		await Promise.all([
			this.ctx.storage.put("in_flight", nextInFlight),
			this.ctx.storage.put("requests", requests),
		]);
		return messages.length;
	}

	async complete(song: RadioSong): Promise<void> {
		const playlist = [song, ...(await this.playlist()).filter((item) => item.id !== song.id)].slice(0, RADIO_MAX_PLAYLIST);
		const existingInFlight = await this.inFlight();
		const completedInFlight = existingInFlight.find((item) => item.song_id === song.id);
		const inFlight = existingInFlight.filter((item) => item.song_id !== song.id);
		const fulfilled = await this.fulfilledRequests();
		const fulfilledRequest = requestFulfillment(song, completedInFlight);
		const nextFulfilled = fulfilledRequest
			? [fulfilledRequest, ...fulfilled.filter((item) => item.id !== fulfilledRequest.id)].slice(0, RADIO_MAX_FULFILLED_REQUESTS)
			: fulfilled;
		await Promise.all([
			this.ctx.storage.put("playlist", playlist),
			this.ctx.storage.put("in_flight", inFlight),
			this.ctx.storage.put("fulfilled_requests", nextFulfilled),
		]);
	}

	async noteFailure(songId: string, error: string): Promise<void> {
		const failures = (await this.ctx.storage.get<Array<{ song_id: string; error: string; failed_at: number }>>("failures")) ?? [];
		await this.ctx.storage.put("failures", [{ song_id: songId, error, failed_at: Date.now() }, ...failures].slice(0, 50));
	}

	async fail(songId: string, error: string): Promise<void> {
		const existingInFlight = await this.inFlight();
		const failed = existingInFlight.find((item) => item.song_id === songId);
		const inFlight = existingInFlight.filter((item) => item.song_id !== songId);
		const requests = failed?.request_id && failed.request_text
			? requeueRequest(await this.requests(), failed)
			: await this.requests();
		await Promise.all([
			this.ctx.storage.put("in_flight", inFlight),
			this.ctx.storage.put("requests", requests),
			this.noteFailure(songId, error),
		]);
	}

	private async reconcileRequests(): Promise<{ playlist: RadioSong[]; requests: RadioRequest[]; fulfilled: FulfilledRadioRequest[] }> {
		const [playlist, requests, fulfilled] = await Promise.all([
			this.playlist(),
			this.requests(),
			this.fulfilledRequests(),
		]);
		if (requests.length === 0) return { playlist, requests, fulfilled };

		const nextRequests = [...requests];
		const nextFulfilled = [...fulfilled];
		for (const request of requests) {
			const song = playlist.find((item) => item.request_id === request.id || item.request_text === request.text);
			if (!song) continue;
			const index = nextRequests.findIndex((item) => item.id === request.id);
			if (index >= 0) nextRequests.splice(index, 1);
			if (!nextFulfilled.some((item) => item.id === request.id)) {
				nextFulfilled.unshift({
					...request,
					fulfilled_at: song.completed_at,
					song_id: song.id,
					song_title: song.title,
				});
			}
		}

		if (nextRequests.length !== requests.length || nextFulfilled.length !== fulfilled.length) {
			await Promise.all([
				this.ctx.storage.put("requests", nextRequests),
				this.ctx.storage.put("fulfilled_requests", nextFulfilled.slice(0, RADIO_MAX_FULFILLED_REQUESTS)),
			]);
		}
		return { playlist, requests: nextRequests, fulfilled: nextFulfilled.slice(0, RADIO_MAX_FULFILLED_REQUESTS) };
	}

	private async playlist(): Promise<RadioSong[]> {
		return (await this.ctx.storage.get<RadioSong[]>("playlist")) ?? [];
	}

	private async requests(): Promise<RadioRequest[]> {
		return (await this.ctx.storage.get<RadioRequest[]>("requests")) ?? [];
	}

	private async fulfilledRequests(): Promise<FulfilledRadioRequest[]> {
		return (await this.ctx.storage.get<FulfilledRadioRequest[]>("fulfilled_requests")) ?? [];
	}

	private async inFlight(): Promise<RadioInFlight[]> {
		return (await this.ctx.storage.get<RadioInFlight[]>("in_flight")) ?? [];
	}
}

function requestFulfillment(song: RadioSong, inFlight?: RadioInFlight): FulfilledRadioRequest | undefined {
	const requestId = song.request_id ?? inFlight?.request_id;
	const requestText = song.request_text ?? inFlight?.request_text;
	if (!requestId || !requestText) return undefined;
	return {
		id: requestId,
		text: requestText,
		created_at: inFlight?.request_created_at ?? song.created_at,
		fulfilled_at: song.completed_at,
		song_id: song.id,
		song_title: song.title,
	};
}

function requeueRequest(requests: RadioRequest[], inFlight: RadioInFlight): RadioRequest[] {
	if (!inFlight.request_id || !inFlight.request_text) return requests;
	if (requests.some((request) => request.id === inFlight.request_id)) return requests;
	return [
		{
			id: inFlight.request_id,
			text: inFlight.request_text,
			created_at: inFlight.request_created_at ?? inFlight.queued_at,
		},
		...requests,
	].slice(0, RADIO_MAX_REQUESTS);
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
		if (url.pathname === "/api/radio/status" && request.method === "GET") {
			return handleRadioStatus(env, request);
		}
		if (url.pathname === "/api/radio/request" && request.method === "POST") {
			return handleRadioRequest(request, env);
		}
		if (url.pathname === "/api/radio/fill" && request.method === "POST") {
			return handleRadioFill(request, env);
		}
		if (url.pathname === "/api/radio/stations" && request.method === "GET") {
			return handleRadioStations(env);
		}
		if (url.pathname === "/api/library" && request.method === "GET") {
			return handleLibrary(url, env);
		}
		const librarySongMatch = url.pathname.match(/^\/api\/library\/([A-Za-z0-9_-]+)$/);
		if (librarySongMatch && request.method === "GET") {
			return handleLibrarySong(librarySongMatch[1], env);
		}
		const radioAudioMatch = url.pathname.match(/^\/api\/radio\/audio\/([A-Za-z0-9_-]+)$/);
		if (radioAudioMatch && (request.method === "GET" || request.method === "HEAD")) {
			return handleRadioAudio(request, radioAudioMatch[1], env);
		}

		return env.ASSETS.fetch(request);
	},

	async scheduled(_controller, env, ctx): Promise<void> {
		if (env.RADIO_AUTOFILL !== "true") return;
		ctx.waitUntil(radioStation(env).fill(RADIO_TARGET_BACKLOG, RADIO_STATION_ID));
	},

	async queue(batch, env): Promise<void> {
		for (const message of batch.messages) {
			const body = normalizeRadioGenerateMessage(message.body as Partial<RadioGenerateMessage>);
			try {
				await generateRadioSong(body, env);
				message.ack();
			} catch (err) {
				const error = err instanceof Error ? err.message : String(err);
				if (message.attempts >= RADIO_MAX_QUEUE_ATTEMPTS) {
					await radioStation(env, body.station_id).fail(body.song_id, error);
					message.ack();
				} else {
					await radioStation(env, body.station_id).noteFailure(body.song_id, error);
					message.retry({ delaySeconds: 30 });
				}
			}
		}
	},
} satisfies ExportedHandler<Env>;

function normalizeRadioGenerateMessage(input: Partial<RadioGenerateMessage>): RadioGenerateMessage {
	const songId = typeof input.song_id === "string" && input.song_id ? input.song_id : crypto.randomUUID();
	const stationId = typeof input.station_id === "string" && input.station_id ? input.station_id : RADIO_STATION_ID;
	const format = input.format === "wav" ? "wav" : "mp3";
	const genre = normalizeFacet(input.genre);
	const requestText = typeof input.request_text === "string" && input.request_text.trim() ? input.request_text.trim() : undefined;
	const creative = creativeDirection(songId, 0, genre, requestText);
	return {
		song_id: songId,
		station_id: stationId,
		format,
		request_text: requestText,
		request_id: typeof input.request_id === "string" && input.request_id ? input.request_id : undefined,
		request_created_at: typeof input.request_created_at === "number" ? input.request_created_at : undefined,
		genre,
		creative_seed: typeof input.creative_seed === "string" && input.creative_seed ? input.creative_seed : creative.seed,
		creative_axis: typeof input.creative_axis === "string" && input.creative_axis ? input.creative_axis : creative.axis,
		creative_bpm: normalizeBoundedInt(input.creative_bpm, 40, 240) ?? creative.bpm,
		queued_at: typeof input.queued_at === "number" ? input.queued_at : Date.now(),
	};
}

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

function radioStation(env: Env, stationId = RADIO_STATION_ID): DurableObjectStub<RadioStation> {
	return env.RADIO_STATION.get(env.RADIO_STATION.idFromName(stationId));
}

async function handleRadioStatus(env: Env, request?: Request): Promise<Response> {
	const url = request ? new URL(request.url) : new URL("https://local/");
	const station = parseStationParams(url);
	if ("error" in station) return json(station, 400);
	const status = await radioStation(env, station.station_id).status();
	return json({ ...status, station_id: station.station_id, genre: station.genre });
}

async function handleRadioRequest(request: Request, env: Env): Promise<Response> {
	let body: unknown;
	try {
		body = await request.json();
	} catch {
		return json({ error: "invalid JSON body" }, 400);
	}
	const input = parseRadioRequest(body);
	if ("error" in input) return json(input, 400);
	const station = parseStationParams(new URL(request.url), body);
	if ("error" in station) return json(station, 400);
	return json(await radioStation(env, station.station_id).request(input.text, station.station_id, station.genre), 202);
}

async function handleRadioFill(request: Request, env: Env): Promise<Response> {
	let target = RADIO_TARGET_BACKLOG;
	let body: unknown;
	if (request.headers.get("Content-Type")?.includes("application/json")) {
		body = await request.json().catch(() => undefined);
		if (body && typeof body === "object" && typeof (body as { target?: unknown }).target === "number") {
			target = Math.max(1, Math.min(25, Math.floor((body as { target: number }).target)));
		}
	}
	const station = parseStationParams(new URL(request.url), body);
	if ("error" in station) return json(station, 400);
	const queued = await radioStation(env, station.station_id).fill(target, station.station_id, station.genre);
	return json({ queued, target, station_id: station.station_id, genre: station.genre }, queued > 0 ? 202 : 200);
}

async function handleRadioStations(env: Env): Promise<Response> {
	const [stations, genres] = await Promise.all([
		env.DB.prepare(
			`SELECT id, name, description, genre_filter, created_at, updated_at
			 FROM stations
			 ORDER BY updated_at DESC
			 LIMIT 100`,
		).all<RadioStationRecord>(),
		env.DB.prepare(
			`SELECT primary_genre AS genre, COUNT(*) AS count
			 FROM songs
			 WHERE primary_genre IS NOT NULL AND primary_genre != ''
			 GROUP BY primary_genre
			 ORDER BY count DESC, primary_genre ASC
			 LIMIT 100`,
		).all<{ genre: string; count: number }>(),
	]);

	return json({
		stations: stations.results ?? [],
		genres: genres.results ?? [],
	});
}

async function handleLibrary(url: URL, env: Env): Promise<Response> {
	const query = parseLibraryQuery(url);
	if ("error" in query) return json(query, 400);
	const result = await listSongs(env.DB, query);
	return json(result);
}

async function handleLibrarySong(songId: string, env: Env): Promise<Response> {
	const row = await env.DB.prepare(
		`SELECT id, station_id, title, prompt, request_id, request_text, format, audio_object_key, metadata_object_key,
			audio_content_type, primary_genre, mood, energy, bpm_min, bpm_max, vocal_style,
			created_at, completed_at, duration_ms
		 FROM songs
		 WHERE id = ?`,
	).bind(songId).first<SongRow>();
	if (!row) return json({ error: "song not found" }, 404);
	const tags = await loadSongTags(env.DB, [songId]);
	return json(songFromRow(row, tags.get(songId) ?? []));
}

async function handleRadioAudio(request: Request, songId: string, env: Env): Promise<Response> {
	const key = radioAudioObjectKey(songId, "mp3");
	const rangeHeader = request.headers.get("Range");
	let range: StoredAudioRange | undefined;
	if (rangeHeader) {
		const head = await env.AUDIO_BUCKET.head(key);
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

	const object = await env.AUDIO_BUCKET.get(key, range ? { range: range.r2Range } : undefined);
	if (!object?.body) return json({ error: "audio not found" }, 404);

	const headers = new Headers();
	object.writeHttpMetadata(headers);
	headers.set("Content-Type", object.httpMetadata?.contentType ?? "audio/mpeg");
	headers.set("Cache-Control", "public, max-age=3600");
	headers.set("ETag", object.httpEtag);
	headers.set("Accept-Ranges", "bytes");
	headers.set("Content-Length", String(range ? range.end - range.start + 1 : object.size));
	if (range) headers.set("Content-Range", `bytes ${range.start}-${range.end}/${range.total}`);

	return new Response(request.method === "HEAD" ? null : object.body, {
		status: storedAudioStatus(range),
		headers,
	});
}

type SongRow = {
	id: string;
	station_id: string;
	title: string;
	prompt: string;
	request_id: string | null;
	request_text: string | null;
	format: MusicInput["format"];
	audio_object_key: string;
	metadata_object_key: string;
	audio_content_type: string;
	primary_genre: string | null;
	mood: string | null;
	energy: number | null;
	bpm_min: number | null;
	bpm_max: number | null;
	vocal_style: string | null;
	created_at: number;
	completed_at: number;
	duration_ms: number;
};

async function listSongs(db: D1Database, query: LibraryQuery): Promise<{ songs: RadioSong[]; next_cursor?: string; limit: number }> {
	const where: string[] = [];
	const params: Array<number | string> = [];
	if (query.station_id) {
		where.push("s.station_id = ?");
		params.push(query.station_id);
	}
	if (query.genre) {
		where.push("s.primary_genre = ?");
		params.push(query.genre);
	}
	if (query.mood) {
		where.push("s.mood = ?");
		params.push(query.mood);
	}
	if (query.tag) {
		where.push("EXISTS (SELECT 1 FROM song_tags st WHERE st.song_id = s.id AND st.tag = ?)");
		params.push(query.tag);
	}

	const orderBy = songOrderBy(query.sort);
	const whereSql = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
	const rows = await db.prepare(
		`SELECT s.id, s.station_id, s.title, s.prompt, s.request_text, s.format, s.audio_object_key,
			s.request_id, s.metadata_object_key, s.audio_content_type, s.primary_genre, s.mood, s.energy, s.bpm_min,
			s.bpm_max, s.vocal_style, s.created_at, s.completed_at, s.duration_ms
		 FROM songs s
		 ${whereSql}
		 ORDER BY ${orderBy}
		 LIMIT ? OFFSET ?`,
	).bind(...params, query.limit + 1, query.cursor).all<SongRow>();

	const page = (rows.results ?? []).slice(0, query.limit);
	const tags = await loadSongTags(db, page.map((row) => row.id));
	return {
		songs: page.map((row) => songFromRow(row, tags.get(row.id) ?? [])),
		next_cursor: (rows.results ?? []).length > query.limit ? String(query.cursor + query.limit) : undefined,
		limit: query.limit,
	};
}

function songOrderBy(sort: LibraryQuery["sort"]): string {
	switch (sort) {
		case "oldest":
			return "s.completed_at ASC, s.id ASC";
		case "title":
			return "s.title COLLATE NOCASE ASC, s.completed_at DESC, s.id DESC";
		case "energy":
			return "s.energy DESC NULLS LAST, s.completed_at DESC, s.id DESC";
		case "newest":
		default:
			return "s.completed_at DESC, s.id DESC";
	}
}

async function loadSongTags(db: D1Database, songIds: string[]): Promise<Map<string, string[]>> {
	if (songIds.length === 0) return new Map();
	const placeholders = songIds.map(() => "?").join(", ");
	const rows = await db.prepare(
		`SELECT song_id, tag
		 FROM song_tags
		 WHERE song_id IN (${placeholders})
		 ORDER BY tag ASC`,
	).bind(...songIds).all<{ song_id: string; tag: string }>();
	const tags = new Map<string, string[]>();
	for (const row of rows.results ?? []) {
		const list = tags.get(row.song_id) ?? [];
		list.push(row.tag);
		tags.set(row.song_id, list);
	}
	return tags;
}

function songFromRow(row: SongRow, tags: string[]): RadioSong {
	return {
		id: row.id,
		station_id: row.station_id,
		title: row.title,
		prompt: row.prompt,
		request_id: row.request_id ?? undefined,
		request_text: row.request_text ?? undefined,
		format: row.format,
		audio_object_key: row.audio_object_key,
		metadata_object_key: row.metadata_object_key,
		audio_content_type: row.audio_content_type,
		primary_genre: row.primary_genre ?? undefined,
		tags,
		mood: row.mood ?? undefined,
		energy: row.energy ?? undefined,
		bpm_min: row.bpm_min ?? undefined,
		bpm_max: row.bpm_max ?? undefined,
		vocal_style: row.vocal_style ?? undefined,
		created_at: row.created_at,
		completed_at: row.completed_at,
		duration_ms: row.duration_ms,
	};
}

async function persistSongCatalog(db: D1Database, song: RadioSong): Promise<void> {
	const now = Date.now();
	const tags = normalizeTags(song.tags);
	const station: RadioStationRecord = {
		id: song.station_id,
		name: stationName(song.station_id, song.primary_genre),
		description: song.primary_genre ? `Continuous ${song.primary_genre} radio generated from listener requests.` : "Continuous AI radio generated from listener requests.",
		genre_filter: song.station_id.startsWith("genre:") ? song.primary_genre : undefined,
		created_at: now,
		updated_at: now,
	};
	const statements = [
		db.prepare(
			`INSERT INTO songs (
				id, station_id, title, prompt, request_id, request_text, format, audio_object_key, metadata_object_key,
				audio_content_type, primary_genre, mood, energy, bpm_min, bpm_max, vocal_style,
				created_at, completed_at, duration_ms
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				station_id = excluded.station_id,
				title = excluded.title,
				prompt = excluded.prompt,
				request_id = excluded.request_id,
				request_text = excluded.request_text,
				format = excluded.format,
				audio_object_key = excluded.audio_object_key,
				metadata_object_key = excluded.metadata_object_key,
				audio_content_type = excluded.audio_content_type,
				primary_genre = excluded.primary_genre,
				mood = excluded.mood,
				energy = excluded.energy,
				bpm_min = excluded.bpm_min,
				bpm_max = excluded.bpm_max,
				vocal_style = excluded.vocal_style,
				created_at = excluded.created_at,
				completed_at = excluded.completed_at,
				duration_ms = excluded.duration_ms`,
		).bind(
			song.id,
			song.station_id,
			song.title,
			song.prompt,
			song.request_id ?? null,
			song.request_text ?? null,
			song.format,
			song.audio_object_key,
			song.metadata_object_key,
			song.audio_content_type,
			song.primary_genre ?? null,
			song.mood ?? null,
			song.energy ?? null,
			song.bpm_min ?? null,
			song.bpm_max ?? null,
			song.vocal_style ?? null,
			song.created_at,
			song.completed_at,
			song.duration_ms,
		),
		db.prepare("DELETE FROM song_tags WHERE song_id = ?").bind(song.id),
		db.prepare(
			`INSERT INTO stations (id, name, description, genre_filter, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
				name = excluded.name,
				description = excluded.description,
				genre_filter = excluded.genre_filter,
				updated_at = excluded.updated_at`,
		).bind(
			station.id,
			station.name,
			station.description ?? null,
			station.genre_filter ?? null,
			station.created_at,
			station.updated_at,
		),
	];

	for (const tag of tags) {
		statements.push(db.prepare("INSERT OR IGNORE INTO song_tags (song_id, tag) VALUES (?, ?)").bind(song.id, tag));
	}
	await db.batch(statements);
}

async function generateRadioSong(message: RadioGenerateMessage, env: Env): Promise<void> {
	const started = Date.now();
	const station = radioStation(env, message.station_id);
	try {
		const metadataObjectKey = radioMetadataObjectKey(message.song_id);
		const existing = await env.AUDIO_BUCKET.get(metadataObjectKey);
		if (existing) {
			const song = normalizeStoredRadioSong(await existing.json<Partial<RadioSong>>(), message);
			await persistSongCatalog(env.DB, song);
			await station.complete(song);
			return;
		}

		const plan = await createRadioPrompt(message, env).catch((err) => {
			console.warn("Radio prompt expansion failed; using fallback prompt", {
				error: err instanceof Error ? err.message : String(err),
				song_id: message.song_id,
			});
			return fallbackRadioPrompt(message);
		});
		const aiInput: Record<string, unknown> = {
			prompt: plan.prompt,
			is_instrumental: false,
			format: message.format,
			lyrics_optimizer: true,
		};
		const result = await env.AI.run(
			"minimax/music-2.6",
			aiInput,
			{
				gateway: {
					id: env.AI_GATEWAY_ID,
					requestTimeoutMs: ATTEMPT_TIMEOUT_MS,
					retries: { maxAttempts: 1 },
				},
				signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
			},
		);

		const audio = extractAudioUrl(result);
		if (!audio) throw new Error(snippet(result) ?? "Model returned no audio URL");

		const upstream = await fetch(audio);
		if (!upstream.ok || !upstream.body) {
			throw new Error(`upstream audio fetch failed before persistence: ${upstream.status}`);
		}

		const audioObjectKey = radioAudioObjectKey(message.song_id, message.format);
		const contentType = upstream.headers.get("content-type") ?? "audio/mpeg";
		await env.AUDIO_BUCKET.put(audioObjectKey, upstream.body, {
			httpMetadata: {
				cacheControl: "public, max-age=3600",
				contentType,
			},
			customMetadata: {
				station: message.station_id,
				title: plan.title,
			},
		});

		const completed = Date.now();
		const song: RadioSong = {
			id: message.song_id,
			station_id: message.station_id,
			title: plan.title,
			prompt: plan.prompt,
			request_id: message.request_id,
			request_text: message.request_text,
			format: message.format,
			audio_object_key: audioObjectKey,
			metadata_object_key: metadataObjectKey,
			audio_content_type: contentType,
			primary_genre: plan.primary_genre,
			tags: plan.tags,
			mood: plan.mood,
			energy: plan.energy,
			bpm_min: plan.bpm_min,
			bpm_max: plan.bpm_max,
			vocal_style: plan.vocal_style,
			created_at: message.queued_at,
			completed_at: completed,
			duration_ms: completed - started,
		};
		await env.AUDIO_BUCKET.put(metadataObjectKey, JSON.stringify(song, null, 2), {
			httpMetadata: {
				cacheControl: "public, max-age=3600",
				contentType: "application/json",
			},
		});
		await persistSongCatalog(env.DB, song);
		await station.complete(song);
	} catch (err) {
		throw err;
	}
}

async function createRadioPrompt(
	message: RadioGenerateMessage,
	env: Env,
): Promise<RadioPromptPlan> {
	const genreLane = message.genre ? `This is for the "${message.genre}" genre radio lane. Keep it recognizably in that lane while still making it surprising.` : "";
	const recentTitles = await recentSongTitles(env.DB, message.station_id);
	const recentTitleInstruction = recentTitles.length > 0 ? `Recent titles to avoid: ${recentTitles.join(", ")}.` : "";
	const seed = message.request_text
		? `A listener requested: "${message.request_text}". Interpret it creatively without copying copyrighted lyrics or imitating a living artist exactly.`
		: "No listener request is active. Invent a vivid left-field song concept fit for a strange internet radio station.";
	const fallback = fallbackRadioPrompt(message);

	const response = await env.AI.run(
		RADIO_TEXT_MODEL,
		{
			messages: [
				{
					role: "system",
					content:
						"You are a music director for an always-on AI radio station. Return compact JSON only. Required string fields: title, prompt. Optional catalog fields: primary_genre, tags array, mood, energy 1-10, bpm_min, bpm_max, vocal_style. Every song must feel meaningfully different from adjacent generations. The title must be original, specific, and not reused. The prompt must be original, richly musical, and suitable for a text-to-music model.",
				},
				{
					role: "user",
					content: `${seed}
${genreLane}
Unique creative seed: ${message.creative_seed}
Mandatory contrast axis: ${message.creative_axis}
Tempo center: around ${message.creative_bpm} BPM
Song ID entropy: ${message.song_id}
${recentTitleInstruction}

Create one surprising station-ready song concept. Avoid references to specific copyrighted songs or direct artist imitation. Do not use generic titles like Echoflux, Open Frequency, Untitled Signal, or Signal Drift. Keep the prompt under 1200 characters. Add useful genre/tags/mood metadata for library filtering.`,
				},
			],
		},
		{
			gateway: {
				id: env.AI_GATEWAY_ID,
				requestTimeoutMs: 30_000,
				retries: { maxAttempts: 1 },
			},
			signal: AbortSignal.timeout(30_000),
		},
	);

	const text = extractTextResponse(response);
	if (!text) return fallback;
	const parsed = parsePromptPlan(text);
	const title = distinctRadioTitle(parsed.title || fallback.title, recentTitles, message);
	return {
		title,
		prompt: parsed.prompt || fallback.prompt,
		primary_genre: parsed.primary_genre || fallback.primary_genre,
		tags: parsed.tags.length > 0 ? parsed.tags : fallback.tags,
		mood: parsed.mood || fallback.mood,
		energy: parsed.energy ?? fallback.energy,
		bpm_min: parsed.bpm_min ?? fallback.bpm_min,
		bpm_max: parsed.bpm_max ?? fallback.bpm_max,
		vocal_style: parsed.vocal_style || fallback.vocal_style,
	};
}

async function recentSongTitles(db: D1Database, stationId: string): Promise<string[]> {
	const rows = await db.prepare(
		`SELECT title
		 FROM songs
		 WHERE station_id = ?
		 ORDER BY completed_at DESC
		 LIMIT 25`,
	).bind(stationId).all<{ title: string }>();
	return (rows.results ?? []).map((row) => row.title).filter(Boolean);
}

function distinctRadioTitle(title: string, recentTitles: string[], message: RadioGenerateMessage): string {
	const normalized = title.trim().replace(/\s+/g, " ") || fallbackTitle(message);
	const lower = normalized.toLowerCase();
	const isRecent = recentTitles.some((recent) => recent.toLowerCase() === lower);
	const isGeneric = ["echoflux", "open frequency", "untitled signal", "signal drift"].includes(lower);
	const isOneWord = normalized.split(/\s+/).length < 2;
	if (!isRecent && !isGeneric && !isOneWord) return normalized.slice(0, 120);
	const suffix = titleCaseWords(seedWord(message.creative_seed, isRecent ? 1 : 0));
	const expanded = `${normalized} ${suffix}`;
	return expanded.slice(0, 120);
}

function normalizeStoredRadioSong(song: Partial<RadioSong>, message: RadioGenerateMessage): RadioSong {
	const completedAt = typeof song.completed_at === "number" ? song.completed_at : Date.now();
	return {
		id: typeof song.id === "string" && song.id ? song.id : message.song_id,
		station_id: typeof song.station_id === "string" && song.station_id ? song.station_id : message.station_id,
		title: typeof song.title === "string" && song.title ? song.title : "Untitled Signal",
		prompt: typeof song.prompt === "string" && song.prompt ? song.prompt : fallbackRadioPrompt(message).prompt,
		request_id: typeof song.request_id === "string" && song.request_id ? song.request_id : message.request_id,
		request_text: typeof song.request_text === "string" ? song.request_text : message.request_text,
		format: song.format === "wav" ? "wav" : "mp3",
		audio_object_key: typeof song.audio_object_key === "string" && song.audio_object_key ? song.audio_object_key : radioAudioObjectKey(message.song_id, message.format),
		metadata_object_key: typeof song.metadata_object_key === "string" && song.metadata_object_key ? song.metadata_object_key : radioMetadataObjectKey(message.song_id),
		audio_content_type: typeof song.audio_content_type === "string" && song.audio_content_type ? song.audio_content_type : "audio/mpeg",
		primary_genre: song.primary_genre,
		tags: normalizeTags(song.tags),
		mood: song.mood,
		energy: song.energy,
		bpm_min: song.bpm_min,
		bpm_max: song.bpm_max,
		vocal_style: song.vocal_style,
		created_at: typeof song.created_at === "number" ? song.created_at : message.queued_at,
		completed_at: completedAt,
		duration_ms: typeof song.duration_ms === "number" ? song.duration_ms : Math.max(0, completedAt - message.queued_at),
	};
}

function fallbackRadioPrompt(message: RadioGenerateMessage): RadioPromptPlan {
	const request = message.request_text?.trim();
	const genre = message.genre ?? normalizeFacet(request) ?? "experimental pop";
	const title = request ? `${titleFromRequest(request)} ${seedWord(message.creative_seed, 0)}` : fallbackTitle(message);
	const seed = request
		? `A listener requested: "${request}". Interpret it creatively without copying copyrighted lyrics or imitating a living artist exactly.`
		: "No listener request is active. Invent a vivid left-field song concept fit for a strange internet radio station.";
	return {
		title,
		prompt: `${seed} ${message.genre ? `Shape it for ${message.genre} radio.` : ""} Creative seed: ${message.creative_seed}. Contrast axis: ${message.creative_axis}. Tempo center: ${message.creative_bpm} BPM. Make a polished, original 2-3 minute song with a strong hook, clear genre fusion, specific instrumentation, vocal direction, production texture, rhythmic motion, and emotional arc. Include an ear-catching intro, a memorable chorus, a dynamic bridge, and a satisfying outro. Avoid direct artist imitation and avoid quoting existing songs.`,
		primary_genre: genre,
		tags: normalizeTags([genre, "ai radio", message.creative_axis, request ?? "original"]),
		mood: "surprising",
		energy: 7,
		bpm_min: Math.max(40, message.creative_bpm - 8),
		bpm_max: Math.min(240, message.creative_bpm + 8),
		vocal_style: "expressive lead vocal with memorable hook",
	};
}

function creativeDirection(songId: string, index: number, genre?: string, request?: string): { axis: string; bpm: number; seed: string } {
	const adjectives = ["neon", "velvet", "glass", "feral", "midnight", "solar", "chrome", "honey", "static", "opal", "paper", "thunder"];
	const places = ["observatory", "subway", "harbor", "greenhouse", "satellite", "market", "arcade", "lighthouse", "desert", "rooftop", "warehouse", "chapel"];
	const gestures = ["handclap ritual", "broken radio hook", "choir pad swell", "rubber bassline", "stuttering drum fill", "tape-warped bridge", "call-and-response refrain", "wordless falsetto lift"];
	const palettes = ["warm analog haze", "icy digital shimmer", "dry close-mic intimacy", "wide festival gloss", "dusty cassette grit", "hyperclean club pressure"];
	const axis = `${pick(adjectives, songId, index)} ${pick(places, songId, index + 3)} with ${pick(gestures, songId, index + 7)} and ${pick(palettes, songId, index + 11)}`;
	const bpm = 72 + (hashString(`${songId}:${index}:${genre ?? ""}:${request ?? ""}`) % 84);
	const seed = `${pick(adjectives, songId, index + 13)}-${pick(places, songId, index + 17)}-${songId.slice(0, 8)}`;
	return { axis, bpm, seed };
}

function fallbackTitle(message: RadioGenerateMessage): string {
	return `${titleCaseWords(seedWord(message.creative_seed, 0))} ${titleCaseWords(seedWord(message.creative_seed, 1))}`;
}

function seedWord(seed: string | undefined, index: number): string {
	const words = (seed || "signal drift").split(/[-\s]+/).filter(Boolean);
	return words[index % Math.max(1, words.length)] ?? "signal";
}

function pick(values: string[], seed: string, salt: number): string {
	return values[(hashString(`${seed}:${salt}`) % values.length + values.length) % values.length];
}

function hashString(value: string): number {
	let hash = 2166136261;
	for (let i = 0; i < value.length; i++) {
		hash ^= value.charCodeAt(i);
		hash = Math.imul(hash, 16777619);
	}
	return hash >>> 0;
}

function titleCaseWords(value: string): string {
	return value.replace(/\b\w/g, (char) => char.toUpperCase());
}

function titleFromRequest(request: string): string {
	const words = request
		.replace(/[^\w\s-]/g, "")
		.split(/\s+/)
		.filter(Boolean)
		.slice(0, 4);
	return words.length > 0 ? words.map((word) => word[0]?.toUpperCase() + word.slice(1)).join(" ") : "Listener Signal";
}

function parsePromptPlan(text: string): Partial<RadioPromptPlan> & { tags: string[] } {
	const trimmed = text.trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "");
	try {
		const parsed = JSON.parse(trimmed) as Record<string, unknown>;
		return {
			title: typeof parsed.title === "string" ? parsed.title.trim().slice(0, 120) : undefined,
			prompt: typeof parsed.prompt === "string" ? parsed.prompt.trim().slice(0, 2000) : undefined,
			primary_genre: normalizeFacet(parsed.primary_genre),
			tags: normalizeTags(parsed.tags),
			mood: normalizeFacet(parsed.mood),
			energy: normalizeBoundedInt(parsed.energy, 1, 10),
			bpm_min: normalizeBoundedInt(parsed.bpm_min, 40, 240),
			bpm_max: normalizeBoundedInt(parsed.bpm_max, 40, 240),
			vocal_style: typeof parsed.vocal_style === "string" ? parsed.vocal_style.trim().slice(0, 160) : undefined,
		};
	} catch {
		return { prompt: trimmed.slice(0, 2000), tags: [] };
	}
}
