export type MusicInput = {
	prompt: string;
	is_instrumental: boolean;
	format: "mp3" | "wav";
	lyrics?: string;
};

export type RadioRequest = {
	id: string;
	text: string;
	created_at: number;
	assigned_song_id?: string;
	assigned_at?: number;
};

export type RadioInFlight = {
	song_id: string;
	queued_at: number;
	creative_seed?: string;
	workflow_instance_id?: string;
	request_id?: string;
	request_created_at?: number;
	request_text?: string;
};

export type FulfilledRadioRequest = RadioRequest & {
	fulfilled_at: number;
	song_id: string;
	song_title: string;
};

export type RadioDraftReservation = {
	song_id: string;
	title: string;
	prompt: string;
	prompt_fingerprint: string;
	lyrics_fingerprint?: string;
	request_text?: string;
	created_at: number;
};

export type RadioDraftReservationResult = {
	accepted: boolean;
	reservations: RadioDraftReservation[];
	reason?: string;
	conflict_song_id?: string;
};

export type RadioSong = {
	id: string;
	station_id: string;
	title: string;
	prompt: string;
	cover_art_object_key?: string;
	cover_art_prompt?: string;
	cover_art_model?: string;
	cover_art_created_at?: number;
	request_id?: string;
	request_text?: string;
	lyrics?: string;
	lyrics_source?: string;
	music_model?: string;
	text_model?: string;
	creative_seed?: string;
	creative_axis?: string;
	creative_bpm?: number;
	generation_input?: Record<string, unknown>;
	prompt_plan?: RadioPromptPlan;
	format: MusicInput["format"];
	audio_object_key: string;
	metadata_object_key: string;
	audio_content_type: string;
	primary_genre?: string;
	genre_key?: string;
	tags: string[];
	mood?: string;
	energy?: number;
	bpm_min?: number;
	bpm_max?: number;
	vocal_style?: string;
	created_at: number;
	completed_at: number;
	duration_ms: number;
};

export type RadioPromptPlan = {
	title: string;
	prompt: string;
	lyrics?: string;
	primary_genre?: string;
	tags: string[];
	mood?: string;
	energy?: number;
	bpm_min?: number;
	bpm_max?: number;
	vocal_style?: string;
};

export type RadioStationRecord = {
	id: string;
	name: string;
	description?: string;
	genre_filter?: string;
	created_at: number;
	updated_at: number;
};

export type RadioStatus = {
	in_flight: RadioInFlight[];
	playlist: RadioSong[];
	fulfilled_requests: FulfilledRadioRequest[];
	requests: RadioRequest[];
	target_backlog: number;
};

export type RadioGenerateMessage = {
	song_id: string;
	station_id: string;
	format: MusicInput["format"];
	request_id?: string;
	request_created_at?: number;
	request_text?: string;
	genre?: string;
	creative_seed: string;
	creative_axis: string;
	creative_bpm: number;
	queued_at: number;
};

export type LibrarySort = "newest" | "oldest" | "title" | "energy";

export type LibraryQuery = {
	cursor: number;
	genre?: string;
	limit: number;
	mood?: string;
	sort: LibrarySort;
	station_id?: string;
	tag?: string;
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
	audio_object_key?: string;
	audio_content_type?: string;
	error?: string;
	attempts: number;
	attempt_log: AttemptLog[];
	created_at: number;
	started_at?: number;
	completed_at?: number;
	expires_at?: number;
};

export type PublicJobRecord = Omit<JobRecord, "audio_content_type" | "audio_object_key" | "audio_url" | "input"> & {
	ready: boolean;
};

export type StoredAudioObject = Pick<
	R2ObjectBody,
	"body" | "httpEtag" | "httpMetadata" | "range" | "size" | "writeHttpMetadata"
>;

export type StoredAudioRange = {
	end: number;
	r2Range: R2Range;
	start: number;
	total: number;
};

export type PreparedAudioBody = {
	body: ArrayBuffer;
	duplicate_removed: boolean;
	original_bytes: number;
	stored_bytes: number;
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
export const ATTEMPT_TIMEOUT_MS = 13 * 60 * 1000;
export const STALE_JOB_MS = ATTEMPT_TIMEOUT_MS + 30 * 1000;
export const JOB_TTL_MS = 60 * 60 * 1000;
export const RATE_LIMIT_WINDOW_MS = 60 * 60 * 1000;
export const RATE_LIMIT_MAX_JOBS = 3;
export const RADIO_STATION_ID = "main";
export const RADIO_TARGET_BACKLOG = 10;
export const RADIO_MAX_PLAYLIST = 250;
export const RADIO_MAX_REQUESTS = 50;
export const RADIO_MAX_FULFILLED_REQUESTS = 100;
export const RADIO_REQUEST_MAX_CHARS = 500;
export const RADIO_IN_FLIGHT_STALE_MS = 45 * 60 * 1000;
export const RADIO_TEXT_MODEL = "@cf/meta/llama-4-scout-17b-16e-instruct";
export const RADIO_LYRICS_MODEL = "@cf/meta/llama-3.3-70b-instruct-fp8-fast";
export const RADIO_MUSIC_MODEL = "minimax/music-2.6";
export const RADIO_COVER_MODELS = [
	"@cf/black-forest-labs/flux-1-schnell",
	"@cf/black-forest-labs/flux-2-klein-4b",
	"@cf/black-forest-labs/flux-2-klein-9b",
	"@cf/black-forest-labs/flux-2-dev",
	"@cf/leonardo/lucid-origin",
	"@cf/leonardo/phoenix-1.0",
	"@cf/bytedance/stable-diffusion-xl-lightning",
	"@cf/stabilityai/stable-diffusion-xl-base-1.0",
	"@cf/lykon/dreamshaper-8-lcm",
	"@cf/runwayml/stable-diffusion-v1-5-img2img",
	"@cf/runwayml/stable-diffusion-v1-5-inpainting",
] as const;
export const RADIO_COVER_MODEL = RADIO_COVER_MODELS[0];
export const LIBRARY_MAX_LIMIT = 100;
export const LIBRARY_DEFAULT_LIMIT = 25;

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
	const {
		audio_content_type: _audioContentType,
		audio_object_key: _audioObjectKey,
		audio_url: _audioUrl,
		input: _input,
		...publicRecord
	} = record;
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

export function audioObjectKey(jobId: string, format: MusicInput["format"]): string {
	return `music/${jobId}.${format}`;
}

export function prepareAudioBodyForStorage(input: ArrayBuffer): PreparedAudioBody {
	const bytes = new Uint8Array(input);
	if (isExactRepeatedMp3(bytes)) {
		const half = bytes.byteLength / 2;
		return {
			body: input.slice(0, half),
			duplicate_removed: true,
			original_bytes: bytes.byteLength,
			stored_bytes: half,
		};
	}
	return {
		body: input,
		duplicate_removed: false,
		original_bytes: bytes.byteLength,
		stored_bytes: bytes.byteLength,
	};
}

function isExactRepeatedMp3(bytes: Uint8Array): boolean {
	if (bytes.byteLength < 2 || bytes.byteLength % 2 !== 0) return false;
	const half = bytes.byteLength / 2;
	if (!looksLikeMp3Start(bytes, 0) || !looksLikeMp3Start(bytes, half)) return false;
	for (let i = 0; i < half; i++) {
		if (bytes[i] !== bytes[half + i]) return false;
	}
	return true;
}

function looksLikeMp3Start(bytes: Uint8Array, offset: number): boolean {
	if (bytes.byteLength - offset < 4) return false;
	if (
		bytes[offset] === 0x49 &&
		bytes[offset + 1] === 0x44 &&
		bytes[offset + 2] === 0x33
	) {
		return true;
	}
	return bytes[offset] === 0xff && (bytes[offset + 1] & 0xe0) === 0xe0;
}

export function radioAudioObjectKey(songId: string, format: MusicInput["format"]): string {
	return `radio/audio/${songId}.${format}`;
}

export function radioMetadataObjectKey(songId: string): string {
	return `radio/metadata/${songId}.json`;
}

export function radioCoverObjectKey(songId: string): string {
	return `radio/covers/${songId}.jpg`;
}

export function genreStationId(genre: string): string {
	return `genre:${slugifyFacet(canonicalGenreKey(genre) ?? genre)}`;
}

export function stationName(id: string, genre?: string): string {
	if (id === RADIO_STATION_ID) return "Main Radio";
	if (genre) return `${titleCase(genre)} Radio`;
	if (id.startsWith("genre:")) return `${titleCase(id.slice("genre:".length).replace(/-/g, " "))} Radio`;
	return titleCase(id.replace(/[:_-]+/g, " "));
}

export function normalizeFacet(value: unknown, maxLength = 80): string | undefined {
	if (typeof value !== "string") return undefined;
	const normalized = value.trim().toLowerCase().replace(/\s+/g, " ");
	if (!normalized) return undefined;
	return normalized.slice(0, maxLength);
}

export function canonicalGenreKey(value: unknown): string | undefined {
	const normalized = normalizeFacet(value);
	if (!normalized) return undefined;
	const tokens = normalized
		.replace(/[\/_+-]+/g, " ")
		.replace(/&/g, " and ")
		.split(/\s+/)
		.map((token) => token.trim())
		.filter((token) => token && token !== "and" && token !== "with" && token !== "elements");
	if (tokens.length === 0) return undefined;
	return [...new Set(tokens)].sort((a, b) => a.localeCompare(b)).join(" ").slice(0, 80);
}

export function normalizeTags(value: unknown): string[] {
	if (!Array.isArray(value)) return [];
	const tags = value
		.map((item) => normalizeFacet(item, 48))
		.filter((item): item is string => Boolean(item));
	return [...new Set(tags)].slice(0, 12);
}

export function normalizeBoundedInt(value: unknown, min: number, max: number): number | undefined {
	if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
	const integer = Math.round(value);
	return Math.min(max, Math.max(min, integer));
}

export function normalizeGeneratedTitle(title: string): string {
	return title
		.trim()
		.replace(/^```(?:json|text)?\s*/i, "")
		.replace(/\s*```$/i, "")
		.replace(/\\n/g, " ")
		.replace(/\r?\n/g, " ")
		.replace(/\s+/g, " ")
		.replace(/\s*["'`]*\s*[,}]+\s*["'`]*\s*$/g, "")
		.replace(/^["'`]+|["'`]+$/g, "")
		.replace(/\s+[a-f0-9]{6,8}$/i, "")
		.replace(/\b(\w+)\s+\1\b/gi, "$1")
		.trim()
		.slice(0, 120);
}

const MOTIF_STOPWORDS: ReadonlySet<string> = new Set([
	// common english (4+ chars)
	"about","above","across","after","again","against","along","also","another","around","because","been","before","behind","below","beneath","beside","between","beyond","both","could","does","done","down","each","either","ever","every","from","gets","gives","gone","good","have","having","here","hold","into","just","keep","know","like","look","made","make","many","more","most","much","must","near","need","never","none","once","only","other","over","past","quite","same","seem","seen","shall","since","some","such","than","that","their","them","then","there","these","they","this","those","though","through","under","until","upon","very","want","were","what","when","where","which","while","will","with","within","without","would","your","yours",
	// section tags (also stripped, but defensive)
	"intro","verse","verses","chorus","choruses","bridge","bridges","outro","outros","prechorus","section","sections",
	// generic music vocabulary that is structural rather than imagery
	"song","songs","track","tracks","music","sound","sounds","beat","beats","tempo","mood","moods","genre","genres","style","styles","vocal","vocals","instrumental","instrument","instruments","instrumentation","arrangement","arrangements","production","melody","melodies","harmony","harmonies","rhythm","rhythms","note","notes","chord","chords","scale","scales","line","lines","lyric","lyrics","hook","hooks","tone","tones","pitch","pitches","mixdown","texture","textures","timbre","timbres",
	// radio/station scaffolding (vocabulary the system prompts themselves use)
	"radio","station","listener","listeners","prompt","prompts","title","titles","draft","drafts","minimax","model","models","catalog","concept","concepts","seed","seeds","creative","axis","entropy","hash","hashes","fragment","fragments","internal","uniqueness","reference","field","fields","metadata","format","return","include","exclude","avoid","copyright","copyrighted","artist","artists","quote","quotes","quoted","copying","original",
	// visual prompt scaffolding
	"square","pictorial","image","visual","direction","focus","scene","color","lighting","character","characters","landscape","architecture","abstract","pattern","motion","blank","unmarked","surface","surfaces","purely","shape","shapes","device","devices","poster","posters","signage","label","labels","logo","logos","watermark","watermarks","letter","letters","numeral","numerals","symbol","symbols","editorial","feeling","tactile","striking","central","composition","contrast","layered","depth","handmade","detail","filled","continuous",
]);

export type OverusedNounTerm = { word: string; count: number };

export function overusedNounTerms(
	documents: ReadonlyArray<string | undefined | null>,
	options: { minDocs?: number; max?: number } = {},
): OverusedNounTerm[] {
	const minDocs = options.minDocs ?? 3;
	const max = options.max ?? 12;
	const docCounts = new Map<string, number>();
	for (const doc of documents) {
		if (!doc) continue;
		const stripped = doc.replace(/\[[^\]]+\]/g, " ").toLowerCase();
		const tokens = new Set(stripped.match(/[a-z]{4,}/g) ?? []);
		for (const token of tokens) {
			if (MOTIF_STOPWORDS.has(token)) continue;
			docCounts.set(token, (docCounts.get(token) ?? 0) + 1);
		}
	}
	return [...docCounts.entries()]
		.filter(([, count]) => count >= minDocs)
		.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
		.slice(0, max)
		.map(([word, count]) => ({ word, count }));
}

export function recurringMotifs(
	documents: ReadonlyArray<string | undefined | null>,
	options: { minDocs?: number; max?: number } = {},
): OverusedNounTerm[] {
	return overusedNounTerms(documents, options);
}

export function overusedNounMatches(
	value: string | undefined | null,
	terms: ReadonlyArray<OverusedNounTerm>,
	options: { ignoreText?: string } = {},
): OverusedNounTerm[] {
	if (!value || terms.length === 0) return [];
	const ignored = new Set((options.ignoreText?.toLowerCase().match(/[a-z]{4,}/g) ?? []));
	const tokens = new Set(value.toLowerCase().match(/[a-z]{4,}/g) ?? []);
	return terms.filter((term) => !ignored.has(term.word) && tokens.has(term.word));
}

export function parseLibraryQuery(url: URL): LibraryQuery | { error: string } {
	const limitRaw = url.searchParams.get("limit");
	const cursorRaw = url.searchParams.get("cursor");
	const sortRaw = url.searchParams.get("sort") ?? "newest";
	const limit = limitRaw ? Number(limitRaw) : LIBRARY_DEFAULT_LIMIT;
	const cursor = cursorRaw ? Number(cursorRaw) : 0;
	if (!Number.isSafeInteger(limit) || limit < 1) return { error: "limit must be a positive integer" };
	if (!Number.isSafeInteger(cursor) || cursor < 0) return { error: "cursor must be a non-negative integer" };
	if (!["newest", "oldest", "title", "energy"].includes(sortRaw)) {
		return { error: "sort must be newest, oldest, title, or energy" };
	}
	return {
		limit: Math.min(limit, LIBRARY_MAX_LIMIT),
		cursor,
		sort: sortRaw as LibrarySort,
		genre: normalizeFacet(url.searchParams.get("genre")),
		mood: normalizeFacet(url.searchParams.get("mood")),
		station_id: normalizeStationId(url.searchParams.get("station_id")),
		tag: normalizeFacet(url.searchParams.get("tag")),
	};
}

export function parseStationParams(url: URL, body?: unknown): { station_id: string; genre?: string } | { error: string } {
	const raw = body && typeof body === "object" ? (body as Record<string, unknown>) : {};
	const rawGenre = normalizeFacet(raw.genre ?? url.searchParams.get("genre"));
	const rawStation = normalizeStationId(raw.station_id ?? url.searchParams.get("station") ?? url.searchParams.get("station_id"));
	if (rawGenre) return { station_id: genreStationId(rawGenre), genre: rawGenre };
	if (rawStation) return { station_id: rawStation };
	return { station_id: RADIO_STATION_ID };
}

export function normalizeStationId(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const trimmed = value.trim().toLowerCase();
	if (!trimmed) return undefined;
	if (!/^[a-z0-9:_-]{1,80}$/.test(trimmed)) return undefined;
	return trimmed;
}

export function slugifyFacet(value: string): string {
	return value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 64) || "station";
}

export function parseRadioRequest(body: unknown): { text: string } | { error: string } {
	if (!body || typeof body !== "object") return { error: "body must be a JSON object" };
	const raw = body as Record<string, unknown>;
	const unknownField = Object.keys(raw).find((key) => !["prompt", "request", "station_id", "genre"].includes(key));
	if (unknownField) return { error: `unsupported field: ${unknownField}` };

	const text = typeof raw.prompt === "string" ? raw.prompt.trim() : typeof raw.request === "string" ? raw.request.trim() : "";
	if (!text) return { error: "prompt is required" };
	if (text.length > RADIO_REQUEST_MAX_CHARS) {
		return { error: `prompt must be <= ${RADIO_REQUEST_MAX_CHARS} chars` };
	}
	return { text };
}

export function extractTextResponse(value: unknown): string | undefined {
	if (typeof value === "string" && value.trim()) return value.trim();
	if (!value || typeof value !== "object") return undefined;
	const record = value as Record<string, unknown>;
	for (const key of ["response", "result", "text", "content"]) {
		const candidate = record[key];
		if (typeof candidate === "string" && candidate.trim()) return candidate.trim();
	}
	return undefined;
}

function titleCase(value: string): string {
	return value.replace(/\b\w/g, (char) => char.toUpperCase());
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
		upstream.headers.get("content-type") ?? audioContentType(record),
	);
	headers.set("Cache-Control", "no-store");

	for (const name of ["content-length", "content-range", "accept-ranges"]) {
		const value = upstream.headers.get(name);
		if (value) headers.set(name, value);
	}
	if (!headers.has("Accept-Ranges")) headers.set("Accept-Ranges", "none");
	return headers;
}

export function parseRangeHeader(value: string | null, totalSize: number): StoredAudioRange | { error: "invalid" | "unsatisfiable" } | undefined {
	if (!value) return undefined;
	const match = /^bytes=(\d*)-(\d*)$/.exec(value.trim());
	if (!match) return { error: "invalid" };

	const [, rawStart, rawEnd] = match;
	if (!rawStart && !rawEnd) return { error: "invalid" };

	if (!rawStart) {
		const suffix = Number(rawEnd);
		if (!Number.isSafeInteger(suffix) || suffix <= 0) return { error: "invalid" };
		if (totalSize <= 0) return { error: "unsatisfiable" };
		const length = Math.min(suffix, totalSize);
		const start = totalSize - length;
		return {
			end: totalSize - 1,
			r2Range: { suffix: length },
			start,
			total: totalSize,
		};
	}

	const start = Number(rawStart);
	const explicitEnd = rawEnd ? Number(rawEnd) : undefined;
	if (!Number.isSafeInteger(start) || start < 0) return { error: "invalid" };
	if (explicitEnd !== undefined && (!Number.isSafeInteger(explicitEnd) || explicitEnd < start)) {
		return { error: "invalid" };
	}
	if (start >= totalSize || totalSize <= 0) return { error: "unsatisfiable" };

	const end = Math.min(explicitEnd ?? totalSize - 1, totalSize - 1);
	return {
		end,
		r2Range: { offset: start, length: end - start + 1 },
		start,
		total: totalSize,
	};
}

export function rangeNotSatisfiableHeaders(totalSize: number): Headers {
	const headers = new Headers();
	headers.set("Accept-Ranges", "bytes");
	headers.set("Content-Range", `bytes */${totalSize}`);
	return headers;
}

export function storedAudioResponseHeaders(record: JobRecord, object: StoredAudioObject, range?: StoredAudioRange): Headers {
	const headers = new Headers();
	object.writeHttpMetadata(headers);
	headers.set("Content-Type", object.httpMetadata?.contentType ?? audioContentType(record));
	headers.set("Cache-Control", "no-store");
	headers.set("ETag", object.httpEtag);
	headers.set("Accept-Ranges", "bytes");
	headers.set("Content-Length", String(range ? range.end - range.start + 1 : object.size));

	if (range) headers.set("Content-Range", `bytes ${range.start}-${range.end}/${range.total}`);
	return headers;
}

export function storedAudioStatus(range?: StoredAudioRange): 200 | 206 {
	return range ? 206 : 200;
}

function audioContentType(record: JobRecord): string {
	return record.audio_content_type ?? (record.input.format === "wav" ? "audio/wav" : "audio/mpeg");
}
