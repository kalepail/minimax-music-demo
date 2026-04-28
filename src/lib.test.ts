import { describe, expect, it } from "vitest";
import {
	ATTEMPT_TIMEOUT_MS,
	JOB_TTL_MS,
	RATE_LIMIT_MAX_JOBS,
	RATE_LIMIT_WINDOW_MS,
	STALE_JOB_MS,
	applyRateLimit,
	audioObjectKey,
	audioResponseHeaders,
	canonicalGenreKey,
	clientRateLimitKey,
	extractAudioUrl,
	extractTextResponse,
	isStaleRunningJob,
	parseLibraryQuery,
	parseRangeHeader,
	parseInput,
	parseRadioRequest,
	parseStationParams,
	publicStatus,
	genreStationId,
	radioAudioObjectKey,
	radioMetadataObjectKey,
	normalizeTags,
	storedAudioResponseHeaders,
	storedAudioStatus,
	shouldCleanUp,
	type JobRecord,
	type StoredAudioObject,
} from "./lib";

const baseJob: JobRecord = {
	state: "complete",
	input: {
		prompt: "A bright synth pop song",
		is_instrumental: false,
		format: "mp3",
		lyrics: "private lyrics",
	},
	audio_url: "https://example.com/audio.mp3",
	audio_object_key: "music/job-1.mp3",
	audio_content_type: "audio/mpeg",
	attempts: 1,
	attempt_log: [],
	created_at: 1,
	completed_at: 2,
	expires_at: 3,
};

describe("parseInput", () => {
	it("normalizes valid input", () => {
		expect(
			parseInput({
				prompt: "  ambient piano  ",
				format: "wav",
				is_instrumental: false,
				lyrics: "  line one\nline two  ",
			}),
		).toEqual({
			prompt: "ambient piano",
			format: "wav",
			is_instrumental: false,
			lyrics: "line one\nline two",
		});
	});

	it("defaults omitted formats to mp3", () => {
		expect(parseInput({ prompt: "song", is_instrumental: false })).toMatchObject({
			format: "mp3",
		});
	});

	it("rejects unsupported formats when provided", () => {
		expect(parseInput({ prompt: "song", format: "flac", is_instrumental: false })).toEqual({
			error: "format must be mp3 or wav",
		});
	});

	it("rejects unsupported fields instead of silently dropping them", () => {
		expect(parseInput({ prompt: "song", sample_rate: 44100 })).toEqual({
			error: "unsupported field: sample_rate",
		});
	});

	it("drops lyrics for instrumental jobs", () => {
		expect(parseInput({ prompt: "song", is_instrumental: true, lyrics: "ignore me" })).toEqual({
			prompt: "song",
			format: "mp3",
			is_instrumental: true,
		});
	});

	it("rejects missing and oversized prompts", () => {
		expect(parseInput({ prompt: "" })).toEqual({ error: "prompt is required" });
		expect(parseInput({ prompt: "x".repeat(2000) })).toMatchObject({ prompt: "x".repeat(2000) });
		expect(parseInput({ prompt: "x".repeat(2001) })).toEqual({ error: "prompt must be <= 2000 chars" });
	});

	it("rejects oversized lyrics instead of truncating them", () => {
		expect(parseInput({ prompt: "song", lyrics: "x".repeat(3500) })).toMatchObject({
			lyrics: "x".repeat(3500),
		});
		expect(parseInput({ prompt: "song", lyrics: "x".repeat(3501) })).toEqual({
			error: "lyrics must be <= 3500 chars",
		});
	});
});

describe("publicStatus", () => {
	it("redacts audio URLs and submitted input", () => {
		expect(publicStatus(baseJob)).toEqual({
			state: "complete",
			attempts: 1,
			attempt_log: [],
			created_at: 1,
			completed_at: 2,
			expires_at: 3,
			ready: true,
		});
	});
});

describe("job lifecycle helpers", () => {
	it("only cleans up final jobs once their expiration has passed", () => {
		expect(shouldCleanUp({ ...baseJob, expires_at: 100 }, 99)).toBe(false);
		expect(shouldCleanUp({ ...baseJob, expires_at: 100 }, 100)).toBe(true);
		expect(shouldCleanUp({ ...baseJob, state: "running", expires_at: 100 }, 100)).toBe(false);
	});

	it("detects stale running jobs at the watchdog deadline", () => {
		const running: JobRecord = {
			...baseJob,
			state: "running",
			started_at: 1000,
			completed_at: undefined,
			expires_at: undefined,
		};

		expect(isStaleRunningJob(running, 999 + STALE_JOB_MS)).toBe(false);
		expect(isStaleRunningJob(running, 1000 + STALE_JOB_MS)).toBe(true);
		expect(isStaleRunningJob(running, 1001 + STALE_JOB_MS)).toBe(true);
	});
});

describe("rate limiting", () => {
	it("allows requests until the per-window limit is reached", () => {
		let result = applyRateLimit(undefined, 1000);
		expect(result.allowed).toBe(true);
		expect(result.remaining).toBe(RATE_LIMIT_MAX_JOBS - 1);

		for (let i = 1; i < RATE_LIMIT_MAX_JOBS; i++) {
			result = applyRateLimit(result.record, 1000 + i);
		}
		expect(result.allowed).toBe(true);
		expect(result.remaining).toBe(0);

		const rejected = applyRateLimit(result.record, 2000);
		expect(rejected.allowed).toBe(false);
		expect(rejected.retry_after_ms).toBe(RATE_LIMIT_WINDOW_MS - 1000);
	});

	it("starts a new window after expiration", () => {
		const first = applyRateLimit(undefined, 1000);
		const next = applyRateLimit(first.record, 1000 + RATE_LIMIT_WINDOW_MS);
		expect(next.allowed).toBe(true);
		expect(next.record.count).toBe(1);
	});

	it("uses Cloudflare client IP headers for rate keys", () => {
		expect(clientRateLimitKey(new Request("https://example.com", { headers: { "CF-Connecting-IP": "203.0.113.10" } }))).toBe(
			"rate:203.0.113.10",
		);
		expect(clientRateLimitKey(new Request("https://example.com", { headers: { "X-Forwarded-For": "198.51.100.1, 10.0.0.1" } }))).toBe(
			"rate:198.51.100.1",
		);
	});
});

describe("extractAudioUrl", () => {
	it("supports direct and nested response shapes", () => {
		expect(extractAudioUrl({ audio: "https://example.com/direct.mp3" })).toBe("https://example.com/direct.mp3");
		expect(extractAudioUrl({ result: { audio: "https://example.com/nested.mp3" } })).toBe(
			"https://example.com/nested.mp3",
		);
	});
});

describe("extractTextResponse", () => {
	it("supports common Workers AI text response shapes", () => {
		expect(extractTextResponse(" hello ")).toBe("hello");
		expect(extractTextResponse({ response: "song plan" })).toBe("song plan");
		expect(extractTextResponse({ result: "fallback result" })).toBe("fallback result");
	});
});

describe("audioObjectKey", () => {
	it("creates stable per-job R2 keys", () => {
		expect(audioObjectKey("job-123", "mp3")).toBe("music/job-123.mp3");
		expect(audioObjectKey("job-123", "wav")).toBe("music/job-123.wav");
	});
});

describe("radio helpers", () => {
	it("creates stable station R2 keys", () => {
		expect(radioAudioObjectKey("song-123", "mp3")).toBe("radio/audio/song-123.mp3");
		expect(radioMetadataObjectKey("song-123")).toBe("radio/metadata/song-123.json");
		expect(genreStationId("Cosmic Disco!")).toBe("genre:cosmic-disco");
		expect(genreStationId("Experimental/Electronic")).toBe("genre:electronic-experimental");
		expect(canonicalGenreKey("experimental electronic")).toBe("electronic experimental");
	});

	it("normalizes listener requests", () => {
		expect(parseRadioRequest({ prompt: "  cosmic funk  " })).toEqual({ text: "cosmic funk" });
		expect(parseRadioRequest({ request: "ambient lullaby", genre: "drone" })).toEqual({ text: "ambient lullaby" });
		expect(parseRadioRequest({ prompt: "" })).toEqual({ error: "prompt is required" });
		expect(parseRadioRequest({ prompt: "x".repeat(501) })).toEqual({ error: "prompt must be <= 500 chars" });
	});

	it("deduplicates and limits normalized tags", () => {
		expect(normalizeTags([" Disco ", "disco", "", 3, "Modular Synth"])).toEqual(["disco", "modular synth"]);
	});

	it("parses library filters and pagination", () => {
		const query = parseLibraryQuery(
			new URL("https://example.com/api/library?limit=200&cursor=25&sort=energy&genre=Ambient%20Drone&tag=Long%20Tail"),
		);
		expect(query).toMatchObject({
			limit: 100,
			cursor: 25,
			sort: "energy",
			genre: "ambient drone",
			tag: "long tail",
		});
		expect(parseLibraryQuery(new URL("https://example.com/api/library?sort=random"))).toEqual({
			error: "sort must be newest, oldest, title, or energy",
		});
	});

	it("derives genre stations from request params", () => {
		expect(parseStationParams(new URL("https://example.com/api/radio/status?genre=Cosmic%20Disco"))).toEqual({
			station_id: "genre:cosmic-disco",
			genre: "cosmic disco",
		});
		expect(parseStationParams(new URL("https://example.com/api/radio/status?station=main"))).toEqual({
			station_id: "main",
		});
	});
});

describe("audioResponseHeaders", () => {
	it("does not advertise byte ranges unless the upstream does", () => {
		const upstream = new Response(null, {
			headers: {
				"content-length": "5",
			},
		});

		const headers = audioResponseHeaders(baseJob, upstream);
		expect(headers.get("Accept-Ranges")).toBe("none");
		expect(headers.get("Content-Length")).toBe("5");
		expect(headers.get("Cache-Control")).toBe("no-store");
		expect(headers.get("Content-Type")).toBe("audio/mpeg");
	});

	it("passes through partial content headers", () => {
		const upstream = new Response("audio", {
			headers: {
				"accept-ranges": "bytes",
				"content-range": "bytes 0-4/10",
				"content-type": "audio/custom",
			},
		});

		const headers = audioResponseHeaders(baseJob, upstream);
		expect(headers.get("Accept-Ranges")).toBe("bytes");
		expect(headers.get("Content-Range")).toBe("bytes 0-4/10");
		expect(headers.get("Content-Type")).toBe("audio/custom");
	});
});

describe("storedAudioResponseHeaders", () => {
	it("serves whole R2 objects with audio metadata", () => {
		const object = makeStoredAudioObject({ size: 10 });
		const headers = storedAudioResponseHeaders(baseJob, object);

		expect(storedAudioStatus()).toBe(200);
		expect(headers.get("Accept-Ranges")).toBe("bytes");
		expect(headers.get("Cache-Control")).toBe("no-store");
		expect(headers.get("Content-Length")).toBe("10");
		expect(headers.get("Content-Type")).toBe("audio/mpeg");
		expect(headers.get("ETag")).toBe('"etag"');
		expect(headers.get("Content-Range")).toBeNull();
	});

	it("serves R2 byte ranges with partial content headers", () => {
		const object = makeStoredAudioObject({ size: 10 });
		const range = parseRangeHeader("bytes=2-5", object.size);
		if (!range || "error" in range) throw new Error("expected valid range");
		const headers = storedAudioResponseHeaders(baseJob, object, range);

		expect(storedAudioStatus(range)).toBe(206);
		expect(headers.get("Content-Length")).toBe("4");
		expect(headers.get("Content-Range")).toBe("bytes 2-5/10");
	});
});

describe("parseRangeHeader", () => {
	it("parses bounded, open-ended, and suffix byte ranges", () => {
		expect(parseRangeHeader("bytes=0-1023", 5000)).toEqual({
			start: 0,
			end: 1023,
			total: 5000,
			r2Range: { offset: 0, length: 1024 },
		});
		expect(parseRangeHeader("bytes=100-", 5000)).toEqual({
			start: 100,
			end: 4999,
			total: 5000,
			r2Range: { offset: 100, length: 4900 },
		});
		expect(parseRangeHeader("bytes=-500", 5000)).toEqual({
			start: 4500,
			end: 4999,
			total: 5000,
			r2Range: { suffix: 500 },
		});
	});

	it("rejects invalid and unsatisfiable ranges", () => {
		expect(parseRangeHeader("bytes=10-5", 100)).toEqual({ error: "invalid" });
		expect(parseRangeHeader("bytes=100-", 100)).toEqual({ error: "unsatisfiable" });
	});
});

describe("timeout constants", () => {
	it("keeps the attempt below the 15-minute Durable Object alarm wall-time limit", () => {
		expect(ATTEMPT_TIMEOUT_MS).toBeLessThan(15 * 60 * 1000);
		expect(STALE_JOB_MS).toBeGreaterThan(ATTEMPT_TIMEOUT_MS);
		expect(JOB_TTL_MS).toBe(60 * 60 * 1000);
	});
});

function makeStoredAudioObject({
	range,
	size,
}: {
	range?: R2Range;
	size: number;
}): StoredAudioObject {
	const httpMetadata = { contentType: "audio/mpeg" };
	return {
		body: new ReadableStream(),
		httpEtag: '"etag"',
		httpMetadata,
		range,
		size,
		writeHttpMetadata(headers) {
			headers.set("Content-Type", httpMetadata.contentType);
		},
	};
}
