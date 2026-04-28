---
name: song-radio
description: Use this skill whenever the user wants to generate many AI songs, build or operate an AI radio station, turn listener requests into music prompts, use Cloudflare Workers AI with MiniMax Music, or coordinate endless/batch song generation with Queues, R2, Durable Objects, Cron, or Workflows.
---

# Song Radio

Use this skill to design, build, or operate an always-on AI music station on Cloudflare. The station should feel like a creative radio director, not a raw prompt forwarder: listener requests become richer concepts, completed songs are archived, and the generation loop keeps the station supplied.

## First steps

1. Retrieve current Cloudflare docs before making Workers, Queues, Durable Objects, R2, Cron, Workflows, Workers AI, or limits decisions.
2. Inspect the local app before changing architecture. Prefer existing bindings, helpers, API shapes, storage conventions, and UI patterns.
3. Confirm the active music model and input schema from current docs or local code. For this repo, the generation path uses `minimax/music-2.6`.
4. Treat generation as asynchronous. Do not hold a normal browser request open for a full song generation unless the existing app already does so safely.

## Default architecture

Use this shape unless the repo strongly points elsewhere:

- Worker HTTP routes for station APIs and static assets.
- Durable Object for station coordination: playlist, recent listener requests, in-flight song IDs, and fill decisions.
- Workflow for generation work. Use one workflow instance per song so long-running multi-step jobs can retry individual phases and recover existing R2/D1 state.
- R2 for permanent audio and metadata storage.
- D1 for the permanent catalog: song rows, tags, stations, sort/filter fields, and pagination.
- Cron trigger for automatic top-up, plus a manual fill endpoint for immediate batches.
- Queue only when a future change needs a lightweight handoff without multi-step durability.

Avoid using a Durable Object as a high-throughput worker pool. Use it for coordination and strong consistency; use Workflows for parallel generation.

## Generation loop

When asked to keep a station filled:

1. Store listener requests in the station Durable Object.
2. On cron or manual fill, remove stale in-flight records.
3. Start enough workflow instances to reach the target in-flight backlog, usually 10.
4. Each workflow instance should:
   - Build a creative song brief from recent listener requests.
   - Treat every model call as stateless. Pass only the data that call needs, and separate instructions from input data with clear sections.
   - Run a text model to turn the listener request plus recent catalog context into a compact, vivid MiniMax prompt with explicit non-repetition constraints.
   - Run a fast structured-output lyric-writing model to write original, structured lyrics using only the prompt plan, listener request, and bounded avoid lists, then validate that the lyrics are long enough, sectioned, unique, and free of technical-token leakage.
   - Compare the draft against recent songs and in-flight draft reservations in a dedicated review call before spending the music call. If the draft overlaps, regenerate once with reviewer guidance.
   - Reserve accepted draft fingerprints in the station Durable Object so same-batch workers can avoid each other's titles, prompts, and lyrics.
   - Call the music model with explicit lyrics and `lyrics_optimizer=false`. Keep `lyrics_optimizer=true` only as a fallback when the lyric-writing pass fails.
   - Stream the returned audio URL into R2.
   - Generate square cover art with a Workers AI text-to-image model and store it in R2. Rotate between supported models for variety, and keep the prompt visual-only so model attention stays on scene/color/texture instead of written language.
   - Store a small metadata JSON record in R2.
   - Insert or update a D1 catalog row and tag rows.
   - Notify the station Durable Object so the playlist updates.
5. Keep audio object keys unique, stable, and append-only.

## Prompt direction

For prompt expansion, ask for JSON with:

```json
{
  "title": "short station-ready title",
  "prompt": "rich original text-to-music prompt",
  "primary_genre": "cosmic disco",
  "tags": ["disco", "modular synth", "euphoric"],
  "mood": "bright",
  "energy": 8,
  "bpm_min": 112,
  "bpm_max": 124,
  "vocal_style": "group chant chorus"
}
```

The generated prompt should include:

- Genre fusion and tempo feel.
- Instrumentation and production texture.
- Vocal direction or instrumental direction.
- Emotional arc and hook.
- A concrete sonic world.

The generated prompt should avoid:

- Direct copyrighted lyrics.
- Requests to imitate a living artist exactly.
- References that require the model to recreate a specific song.
- Vague one-line concepts with no arrangement detail.

## Lyric direction

For lyric writing, ask for JSON with:

```json
{
  "lyrics": "[Verse 1]\n...\n\n[Chorus]\n...",
  "lyric_theme": "one-sentence theme",
  "hook": "short chorus hook summary"
}
```

The lyrics should:

- Use bracketed structure tags such as `[Intro]`, `[Verse 1]`, `[Chorus]`, `[Bridge]`, and `[Outro]`.
- Satisfy a listener request once without pasting the request as the whole lyric.
- Avoid song IDs, UUIDs, technical tokens, catalog-only titles as headings, markdown, metadata, and direct copyrighted lyrics.
- Be concrete, singable, and varied from recent songs.
- Stay under MiniMax's lyric character limit.

## Station APIs

Prefer these route shapes when building a web UI:

- `GET /api/radio/status` returns playlist, requests, in-flight jobs, and target backlog.
- `POST /api/radio/request` accepts `{ "prompt": "..." }`, stores the request, and triggers fill.
- `POST /api/radio/fill` manually tops up the station backlog.
- `GET /api/radio/stations` returns D1-backed stations and genre counts.
- `GET /api/radio/audio/:songId` streams a stored R2 object with byte-range support.
- `GET /api/radio/cover/:songId` serves generated cover art from R2.
- `POST /api/radio/backfill-covers` generates missing covers. Protect it with a Worker secret. Accept `{ "regenerate": true }` when existing covers should be replaced after model or prompt changes.
- `GET /api/library` returns D1-backed song pages with `limit`, `cursor`, `sort`, `genre`, `tag`, `mood`, and `station_id` filters.
- `GET /api/library/:songId` returns one song record with tags.

## Operational guardrails

- Keep workflow params small and serializable; store large generated artifacts in R2 or Durable Object storage.
- Use stable workflow instance IDs for each song and reconcile skipped `createBatch` results before reporting jobs as queued.
- Cap concurrency deliberately. A target of 10 in-flight songs is a reasonable default for this demo.
- Keep R2 object keys unique because concurrent writes to the same key are rate-limited.
- Keep D1 writes idempotent. On Workflow retry, recover from existing R2 metadata and upsert D1 before marking the station song complete.
- Add D1 indexes for every UI filter or sort path that will be used repeatedly.
- Add an explicit `RADIO_AUTOFILL` flag so cron can be disabled without removing the code.
- Run `npx wrangler types` after changing bindings.

## Commands

```sh
npx wrangler r2 bucket create minimax-music-demo-audio
npx wrangler d1 create minimax-music-demo-catalog
npx wrangler d1 migrations apply minimax-music-demo-catalog --local
npx wrangler d1 migrations apply minimax-music-demo-catalog --remote
npx wrangler types
npx wrangler dev
npx wrangler deploy
```
