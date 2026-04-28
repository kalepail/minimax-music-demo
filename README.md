# MiniMax Music 2.6 Demo

A Cloudflare Worker that generates songs with the [MiniMax Music 2.6](https://developers.cloudflare.com/ai/models/minimax/music-2.6/) model on Workers AI, served behind a tiny single-page frontend.

Single-song generation runs asynchronously: the Worker hands off to a Durable Object, which calls the model through an AI Gateway and copies the finished audio to R2 for one hour. The browser polls a status endpoint and streams the finished audio through the Worker.

The radio station flow uses a `RadioStation` Durable Object plus one Cloudflare Workflow instance per song. Cron or the manual fill endpoint keeps up to 10 song generations in flight, each workflow asks one text model for a richer music prompt, asks a stronger lyric model for structured original lyrics, calls MiniMax with those lyrics, stores finished audio and cover art in R2, and writes song metadata to D1.

## Architecture

```
public/index.html       Single-page form + status polling
src/index.ts            Worker routes, MusicJob DO, RadioStation DO, RadioSongWorkflow
wrangler.jsonc          AI, DO, Workflow, R2, Cron, asset bindings
migrations/             D1 song catalog schema
song-radio/SKILL.md     Reusable station/song-generation skill workflow
```

### API

| Method | Path                    | Purpose                                            |
|--------|-------------------------|----------------------------------------------------|
| POST   | `/api/generate`         | Submit a job. Returns `{ jobId }` (HTTP 202).      |
| GET    | `/api/status/:jobId`    | Poll job state and attempt log.                    |
| GET    | `/api/audio/:jobId`     | Stream the generated audio once `state=complete`.  |
| GET    | `/api/radio/status`     | Current station playlist, requests, and in-flight songs. |
| POST   | `/api/radio/request`    | Add a listener request and top up the station backlog. |
| POST   | `/api/radio/fill`       | Manually top up station generation to the target backlog. |
| GET    | `/api/radio/stations`   | List saved stations and genres from the D1 catalog. |
| GET    | `/api/radio/audio/:id`  | Stream a stored station song from R2. |
| GET    | `/api/radio/cover/:id`  | Serve generated cover art from R2. |
| POST   | `/api/radio/backfill-covers` | Admin-only cover-art backfill for catalog songs. |
| GET    | `/api/library`          | Paginated/sortable/filterable D1 song library. |
| GET    | `/api/library/:id`      | Read one cataloged song with tags. |

`POST /api/generate` body:

```json
{
  "prompt": "An upbeat synthwave track about coding at midnight",
  "format": "mp3",
  "is_instrumental": false,
  "lyrics": "optional — omit to let lyrics_optimizer write them"
}
```

Validation follows the Cloudflare Workers AI schema for `minimax/music-2.6`:

- `prompt` is required and must be 1-2000 characters after trimming.
- `lyrics` is optional for this demo. When provided for non-instrumental jobs, it must be 1-3500 characters after trimming.
- `format` is optional and defaults to `mp3`; if provided, it must be `mp3` or `wav`.
- Leaving `lyrics` blank on a non-instrumental job enables `lyrics_optimizer`; instrumental jobs ignore lyrics.

## Prerequisites

- A Cloudflare account with Workers AI enabled
- An [AI Gateway](https://developers.cloudflare.com/ai-gateway/) named `default` (or update `AI_GATEWAY_ID` in `wrangler.jsonc`)
- An R2 bucket named `minimax-music-demo-audio`
- Cloudflare Workflows enabled for the account
- A D1 database named `minimax-music-demo-catalog`
- Node 20+ and `pnpm`

## Setup

```bash
pnpm install
npx wrangler r2 bucket create minimax-music-demo-audio
npx wrangler d1 create minimax-music-demo-catalog
npx wrangler d1 migrations apply minimax-music-demo-catalog --local
npx wrangler d1 migrations apply minimax-music-demo-catalog --remote
pnpm dev          # local dev on http://localhost:8787
pnpm test         # run focused helper tests
pnpm deploy       # deploy to your Cloudflare account
```

After `wrangler d1 create`, copy the generated database UUID into `database_id` in `wrangler.jsonc`.

## Public demo protection

`POST /api/generate` is rate limited by client IP to 3 accepted jobs per hour.

Bindings used (declared in `wrangler.jsonc`):

- `AI` — Workers AI
- `MUSIC_JOB` — Durable Object class `MusicJob`
- `RADIO_STATION` — Durable Object class `RadioStation`
- `RADIO_SONG_WORKFLOW` — Workflow class `RadioSongWorkflow` for durable station song jobs
- `AUDIO_BUCKET` — R2 bucket for generated audio
- `DB` — D1 catalog for songs, tags, stations, sorting, and pagination
- `ASSETS` — static assets from `./public/`

`RADIO_AUTOFILL=true` enables the cron trigger to keep the station topped up. Set it to `false` in `wrangler.jsonc` if you only want manual fills from the UI.

## Job lifecycle

- One synchronous attempt per job, with a 13-minute model timeout to leave room for R2 persistence under the Durable Object alarm wall-time limit.
- A watchdog alarm marks interrupted running jobs as failed after the attempt timeout plus a short grace period.
- Frontend polls every 3 seconds for up to 15 minutes, with the job ID persisted in the URL fragment and local storage so a refresh reconnects to the same job.
- Job records live in Durable Object storage for one hour, then are removed by a cleanup alarm.
- Finished audio is copied to R2, served through `/api/audio/:jobId` with byte-range support, and deleted when the job record expires.

## Radio lifecycle

- Cron runs every 5 minutes and calls `RadioStation.fill(10)` when `RADIO_AUTOFILL=true`.
- The station DO remembers only coordination state: listener requests, in-flight song IDs, and short-lived draft reservations. Playlist and fulfilled-request history are read from D1.
- Each model call is treated as a stateless, isolated task. Durable state from prior steps is passed explicitly as bounded input data: listener request, recent catalog context, in-flight drafts, generated prompt plan, lyrics, and catalog metadata.
- `RadioSongWorkflow` uses `@cf/meta/llama-4-scout-17b-16e-instruct` to expand a listener request into a rich, non-repeating MiniMax prompt. The prompt-planning call does not assume model memory; it receives the relevant recent titles, prompt shapes, and metadata for that one call.
- A separate lyric pass uses `@cf/zai-org/glm-4.7-flash` with structured JSON output to write original MiniMax-compatible sectioned lyrics, with `@cf/moonshotai/kimi-k2.5` as a higher-cost fallback when the primary lyric pass fails or is rejected. The workflow rejects technical-token leakage and weak structure, then calls `minimax/music-2.6` with `lyrics_optimizer=false` and explicit lyrics. If both lyric passes fail, the station falls back to MiniMax's lyrics optimizer so the workflow keeps moving.
- Before calling MiniMax, each workflow asks `@cf/zai-org/glm-4.7-flash` to compare the draft against recent songs and in-flight drafts. The station Durable Object also keeps short-lived draft title/prompt summaries so same-batch workflows can reject and regenerate overlapping concepts.
- Finished station audio is stored under `radio/audio/` in R2.
- Generated cover art is stored under `radio/covers/` in R2. Cover generation rotates across supported Workers AI image models and uses visual-only prompts to reduce title/text artifacts.
- Finished song metadata, including prompt plan, explicit lyrics for new radio songs, model names, exact generation input, and lyric source, is stored in D1 tables `songs`, `song_tags`, and `stations`. Treat D1 as the catalog source of truth; older songs generated through MiniMax's optimizer may only record their lyric source because that API path does not return the generated lyrics.

## Library queries

`GET /api/library` supports:

- `limit` — page size, capped at 100.
- `cursor` — offset cursor returned as `next_cursor`.
- `sort` — `newest`, `oldest`, `title`, or `energy`.
- `genre`, `tag`, `mood`, `station_id` — filters backed by D1 indexes.

Genre station examples:

```bash
curl "http://localhost:8787/api/library?genre=ambient%20drone&sort=energy"
curl -X POST "http://localhost:8787/api/radio/fill" \
  -H "Content-Type: application/json" \
  -d '{"genre":"ambient drone","target":10}'
```

Cover backfill requires the `COVER_BACKFILL_TOKEN` Worker secret and an `Authorization: Bearer ...` header because it can spend Workers AI image-generation quota. Pass `{ "regenerate": true }` to replace existing covers, for example after changing the cover model or prompt.

## License

Apache 2.0 — see [LICENSE](LICENSE).
