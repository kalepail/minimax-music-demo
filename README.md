# MiniMax Music 2.6 Demo

A minimal Cloudflare Worker that generates songs with the [MiniMax Music 2.6](https://developers.cloudflare.com/workers-ai/models/) model on Workers AI, served behind a tiny single-page frontend.

Generation runs asynchronously: the Worker hands off to a Durable Object, which calls the model through an AI Gateway (with cache-key reuse) and stores the result in DO storage for one hour. The browser polls a status endpoint and streams the finished audio through the Worker.

## Architecture

```
public/index.html       Single-page form + status polling
src/index.ts            Worker routes + MusicJob Durable Object
wrangler.jsonc          AI binding, DO binding, asset binding
```

### API

| Method | Path                    | Purpose                                            |
|--------|-------------------------|----------------------------------------------------|
| POST   | `/api/generate`         | Submit a job. Returns `{ jobId }` (HTTP 202).      |
| GET    | `/api/status/:jobId`    | Poll job state and attempt log.                    |
| GET    | `/api/audio/:jobId`     | Stream the generated audio once `state=complete`.  |

`POST /api/generate` body:

```json
{
  "prompt": "An upbeat synthwave track about coding at midnight",
  "format": "mp3",
  "is_instrumental": false,
  "lyrics": "optional — omit to let lyrics_optimizer write them"
}
```

## Prerequisites

- A Cloudflare account with Workers AI enabled
- An [AI Gateway](https://developers.cloudflare.com/ai-gateway/) named `default` (or update `AI_GATEWAY_ID` in `wrangler.jsonc`)
- Node 20+ and `pnpm`

## Setup

```bash
pnpm install
pnpm dev          # local dev on http://localhost:8787
pnpm deploy       # deploy to your Cloudflare account
```

Bindings used (declared in `wrangler.jsonc`):

- `AI` — Workers AI
- `MUSIC_JOB` — Durable Object class `MusicJob`
- `ASSETS` — static assets from `./public/`

## Job lifecycle

- One synchronous attempt per job, 30-minute timeout — no automatic retries.
- Frontend polls every 3 seconds for up to 32 minutes, with the job ID persisted in the URL hash so a refresh reconnects to an in-flight job.
- Job records (including the audio URL) live in Durable Object storage for one hour, then are removed by an alarm.
- Audio is proxied with `Cache-Control: no-store` — nothing is persisted to R2, KV, or D1.

## License

Apache 2.0 — see [LICENSE](LICENSE).
