# MiniMax Music 2.6 Demo

A minimal Cloudflare Worker that generates songs with the [MiniMax Music 2.6](https://developers.cloudflare.com/ai/models/minimax/music-2.6/) model on Workers AI, served behind a tiny single-page frontend.

Generation runs asynchronously: the Worker hands off to a Durable Object, which calls the model through an AI Gateway and copies the finished audio to R2 for one hour. The browser polls a status endpoint and streams the finished audio through the Worker.

## Architecture

```
public/index.html       Single-page form + status polling
src/index.ts            Worker routes + MusicJob Durable Object
wrangler.jsonc          AI binding, DO binding, R2 binding, asset binding
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

Validation follows the Cloudflare Workers AI schema for `minimax/music-2.6`:

- `prompt` is required and must be 1-2000 characters after trimming.
- `lyrics` is optional for this demo. When provided for non-instrumental jobs, it must be 1-3500 characters after trimming.
- `format` is optional and defaults to `mp3`; if provided, it must be `mp3` or `wav`.
- Leaving `lyrics` blank on a non-instrumental job enables `lyrics_optimizer`; instrumental jobs ignore lyrics.

## Prerequisites

- A Cloudflare account with Workers AI enabled
- An [AI Gateway](https://developers.cloudflare.com/ai-gateway/) named `default` (or update `AI_GATEWAY_ID` in `wrangler.jsonc`)
- An R2 bucket named `minimax-music-demo-audio`
- Node 20+ and `pnpm`

## Setup

```bash
pnpm install
npx wrangler r2 bucket create minimax-music-demo-audio
pnpm dev          # local dev on http://localhost:8787
pnpm test         # run focused helper tests
pnpm deploy       # deploy to your Cloudflare account
```

## Public demo protection

`POST /api/generate` is rate limited by client IP to 3 accepted jobs per hour.

Bindings used (declared in `wrangler.jsonc`):

- `AI` — Workers AI
- `MUSIC_JOB` — Durable Object class `MusicJob`
- `AUDIO_BUCKET` — R2 bucket for generated audio
- `ASSETS` — static assets from `./public/`

## Job lifecycle

- One synchronous attempt per job, with a 13-minute model timeout to leave room for R2 persistence under the Durable Object alarm wall-time limit.
- A watchdog alarm marks interrupted running jobs as failed after the attempt timeout plus a short grace period.
- Frontend polls every 3 seconds for up to 15 minutes, with the job ID persisted in the URL hash and local storage so a refresh reconnects to the same job.
- Job records live in Durable Object storage for one hour, then are removed by a cleanup alarm.
- Finished audio is copied to R2, served through `/api/audio/:jobId` with byte-range support, and deleted when the job record expires.

## License

Apache 2.0 — see [LICENSE](LICENSE).
