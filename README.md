# Asana Extractor

A Go service that periodically fetches data from the Asana API and saves it locally as JSON files. It runs on a CRON-like schedule, handles pagination, respects rate limits, and tracks what it has already processed so it doesn't repeat work across restarts.

## How It Works

The service starts an HTTP server on `:8080` and kicks off a scheduler that runs fetch jobs for each data type (currently `projects` and `users`) on a configurable interval — 5 minutes by default.

Each fetch job follows this flow:

1. **Stream GUIDs** — The Asana client paginates through the list endpoint (`/api/1.0/projects`, `/api/1.0/users`), streaming GUIDs into a channel as they arrive. This keeps memory flat regardless of how many items exist.

2. **Worker pool picks them up** — A pool of workers (max 5 concurrent by default) reads from that channel. For each GUID, a worker first checks the in-memory cache to see if it was already processed. If it was, the item gets skipped.

3. **Fetch full details** — For new items, the worker makes a second API call to get the full entity JSON (`/api/1.0/projects/{guid}`). Failed requests are retried with exponential backoff (up to 3 retries). 404s are silently skipped.

4. **Save to disk** — The raw JSON response is pretty-printed and written to `./exports/{data_type}/{guid}.json`. The storage layer uses per-directory mutexes so concurrent workers don't step on each other.

5. **Mark as processed** — The GUID is added to a set in the local cache so subsequent runs skip it. The cache is persisted to `cache.gob` on shutdown and loaded back on startup.

## Project Structure

```
asana-extractor/
  cmd/api/main.go            - Entrypoint: wires everything together, starts HTTP server + scheduler
  internal/
    asana/
      client.go              - HTTP client for Asana API (pagination, streaming, rate limiting)
      types.go               - API response types (GetItemResponse, ResponseItem, NextPage, APIError)
    cache/
      interface.go           - Cache interface (Redis-compatible API: Get/Set/SetAdd/SetIsMember)
      local_cache.go         - In-memory implementation with TTL, cleanup goroutine, gob persistence
    config/
      config.go              - Thread-safe config with JSON serialization, env var loading, runtime updates
    domain/
      base.go                - Base entity with GUID, created/modified timestamps
      project.go             - Project model
      user.go                - User model
    handlers/
      handlers.go            - HTTP handlers for the management API
    limiter/
      limiter.go             - Token bucket rate limiter + adaptive variant with backoff/recovery
    scheduler/
      scheduler.go           - CRON-like scheduler with per-job state tracking, duplicate prevention
    storage/
      file_storage.go        - File-based JSON storage with per-directory locking
    testutil/
      mocks.go               - Mock implementations for all interfaces (fetcher, streamer, limiter)
    worker/
      pool.go                - Concurrent worker pool with semaphore, retry logic, stats tracking
  pkg/logger/
    logger.go                - Structured logger (slog wrapper)
  exports/                   - Output directory for extracted JSON files
```

## Configuration

The service loads configuration in this order:

1. Hardcoded defaults (see below)
2. `ASANA_API` environment variable for the auth token
3. `config.json` file if present (overrides defaults)
4. Runtime updates via HTTP API (persisted to `config.json` on shutdown)

### Defaults

| Setting                | Default                    |
|------------------------|----------------------------|
| Base API URL           | `https://app.asana.com/`   |
| CRON interval          | 5 minutes (per data type)  |
| Rate limit             | 10 req/s, burst of 5       |
| Page size              | 100                        |
| Max concurrent workers | 5                          |
| Exports path           | `./exports`                |

### Environment Variables

| Variable    | Description       |
|-------------|-------------------|
| `ASANA_API` | Asana API token   |

You can also put this in a `.env` file — the token is read via `os.Getenv` at startup.

## API Endpoints

All responses follow `{"success": bool, "data": ..., "error": "..."}` format.

### Health & Config
- `GET /health` — Health check
- `GET /config` — Current configuration (token is masked)

### Token Management
- `PUT /api/token` — Update API token (`{"token": "..."}`)
- `GET /api/token/status` — Check if token is set (shows masked hint)

### CRON Intervals
- `GET /api/cron/intervals` — Current intervals for all data types
- `PUT /api/cron/interval` — Update interval (`{"data_type": "projects", "interval": "10m"}`, min 1m)

### Rate Limiting
- `GET /api/rate-limit` — Current rate limit settings
- `PUT /api/rate-limit` — Update limits (`{"per_second": 20, "burst": 10}`)

### Job Management
- `POST /api/jobs/trigger` — Manually trigger a fetch job (`{"data_type": "projects"}`)
- `GET /api/jobs/status` — All job statuses (or `?data_type=projects` for one)

### Stats
- `GET /api/stats/storage` — File counts and sizes per data type
- `GET /api/stats/workers` — Worker pool stats (processed, skipped, failed, in-progress)

## Rate Limiting

The rate limiter uses a token bucket algorithm. On top of that, there is an adaptive layer:

- On a **429 response**, the rate and burst are cut in half (down to a floor of 1). The `Retry-After` header is respected if present.
- On **10 consecutive successes**, the rate nudges back up by 10% (guaranteed at least +1), capped at the original configured value.

This means the service automatically backs off when Asana pushes back and gradually recovers once things are stable.

## Caching & Deduplication

The local cache implements a Redis-like interface (`Get`, `Set`, `SetAdd`, `SetIsMember`, etc.) backed by a simple `map` with mutex protection. It supports:

- TTL-based expiration with periodic cleanup
- Set operations for tracking processed GUIDs
- Gob-based persistence to disk (`cache.gob`)

On each run, the worker pool checks `SetIsMember` before fetching an item. This means restarting the service won't re-fetch everything — the cache file survives restarts.

## Concurrency

- **Worker pool**: Uses a channel-based semaphore (`make(chan struct{}, maxWorkers)`) to limit concurrent API calls. Each item is processed in its own goroutine, but the semaphore ensures no more than `maxWorkers` run at once.
- **Scheduler**: Each job tracks its running state with `sync/atomic.CompareAndSwapInt32` to prevent duplicate executions — no TOCTOU race.
- **Storage**: Per-directory `sync.RWMutex` so saves to different data types don't block each other.
- **Config**: `sync.RWMutex` on all getters/setters so runtime updates are safe.

## Running

```bash
# Set your token
export ASANA_API="your-asana-api-token"

# Build and run
go build -o asana-extractor ./cmd/api
./asana-extractor

# Or just
go run ./cmd/api
```

The server starts on `:8080`. Fetch jobs begin immediately and repeat on the configured interval.

## Testing

```bash
go test ./... -v
```

Every package has tests. The test suite uses mock implementations for the HTTP client, rate limiter, entity fetcher, and GUID streamer — no real API calls are made. Tests cover:

- Asana client pagination, error handling, streaming, rate limit errors
- Cache set/get/TTL/expiration/persistence
- Config defaults, file load/save, thread-safe access
- Handler HTTP status codes and response shapes for all endpoints
- Limiter token bucket behavior, adaptive backoff, recovery, minimum rates
- Scheduler job lifecycle, duplicate prevention, failure handling, start/stop
- Storage save/load/delete/list, concurrent access, stats
- Worker pool processing, skip logic, retry with backoff, concurrency limits, context cancellation

## Used Claude with commands:

### Command 1 — Initial Architecture

> you are a senior Go developer (you use go 1.25) and you are tasked to create me an application that fetches data from Asana like a CRON type with configurable intervals, it should be a paginated response (Asana type) with `next_page` and `offset` as the page token. The application should create individual JSON files for each item in the response. Each file should be named by the GID of the item. The first request to the Asana API is to get all items, from there we get each individual item and save it as a JSON file. The application should work in parallel with max 5 concurrent fetches for the individual items. The application should have a local cache (like redis but a local cache in Go implementing a set) to track already processed items, so that on the next CRON execution, we skip items that have already been processed. The application should handle rate limiting from the API.


### Command 2 — Integration Review

> ok, now we have to integrate to the existing flow the following changes and implementation:
we should call a get user and a get project endpoint by passing at the end of the url the id we have extracted on getting paginated users and projects , so in the json files we would save entire element instead of just id and type that we extracted earlier, those extracted at get paginated we dont need saved. Also we should implement testing for entire functionality , provide only the needed to make changes to the existing project .
