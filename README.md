# TopStats Analytics for Lua

The official Lua client for [TopStats Analytics](https://topstats.gg), built
for the game platforms where Lua lives. Lua has no standard HTTP, JSON, or
timers, so this SDK is a pure core with everything platform-shaped injected:
the same file runs inside FiveM on its natives, inside Garry's Mod on GLua's,
and inside plain Lua under the test suite.

Platform integrations vendor `src/topstats.lua`; this repository is its
canonical home, test suite, and contract reference. It is deliberately a
single dependency-free file.

## Usage

```lua
local TopStats = require('topstats')

local core, problem = TopStats.new(platform, {
    api_key = 'ts_live_your_key',
    default_source = 'my-game',
})

core:track('mission_completed', { payout = 500 }, {
    actor = 'license:abc',
    actor_label = 'Ada',
})

core:flush()
core:shutdown()
```

## The platform table

Everything the host environment must provide:

| Field | Signature | FiveM example |
| --- | --- | --- |
| `http` | `(url, body, headers, callback(status, body, retry_after))` | `PerformHttpRequest` |
| `set_timeout` | `(ms, fn)` | `SetTimeout` |
| `now_iso` | `() -> Z-suffixed ISO 8601 UTC string` | `os.date('!%Y-%m-%dT%H:%M:%SZ')` |
| `json_encode` | `(value) -> string or nil` | `json.encode` |
| `log` | `(message)` | `print` |
| `random` | `() -> [0, 1)` | `math.random` |
| `getenv` | optional `(name) -> string or nil` | `os.getenv` |

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `api_key` | required | Your workspace API key. |
| `host` | `https://topstats.gg` | API origin. Also the `TOPSTATS_HOST` env var; a blank value is treated as unset. |
| `default_source` | unset | `_source` applied to events that do not set their own. |
| `flush_at` | 20 | Buffered events that trigger a send. |
| `flush_seconds` | 5 | Timer flush period. |
| `max_retries` | 3 | Retries after the first attempt, for 429, 5xx, and network errors only. |
| `max_queue_size` | 10000 | Buffer bound. When full, the oldest events are dropped and logged. |

## Behaviour

The shared TopStats SDK semantics: `track` buffers and never raises into
caller code, batches split at 500 events and 2 MiB per request, an event
over 65536 bytes is dropped and logged rather than sent, failed sends retry
with jittered backoff honouring `Retry-After`, 400, 401, 402, and 413 are
never retried, and the API key never appears in any log line.

Timestamps are second precision (`2026-01-01T00:00:00Z`): portable Lua has
no millisecond clock, and the API accepts the fraction-free form.

## Compatibility

CI runs the suite on Lua 5.1 and 5.4, which brackets GLua (5.1 lineage) and
FiveM (5.4). The source avoids everything outside that intersection.

Full product documentation: <https://docs.topstats.gg/docs/analytics>
