--- The official TopStats Analytics SDK for Lua.
---
--- Lua has no standard HTTP, JSON, or timers, so everything platform-shaped
--- is injected: the same core runs inside FiveM on its natives, inside
--- Garry's Mod on GLua's, and inside plain Lua under the test suite. The
--- platform integrations vendor this file; its canonical home is
--- https://github.com/top-stats/analytics-lua-sdk.
---
--- Mirrors the semantics every TopStats SDK shares: buffered capture that
--- never raises into caller code, batch splitting at 500 events and the
--- 2 MiB body limit, oversized events dropped before sending, retries with
--- jittered backoff on 429 and 5xx only, and a bounded drop-oldest queue.

local MAX_BATCH_SIZE = 500
local MAX_EVENT_BYTES = 65536
local MAX_BODY_BYTES = 2097152
-- {"events":[ plus ]} around the comma-joined events.
local WRAPPER_BYTES = 13

local MAX_NAME_LENGTH = 128
local MAX_PROPERTY_KEY_LENGTH = 128
local MAX_SOURCE_LENGTH = 128
local MAX_ACTOR_LENGTH = 256
local MAX_ACTOR_LABEL_LENGTH = 256

local INITIAL_RETRY_DELAY_MS = 500
local MAX_RETRY_DELAY_MS = 30000
local MAX_RETRY_AFTER_MS = 60000

local DEFAULT_HOST = 'https://topstats.gg'

local TopStatsCore = {}
TopStatsCore.__index = TopStatsCore

local function trim(value)
    return (value:gsub('^%s+', ''):gsub('%s+$', ''))
end

--- platform must provide:
---   http(url, body, headers, callback(status, response_body, retry_after))
---   set_timeout(ms, fn)
---   now_iso()  -> Z-suffixed ISO 8601 UTC string
---   json_encode(value) -> string
---   log(message)
---   random() -> [0, 1)
function TopStatsCore.new(platform, options)
    local api_key = options.api_key

    if type(api_key) ~= 'string' or trim(api_key) == '' then
        return nil, 'an API key is required'
    end

    -- Explicit host wins, then a non-blank TOPSTATS_HOST when the platform
    -- exposes environment variables, then the default. A blank value is what
    -- an unset variable looks like, so it must not be treated as a host.
    -- Checked one by one rather than via a candidate table: a nil first
    -- entry would leave a hole that ipairs stops at.
    local function usable_host(candidate)
        return type(candidate) == 'string' and trim(candidate) ~= ''
    end

    local getenv = platform.getenv or os.getenv
    local host = DEFAULT_HOST

    if usable_host(options.host) then
        host = trim(options.host):gsub('/+$', '')
    elseif usable_host(getenv('TOPSTATS_HOST')) then
        host = trim(getenv('TOPSTATS_HOST')):gsub('/+$', '')
    end

    local self = setmetatable({
        platform = platform,
        api_key = trim(api_key),
        events_url = host .. '/v1/events',
        default_source = options.default_source,
        flush_at = math.max(1, options.flush_at or 20),
        flush_interval_ms = math.max(1000, (options.flush_seconds or 5) * 1000),
        max_retries = math.max(0, options.max_retries or 3),
        max_queue_size = math.max(1, options.max_queue_size or 10000),
        queue = {},
        shut_down = false,
        sending = false,
    }, TopStatsCore)

    self:schedule_flush()
    return self
end

function TopStatsCore:schedule_flush()
    self.platform.set_timeout(self.flush_interval_ms, function()
        if self.shut_down then
            return
        end

        self:flush()
        self:schedule_flush()
    end)
end

local function put_string(target, field, value, limit)
    if value == nil then
        return true
    end

    if type(value) ~= 'string' or #value > limit or value == '' then
        return false, field .. ' must be a non-empty string of at most ' .. limit .. ' characters'
    end

    target[field] = value
    return true
end

--- Buffers one event. Never raises; problems are logged and the event is
--- dropped. context keys: actor, actor_label, source, timestamp (a wire
--- string; unset events are stamped at capture time).
function TopStatsCore:track(name, properties, context)
    if self.shut_down then
        return
    end

    context = context or {}

    if type(name) ~= 'string' or name == '' or #name > MAX_NAME_LENGTH then
        self.platform.log('event name must be 1 to ' .. MAX_NAME_LENGTH .. ' characters; dropped')
        return
    end

    local wire = { name = name }

    -- An empty Lua table would encode as [] and the API rejects a
    -- non-object properties field, so empties are omitted entirely.
    if properties ~= nil and type(properties) == 'table' and next(properties) ~= nil then
        for key in pairs(properties) do
            if type(key) ~= 'string' or key == '' or #key > MAX_PROPERTY_KEY_LENGTH then
                self.platform.log('property keys must be strings of 1 to '
                    .. MAX_PROPERTY_KEY_LENGTH .. ' characters; event dropped')
                return
            end
        end

        wire.properties = properties
    end

    local ok, problem = put_string(wire, '_source', context.source or self.default_source, MAX_SOURCE_LENGTH)

    if ok then
        ok, problem = put_string(wire, '_actor', context.actor, MAX_ACTOR_LENGTH)
    end

    if ok then
        ok, problem = put_string(wire, '_actorLabel', context.actor_label, MAX_ACTOR_LABEL_LENGTH)
    end

    if not ok then
        self.platform.log(problem .. '; event dropped')
        return
    end

    -- Stamp unset timestamps at capture time, not send time, so an event
    -- that waits in the buffer keeps the moment it actually happened.
    wire._timestamp = context.timestamp or self.platform.now_iso()

    local encoded = self.platform.json_encode(wire)

    if encoded == nil then
        self.platform.log('event does not serialise to JSON; dropped')
        return
    end

    if #encoded > MAX_EVENT_BYTES then
        self.platform.log('event "' .. name .. '" is ' .. #encoded
            .. ' bytes, over the ' .. MAX_EVENT_BYTES .. ' byte limit; dropped')
        return
    end

    while #self.queue >= self.max_queue_size do
        table.remove(self.queue, 1)
        self.platform.log('queue full; dropped the oldest buffered event')
    end

    self.queue[#self.queue + 1] = encoded

    if #self.queue >= self.flush_at then
        self:flush()
    end
end

--- Splits the queued events into request bodies at the batch-size cap and
--- the body byte limit, whichever hits first.
local function build_batches(queue)
    local batches = {}
    local current = {}
    local current_bytes = WRAPPER_BYTES

    for _, encoded in ipairs(queue) do
        local separator = 0

        if #current > 0 then
            separator = 1
        end

        local projected = current_bytes + separator + #encoded

        if #current > 0 and (#current >= MAX_BATCH_SIZE or projected > MAX_BODY_BYTES) then
            batches[#batches + 1] = '{"events":[' .. table.concat(current, ',') .. ']}'
            current = {}
            current_bytes = WRAPPER_BYTES
        end

        if #current > 0 then
            current_bytes = current_bytes + 1
        end

        current_bytes = current_bytes + #encoded
        current[#current + 1] = encoded
    end

    if #current > 0 then
        batches[#batches + 1] = '{"events":[' .. table.concat(current, ',') .. ']}'
    end

    return batches
end

function TopStatsCore:flush()
    if #self.queue == 0 then
        return
    end

    local batches = build_batches(self.queue)
    self.queue = {}

    for _, body in ipairs(batches) do
        self:send(body, 0)
    end
end

--- Whether the transport retries this failure: 429, 5xx, and network errors
--- (status 0 or negative from PerformHttpRequest) only. 400, 401, 402, and
--- 413 are permanent.
local function is_retryable(status)
    return status <= 0 or status == 429 or status >= 500
end

function TopStatsCore:retry_delay_ms(attempt, retry_after)
    if retry_after ~= nil then
        local seconds = tonumber(retry_after)

        if seconds ~= nil and seconds >= 0 then
            return math.min(math.floor(seconds * 1000), MAX_RETRY_AFTER_MS)
        end
    end

    -- Half the window is fixed and half is random. The rate limit is keyed
    -- on the client address, so servers behind one egress IP hit the same
    -- 429 together; without jitter they would retry in lockstep.
    local ceiling = math.min(INITIAL_RETRY_DELAY_MS * (2 ^ math.min(attempt, 16)), MAX_RETRY_DELAY_MS)
    local half = ceiling / 2
    return math.floor(half + self.platform.random() * half)
end

function TopStatsCore:send(body, attempt)
    local headers = {
        ['Authorization'] = 'Bearer ' .. self.api_key,
        ['Content-Type'] = 'application/json',
    }

    self.platform.http(self.events_url, body, headers, function(status, _, retry_after)
        if status >= 200 and status < 300 then
            return
        end

        if is_retryable(status) and attempt < self.max_retries then
            self.platform.set_timeout(self:retry_delay_ms(attempt, retry_after), function()
                self:send(body, attempt + 1)
            end)
            return
        end

        -- The message never includes the key or the body, so nothing
        -- sensitive can reach the console.
        self.platform.log('send failed with status ' .. status .. '; giving up on this batch')
    end)
end

--- Flushes the tail and refuses further tracking. Safe to call twice.
function TopStatsCore:shutdown()
    if self.shut_down then
        return
    end

    self:flush()
    self.shut_down = true
end

-- FiveM loads server_scripts into a shared resource environment with no
-- module system, so the global carries the core to main.lua; the return
-- serves require() in the test suite.
_G.TopStatsCore = TopStatsCore

return TopStatsCore
