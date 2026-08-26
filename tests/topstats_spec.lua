--- The suite drives the SDK through a fake platform: HTTP calls are
--- recorded and answered from a script, timers are captured for manual
--- firing, and JSON comes from dkjson. Nothing here touches the network.

local dkjson = require('dkjson')
local TopStats = require('src.topstats')

local function fake_platform(script)
    local platform = {
        requests = {},
        timers = {},
        logs = {},
        script = script or {},
        env = {},
    }

    platform.http = function(url, body, headers, callback)
        platform.requests[#platform.requests + 1] = {
            url = url,
            body = body,
            headers = headers,
        }

        local step = table.remove(platform.script, 1) or { status = 202 }
        callback(step.status, step.body or '', step.retry_after)
    end

    platform.set_timeout = function(ms, fn)
        platform.timers[#platform.timers + 1] = { ms = ms, fn = fn }
    end

    platform.now_iso = function()
        return '2026-01-01T00:00:00Z'
    end

    platform.json_encode = function(value)
        return dkjson.encode(value)
    end

    platform.log = function(message)
        platform.logs[#platform.logs + 1] = message
    end

    platform.random = function()
        return 0.5
    end

    platform.getenv = function(name)
        return platform.env[name]
    end

    return platform
end

local API_KEY = 'ts_test_fake_key_for_unit_tests_only'

local function client(platform, overrides)
    local options = { api_key = API_KEY, flush_at = 1000 }

    for key, value in pairs(overrides or {}) do
        options[key] = value
    end

    local core, problem = TopStats.new(platform, options)
    assert.is_nil(problem)
    return core
end

local function decoded_events(request)
    local parsed = dkjson.decode(request.body)
    return parsed.events
end

--- Fires every retry timer queued so far (the flush timer sits at index 1
--- and is left alone).
local function run_retry_timers(platform)
    while #platform.timers > 1 do
        local timer = table.remove(platform.timers, 2)
        timer.fn()
    end
end

describe('the TopStats Lua SDK', function()
    it('sends the batch shape with auth headers and context fields', function()
        local platform = fake_platform()
        local core = client(platform, { default_source = 'fivem' })

        core:track('mission_completed', { payout = 500 }, {
            actor = 'license:abc',
            actor_label = 'Ada',
        })
        core:flush()

        assert.equal(1, #platform.requests)
        local request = platform.requests[1]
        assert.equal('https://topstats.gg/v1/events', request.url)
        assert.equal('Bearer ' .. API_KEY, request.headers['Authorization'])
        assert.equal('application/json', request.headers['Content-Type'])

        local events = decoded_events(request)
        assert.equal(1, #events)
        assert.equal('mission_completed', events[1].name)
        assert.equal(500, events[1].properties.payout)
        assert.equal('license:abc', events[1]._actor)
        assert.equal('Ada', events[1]._actorLabel)
        assert.equal('fivem', events[1]._source)
        assert.equal('2026-01-01T00:00:00Z', events[1]._timestamp)
    end)

    it('omits empty properties so they cannot encode as an array', function()
        local platform = fake_platform()
        local core = client(platform)

        core:track('bare', {})
        core:flush()

        local events = decoded_events(platform.requests[1])
        assert.is_nil(events[1].properties)
        assert.is_not_nil(events[1]._timestamp)
        assert.is_nil(events[1]._actor)
    end)

    it('splits batches at the event count cap', function()
        local platform = fake_platform()
        local core = client(platform)

        for _ = 1, 501 do
            core:track('tick')
        end

        core:flush()

        assert.equal(2, #platform.requests)
        assert.equal(500, #decoded_events(platform.requests[1]))
        assert.equal(1, #decoded_events(platform.requests[2]))
    end)

    it('splits batches at the body byte limit', function()
        local platform = fake_platform()
        local core = client(platform)
        local payload = string.rep('x', 60000)

        for _ = 1, 40 do
            core:track('big', { blob = payload })
        end

        core:flush()

        assert.is_true(#platform.requests >= 2)

        for _, request in ipairs(platform.requests) do
            assert.is_true(#request.body <= 2097152)
        end
    end)

    it('drops an oversized event and reports it, never sending it', function()
        local platform = fake_platform()
        local core = client(platform)

        core:track('huge', { blob = string.rep('x', 70000) })
        core:flush()

        assert.equal(0, #platform.requests)
        assert.is_truthy(platform.logs[1]:find('huge'))
        assert.is_truthy(platform.logs[1]:find('dropped'))
    end)

    it('never raises on bad input and drops the oldest on overflow', function()
        local platform = fake_platform()
        local core = client(platform, { max_queue_size = 2 })

        core:track('')
        core:track(string.rep('n', 200))
        core:track('event', { [''] = 1 })
        core:track('event', nil, { actor = string.rep('a', 300) })
        assert.equal(0, #platform.requests)
        assert.equal(4, #platform.logs)

        core:track('first')
        core:track('second')
        core:track('third')
        core:flush()

        local body = platform.requests[1].body
        assert.is_nil(body:find('"first"'))
        assert.is_truthy(body:find('"second"'))
        assert.is_truthy(body:find('"third"'))
    end)

    it('retries 429, 5xx, and network failures with backoff', function()
        local platform = fake_platform({
            { status = 429 },
            { status = 0 },
            { status = 503 },
            { status = 202 },
        })
        local core = client(platform)

        core:track('event')
        core:flush()
        run_retry_timers(platform)
        run_retry_timers(platform)
        run_retry_timers(platform)

        assert.equal(4, #platform.requests)
        assert.equal(0, #platform.logs)
    end)

    it('honours Retry-After and never retries permanent statuses', function()
        local platform = fake_platform({
            { status = 429, retry_after = '7' },
            { status = 202 },
        })
        local core = client(platform)

        core:track('event')
        core:flush()

        assert.equal(7000, platform.timers[2].ms)
        platform.timers[2].fn()
        assert.equal(2, #platform.requests)

        for _, status in ipairs({ 400, 401, 402, 413 }) do
            local permanent = fake_platform({ { status = status } })
            local permanent_core = client(permanent)

            permanent_core:track('event')
            permanent_core:flush()
            run_retry_timers(permanent)

            assert.equal(1, #permanent.requests, 'status ' .. status .. ' must not retry')
            assert.is_truthy(permanent.logs[1]:find(tostring(status)))
        end
    end)

    it('gives up after max retries and reports without leaking the key', function()
        local platform = fake_platform({
            { status = 503 },
            { status = 503 },
            { status = 503 },
            { status = 503 },
        })
        local core = client(platform)

        core:track('event')
        core:flush()
        run_retry_timers(platform)
        run_retry_timers(platform)
        run_retry_timers(platform)

        -- 1 initial + 3 retries.
        assert.equal(4, #platform.requests)
        assert.equal(1, #platform.logs)
        assert.is_nil(platform.logs[1]:find(API_KEY))
    end)

    it('flushes at the threshold without an explicit flush', function()
        local platform = fake_platform()
        local core = client(platform, { flush_at = 2 })

        core:track('one')
        core:track('two')

        assert.equal(1, #platform.requests)
        assert.equal(2, #decoded_events(platform.requests[1]))
    end)

    it('shutdown flushes the tail, refuses later tracking, and is idempotent', function()
        local platform = fake_platform()
        local core = client(platform)

        core:track('event')
        core:shutdown()
        core:shutdown()
        core:track('late')
        core:flush()

        assert.equal(1, #platform.requests)
    end)

    it('resolves the host from the option, then the env var, then the default', function()
        local explicit = fake_platform()
        local explicit_core = client(explicit, { host = 'https://staging.example.com/' })
        explicit_core:track('event')
        explicit_core:flush()
        assert.equal('https://staging.example.com/v1/events', explicit.requests[1].url)

        local from_env = fake_platform()
        from_env.env.TOPSTATS_HOST = 'https://self-hosted.example.com'
        local env_core = client(from_env)
        env_core:track('event')
        env_core:flush()
        assert.equal('https://self-hosted.example.com/v1/events', from_env.requests[1].url)

        local blank_env = fake_platform()
        blank_env.env.TOPSTATS_HOST = '   '
        local default_core = client(blank_env)
        default_core:track('event')
        default_core:flush()
        assert.equal('https://topstats.gg/v1/events', blank_env.requests[1].url)
    end)

    it('starts on a runtime with no os.getenv at all', function()
        local platform = fake_platform()
        platform.getenv = nil
        local saved = os.getenv
        os.getenv = nil -- luacheck: ignore 122

        local core, problem = TopStats.new(platform, { api_key = API_KEY })
        os.getenv = saved -- luacheck: ignore 122

        assert.is_nil(problem)
        assert.is_not_nil(core)
    end)

    it('refuses to start without an api key', function()
        local core, problem = TopStats.new(fake_platform(), { api_key = '   ' })
        assert.is_nil(core)
        assert.is_truthy(problem:find('API key'))
    end)
end)
