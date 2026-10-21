package broker

import "github.com/redis/go-redis/v9"

// LuaClaimTaskScript atomically claims the next ready task based on priority order:
// Critical > High > Default > Low. If the queue is paused or empty, it returns nil.
// If a task is claimed, it atomically adds it to the Active ZSET with score = now + visibilityTimeout
// and updates the task hash with state="active", worker_id, and heartbeat timestamp.
const LuaClaimTaskScript = `
-- KEYS[1]: paused key
-- KEYS[2]: critical ready list
-- KEYS[3]: high ready list
-- KEYS[4]: default ready list
-- KEYS[5]: low ready list
-- KEYS[6]: active zset
-- ARGV[1]: current timestamp (seconds)
-- ARGV[2]: visibility timeout (seconds)
-- ARGV[3]: worker_id
-- ARGV[4]: task key prefix (e.g. "goqueue:task:")

if redis.call('EXISTS', KEYS[1]) == 1 then
    return nil
end

local priority_queues = {KEYS[2], KEYS[3], KEYS[4], KEYS[5]}
for _, q_key in ipairs(priority_queues) do
    local task_id = redis.call('RPOP', q_key)
    if task_id then
        local now = tonumber(ARGV[1])
        local vis_timeout = tonumber(ARGV[2])
        local expire_at = now + vis_timeout
        redis.call('ZADD', KEYS[6], expire_at, task_id)
        local task_key = ARGV[4] .. task_id
        redis.call('HSET', task_key, 'state', 'active', 'worker_id', ARGV[3], 'heartbeat_at', ARGV[1])
        return task_id
    end
end

return nil
`

// LuaAckTaskScript atomically removes the task from the active lease set,
// marks the state as completed, sets completed_at, applies retention TTL,
// and increments the processed counter.
const LuaAckTaskScript = `
-- KEYS[1]: active zset
-- KEYS[2]: task hash key
-- KEYS[3]: stats hash key
-- ARGV[1]: task_id
-- ARGV[2]: completed timestamp (seconds)
-- ARGV[3]: retention TTL (seconds)

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'state', 'completed', 'completed_at', ARGV[2])
local ttl = tonumber(ARGV[3])
if ttl > 0 then
    redis.call('EXPIRE', KEYS[2], ttl)
end
redis.call('HINCRBY', KEYS[3], 'processed', 1)
return 1
`

// LuaNackTaskScript handles failed execution:
// Atomically removes task from active set. If retries remain, increments current_retry,
// updates error and stack trace, and schedules it for retry in the Scheduled ZSET.
// If retries are exhausted, it atomically moves it to the Dead-Letter Queue (DLQ)
// and updates the stats counter.
const LuaNackTaskScript = `
-- KEYS[1]: active zset
-- KEYS[2]: scheduled zset
-- KEYS[3]: dlq zset
-- KEYS[4]: task hash key
-- KEYS[5]: stats hash key
-- ARGV[1]: task_id
-- ARGV[2]: current timestamp (seconds)
-- ARGV[3]: backoff delay (seconds)
-- ARGV[4]: last error message
-- ARGV[5]: stack trace

redis.call('ZREM', KEYS[1], ARGV[1])
local current_retry = tonumber(redis.call('HGET', KEYS[4], 'current_retry') or '0')
local max_retries = tonumber(redis.call('HGET', KEYS[4], 'max_retries') or '3')
current_retry = current_retry + 1

redis.call('HSET', KEYS[4], 'current_retry', current_retry, 'last_failed_at', ARGV[2], 'last_error', ARGV[4], 'stack_trace', ARGV[5])

if current_retry <= max_retries then
    local retry_at = tonumber(ARGV[2]) + tonumber(ARGV[3])
    redis.call('HSET', KEYS[4], 'state', 'pending', 'scheduled_at', retry_at)
    redis.call('ZADD', KEYS[2], retry_at, ARGV[1])
    return 0 -- scheduled retry
else
    redis.call('HSET', KEYS[4], 'state', 'dead_lettered')
    redis.call('ZADD', KEYS[3], tonumber(ARGV[2]), ARGV[1])
    redis.call('HINCRBY', KEYS[5], 'failed', 1)
    return 1 -- dead lettered
end
`

// LuaHeartbeatScript extends the visibility timeout lease for an active task.
// If the task is no longer in the active set (e.g. lease expired and reclaimed), it returns 0.
const LuaHeartbeatScript = `
-- KEYS[1]: active zset
-- KEYS[2]: task hash key
-- ARGV[1]: task_id
-- ARGV[2]: current timestamp (seconds)
-- ARGV[3]: extend_by duration (seconds)

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then
    return 0
end

local new_expire = tonumber(ARGV[2]) + tonumber(ARGV[3])
redis.call('ZADD', KEYS[1], new_expire, ARGV[1])
redis.call('HSET', KEYS[2], 'heartbeat_at', ARGV[2])
return 1
`

// LuaScheduleDueScript finds tasks in the scheduled ZSET whose execute_at <= now,
// removes them from scheduled, and moves them into their respective ready priority list.
const LuaScheduleDueScript = `
-- KEYS[1]: scheduled zset
-- KEYS[2]: critical ready list
-- KEYS[3]: high ready list
-- KEYS[4]: default ready list
-- KEYS[5]: low ready list
-- ARGV[1]: current timestamp (seconds)
-- ARGV[2]: limit count
-- ARGV[3]: task key prefix

local due_tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local count = 0

for _, task_id in ipairs(due_tasks) do
    redis.call('ZREM', KEYS[1], task_id)
    local task_key = ARGV[3] .. task_id
    local priority = redis.call('HGET', task_key, 'priority') or 'default'
    local target_key = KEYS[4] -- default
    if priority == 'critical' then
        target_key = KEYS[2]
    elseif priority == 'high' then
        target_key = KEYS[3]
    elseif priority == 'low' then
        target_key = KEYS[5]
    end
    redis.call('LPUSH', target_key, task_id)
    redis.call('HSET', task_key, 'state', 'pending')
    count = count + 1
end

return count
`

// LuaReclaimExpiredScript finds active tasks whose lease expired (visibility timeout exceeded),
// removes them from active set, and either reschedules them with backoff or sends them to DLQ.
const LuaReclaimExpiredScript = `
-- KEYS[1]: active zset
-- KEYS[2]: scheduled zset
-- KEYS[3]: dlq zset
-- KEYS[4]: stats hash key
-- ARGV[1]: current timestamp (seconds)
-- ARGV[2]: limit count
-- ARGV[3]: default backoff delay (seconds)
-- ARGV[4]: task key prefix

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local reclaimed = 0
local deadlettered = 0

for _, task_id in ipairs(expired) do
    redis.call('ZREM', KEYS[1], task_id)
    local task_key = ARGV[4] .. task_id
    local current_retry = tonumber(redis.call('HGET', task_key, 'current_retry') or '0')
    local max_retries = tonumber(redis.call('HGET', task_key, 'max_retries') or '3')
    current_retry = current_retry + 1

    redis.call('HSET', task_key, 'current_retry', current_retry, 'last_failed_at', ARGV[1], 'last_error', 'visibility lease expired / worker timeout')
    if current_retry <= max_retries then
        local retry_at = tonumber(ARGV[1]) + tonumber(ARGV[3])
        redis.call('HSET', task_key, 'state', 'pending', 'scheduled_at', retry_at)
        redis.call('ZADD', KEYS[2], retry_at, task_id)
        reclaimed = reclaimed + 1
    else
        redis.call('HSET', task_key, 'state', 'dead_lettered')
        redis.call('ZADD', KEYS[3], tonumber(ARGV[1]), task_id)
        redis.call('HINCRBY', KEYS[4], 'failed', 1)
        deadlettered = deadlettered + 1
    end
end

return {reclaimed, deadlettered}
`

// LuaRetryDLQScript moves a dead-lettered task from the DLQ back into its priority ready queue.
const LuaRetryDLQScript = `
-- KEYS[1]: dlq zset
-- KEYS[2]: critical ready list
-- KEYS[3]: high ready list
-- KEYS[4]: default ready list
-- KEYS[5]: low ready list
-- KEYS[6]: task hash key
-- ARGV[1]: task_id

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
    return 0
end

local priority = redis.call('HGET', KEYS[6], 'priority') or 'default'
local target_key = KEYS[4]
if priority == 'critical' then
    target_key = KEYS[2]
elseif priority == 'high' then
    target_key = KEYS[3]
elseif priority == 'low' then
    target_key = KEYS[5]
end

redis.call('HSET', KEYS[6], 'current_retry', 0, 'state', 'pending', 'last_error', '', 'stack_trace', '')
redis.call('LPUSH', target_key, ARGV[1])
return 1
`

// LuaPurgeDLQScript removes all tasks from the DLQ and deletes their hashes.
const LuaPurgeDLQScript = `
-- KEYS[1]: dlq zset
-- ARGV[1]: task key prefix

local dlq_tasks = redis.call('ZRANGE', KEYS[1], 0, -1)
local count = 0
for _, task_id in ipairs(dlq_tasks) do
    redis.call('DEL', ARGV[1] .. task_id)
    count = count + 1
end
redis.call('DEL', KEYS[1])
return count
`

// ScriptManager pre-compiles and caches Redis Lua scripts.
type ScriptManager struct {
	ClaimTask      *redis.Script
	AckTask        *redis.Script
	NackTask       *redis.Script
	Heartbeat      *redis.Script
	ScheduleDue    *redis.Script
	ReclaimExpired *redis.Script
	RetryDLQ       *redis.Script
	PurgeDLQ       *redis.Script
}

// NewScriptManager initializes redis.Script instances for atomic execution.
func NewScriptManager() *ScriptManager {
	return &ScriptManager{
		ClaimTask:      redis.NewScript(LuaClaimTaskScript),
		AckTask:        redis.NewScript(LuaAckTaskScript),
		NackTask:       redis.NewScript(LuaNackTaskScript),
		Heartbeat:      redis.NewScript(LuaHeartbeatScript),
		ScheduleDue:    redis.NewScript(LuaScheduleDueScript),
		ReclaimExpired: redis.NewScript(LuaReclaimExpiredScript),
		RetryDLQ:       redis.NewScript(LuaRetryDLQScript),
		PurgeDLQ:       redis.NewScript(LuaPurgeDLQScript),
	}
}
