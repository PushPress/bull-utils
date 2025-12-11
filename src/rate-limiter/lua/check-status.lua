-- Check rate limit status without incrementing
-- KEYS[1] = rate limit key
-- ARGV[1] = limit (unused, kept for consistency)
-- Returns: [current_count, ttl_remaining]
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local ttl = redis.call('TTL', KEYS[1])
if ttl == -2 then
  ttl = 0
end
return {current, ttl}



