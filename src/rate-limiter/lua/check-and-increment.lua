-- Atomic check-and-increment operation for rate limiting
-- KEYS[1] = rate limit key
-- ARGV[1] = limit
-- ARGV[2] = window TTL in seconds
-- Returns: [current_count, is_allowed (0 or 1), ttl_remaining]
local current = redis.call('INCR', KEYS[1])
local ttl = redis.call('TTL', KEYS[1])
if current == 1 or ttl == -1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  ttl = tonumber(ARGV[2])
end
if current > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return {current - 1, 0, ttl}
end
return {current, 1, ttl}


