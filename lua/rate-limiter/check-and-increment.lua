--- Atomic check-and-increment operation for rate limiting
--- KEYS[1] = rate limit key
--- ARGV[1] = limit
--- ARGV[2] = window TTL in seconds
---@return integer current_count Current count of requests in this window
---@return integer is_allowed 1 if allowed, 0 if rate limited
---@return integer ttl_remaining Remaining TTL in seconds
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

