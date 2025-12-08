import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Redis } from 'ioredis';
import type { Logger } from '../logger';

/**
 * Get the directory path of the current module
 * Works in both ESM and CJS contexts
 */
function getModuleDir(): string {
  if (typeof __dirname !== 'undefined') {
    // CJS context
    return __dirname;
  }
  // ESM context
  return dirname(fileURLToPath(import.meta.url));
}

/**
 * Load a Lua script from the lua directory
 * Handles both development and production contexts
 */
function loadLuaScript(filename: string): string {
  const moduleDir = getModuleDir();

  // Try to find package root by going up from dist or src
  // dist/rate-limiter -> package root -> lua/rate-limiter
  // src/rate-limiter -> package root -> lua/rate-limiter
  const packageRoot = join(moduleDir, '..', '..');
  const luaDir = join(packageRoot, 'lua', 'rate-limiter');
  const scriptPath = join(luaDir, filename);

  return readFileSync(scriptPath, 'utf-8');
}

/**
 * Configuration for the RateLimiter
 */
export interface RateLimiterConfig {
  /** Redis connection instance */
  redis: Redis;
  /** Maximum number of requests allowed per window */
  limit: number;
  /** Window duration in milliseconds */
  windowMs: number;
  /** Optional Redis key prefix for namespacing */
  keyPrefix?: string;
  /** Optional logger for debugging */
  logger?: Logger;
}

/**
 * Result of a successful rate limit acquisition
 */
export interface AcquireSuccess {
  acquired: true;
  /** Number of tokens remaining in the current window */
  remainingTokens: number;
  /** Current count of requests in this window */
  currentCount: number;
}

export interface AcquireFailure {
  acquired: false;
  /** Remaining seconds until the window expires and the lock is free */
  ttlSeconds: number;
}

export type AcquireResult = AcquireSuccess | AcquireFailure;

/**
 * Result returned from checking rate limit status
 */
export interface RateLimitStatus {
  /** Current count of requests in this window */
  currentCount: number;
  /** Maximum allowed requests per window */
  limit: number;
  /** Whether the rate limit has been exceeded */
  isLimited: boolean;
  /** Milliseconds until the current window resets */
  resetInMs: number;
  /** Number of tokens remaining in the current window */
  remainingTokens: number;
}

/**
 * Lua script for atomic check-and-increment operation.
 * Evaluating a lua script directly ensures no race conditions happen
 * as multiple processes may be attempting to increment the same key
 *
 * KEYS[1] = rate limit key
 * ARGV[1] = limit
 * ARGV[2] = window TTL in seconds
 *
 * Returns: [current_count, is_allowed (0 or 1), ttl_remaining]
 */
const CHECK_AND_INCREMENT_SCRIPT = loadLuaScript('check-and-increment.lua');

/**
 * Lua script for checking rate limit status without incrementing.
 *
 * KEYS[1] = rate limit key
 * ARGV[1] = limit (unused, kept for consistency)
 *
 * Returns: [current_count, ttl_remaining]
 */
const CHECK_STATUS_SCRIPT = loadLuaScript('check-status.lua');

const DEFAULT_KEY_PREFIX = 'bull-utils';

/**
 * A Redis-backed global rate limiter using fixed window algorithm.
 *
 * This rate limiter provides distributed rate limiting across multiple
 * processes/servers by using Redis as the shared state store. It uses
 * a fixed window algorithm where requests are counted within discrete
 * time windows.
 *
 * @example
 * ```typescript
 * const rateLimiter = new RateLimiter({
 *   redis,
 *   limit: 100,        // 100 requests
 *   windowMs: 60000,   // per minute
 *   keyPrefix: 'myapp',
 * });
 *
 * // Blocking acquisition - waits if rate limited
 * const result = await rateLimiter.acquire('tenant-123');
 * console.log(`Acquired, ${result.remainingTokens} tokens left`);
 *
 * // Non-blocking status check
 * const status = await rateLimiter.check('tenant-123');
 * if (status.isLimited) {
 *   console.log(`Rate limited, resets in ${status.resetInMs}ms`);
 * }
 * ```
 */
export class RateLimiter {
  readonly #redis: Redis;
  readonly #limit: number;
  readonly #windowMs: number;
  readonly #keyPrefix: string;
  readonly #logger?: Logger;
  readonly #windowSeconds: number;

  constructor(config: RateLimiterConfig) {
    if (config.limit <= 0) {
      throw new Error('limit must be positive');
    }
    if (config.windowMs <= 0) {
      throw new Error('windowMs must be positive');
    }

    this.#redis = config.redis;
    this.#limit = config.limit;
    this.#windowMs = config.windowMs;
    this.#keyPrefix = config.keyPrefix ?? DEFAULT_KEY_PREFIX;
    this.#logger = config.logger;
    this.#windowSeconds = Math.ceil(this.#windowMs / 1000);
  }

  /**
   * Get the Redis key for a given group key.
   * Uses the current window timestamp to create time-bounded keys.
   */
  #getKey(groupKey: string): string {
    const windowTimestamp = this.#getCurrentWindowTimestamp();
    return `${this.#keyPrefix}:ratelimit:${groupKey}:${windowTimestamp}`;
  }

  /**
   * Calculate the current window timestamp.
   */
  #getCurrentWindowTimestamp(): number {
    return Math.floor(Date.now() / this.#windowMs) * this.#windowMs;
  }

  /**
   * Calculate milliseconds until the next window starts.
   */
  getTimeUntilNextWindow(): number {
    const now = Date.now();
    const currentWindowStart = this.#getCurrentWindowTimestamp();
    const nextWindowStart = currentWindowStart + this.#windowMs;
    return nextWindowStart - now;
  }

  /**
   * Attempt to acquire a token from the rate limiter.
   *
   * This method atomically checks and increments the rate limit counter.
   * If the limit has been exceeded, it returns a failure result with the
   * TTL (time-to-live) in seconds until the current window expires.
   *
   * @param groupKey - The group key to rate limit (e.g., tenant ID, API key)
   * @returns Promise resolving to either:
   *   - `AcquireSuccess` with `acquired: true` and remaining token count
   *   - `AcquireFailure` with `acquired: false` and TTL until window resets
   *
   * @example
   * ```typescript
   * const result = await rateLimiter.acquire('tenant-123');
   * if (result.acquired) {
   *   console.log(`Acquired! ${result.remainingTokens} tokens remaining`);
   * } else {
   *   console.log(`Rate limited. Retry in ${result.ttlSeconds} seconds`);
   * }
   * ```
   */
  async acquire(groupKey: string): Promise<AcquireResult> {
    const key = this.#getKey(groupKey);

    const result = (await this.#redis.eval(
      CHECK_AND_INCREMENT_SCRIPT,
      1,
      key,
      this.#limit,
      this.#windowSeconds,
    )) as [number, number, number];

    const [currentCount, isAllowed, ttlSeconds] = result;

    if (isAllowed) {
      this.#logger?.debug(
        { groupKey, currentCount, limit: this.#limit },
        'Rate limit token acquired',
      );

      return {
        acquired: true,
        remainingTokens: this.#limit - currentCount,
        currentCount,
      } as const;
    }

    return {
      acquired: false,
      ttlSeconds,
    } as const;
  }

  /**
   * Check the current rate limit status without consuming a token.
   *
   * This is useful for pre-flight checks or displaying rate limit
   * information to users.
   *
   * @param groupKey - The group key to check (e.g., tenant ID, API key)
   * @returns Promise resolving to current rate limit status
   *
   * @example
   * ```typescript
   * const status = await rateLimiter.check('tenant-123');
   * if (status.isLimited) {
   *   console.log(`Rate limited! Resets in ${status.resetInMs}ms`);
   * } else {
   *   console.log(`${status.remainingTokens} tokens available`);
   * }
   * ```
   */
  async check(groupKey: string): Promise<RateLimitStatus> {
    const key = this.#getKey(groupKey);

    const result = (await this.#redis.eval(
      CHECK_STATUS_SCRIPT,
      1,
      key,
      this.#limit,
    )) as [number, number];

    const [currentCount, ttlSeconds] = result;
    const resetInMs =
      ttlSeconds > 0 ? ttlSeconds * 1000 : this.getTimeUntilNextWindow();

    return {
      currentCount,
      limit: this.#limit,
      isLimited: currentCount >= this.#limit,
      resetInMs,
      remainingTokens: Math.max(0, this.#limit - currentCount),
    };
  }

  /**
   * Reset the rate limit for a specific group key.
   *
   * This removes the current window's count, effectively giving
   * the group a fresh set of tokens. Useful for testing or
   * administrative purposes.
   *
   * @param groupKey - The group key to reset (e.g., tenant ID, API key)
   *
   * @example
   * ```typescript
   * // Reset a specific tenant's rate limit
   * await rateLimiter.reset('tenant-123');
   * ```
   */
  async reset(groupKey: string): Promise<void> {
    const key = this.#getKey(groupKey);
    await this.#redis.del(key);

    this.#logger?.debug({ groupKey }, 'Rate limit reset');
  }

  /**
   * Get the configured limit.
   */
  get limit(): number {
    return this.#limit;
  }

  /**
   * Get the configured window duration in milliseconds.
   */
  get windowMs(): number {
    return this.#windowMs;
  }
}
