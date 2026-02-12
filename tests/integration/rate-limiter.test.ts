import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import Redis from "ioredis";
import { RateLimiter } from "../../src/index.js";
import { promisify } from "util";

const sleep = promisify(setTimeout);

describe("RateLimiter Integration Tests", () => {
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis({
      host: process.env.REDIS_HOST || "localhost",
      port: parseInt(process.env.REDIS_PORT || "6379"),
      db: 15, // Use a test database
      maxRetriesPerRequest: null,
    });

    // Clear the test database
    await redis.flushdb();
  });

  afterEach(async () => {
    await redis?.quit();
  });

  describe.only("RateLimiter", () => {
    it("should allow requests up to the limit", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 3,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupKey = "test-group";

      // First 3 requests should succeed immediately
      const result1 = await rateLimiter.acquire(groupKey);
      if (!result1.acquired == true) {
        throw new Error("Cannot acquire");
      }

      expect(result1.remainingTokens).toBe(2);
      expect(result1.currentCount).toBe(1);

      const result2 = await rateLimiter.acquire(groupKey);
      if (!result2.acquired == true) {
        throw new Error("cannot acquire");
      }
      expect(result2.remainingTokens).toBe(1);
      expect(result2.currentCount).toBe(2);

      const result3 = await rateLimiter.acquire(groupKey);
      if (!result3.acquired == true) {
        throw new Error("cannot acquire");
      }
      expect(result3.remainingTokens).toBe(0);
      expect(result3.currentCount).toBe(3);
    });

    it("should check status without consuming a token", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 5,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupKey = "status-check";

      // Check initial status
      let status = await rateLimiter.check(groupKey);
      expect(status.currentCount).toBe(0);
      expect(status.limit).toBe(5);
      expect(status.isLimited).toBe(false);
      expect(status.remainingTokens).toBe(5);

      // Acquire some tokens
      await rateLimiter.acquire(groupKey);
      await rateLimiter.acquire(groupKey);

      // Check status again - should show 2 used
      status = await rateLimiter.check(groupKey);
      expect(status.currentCount).toBe(2);
      expect(status.isLimited).toBe(false);
      expect(status.remainingTokens).toBe(3);
    });

    it("should report rate limited status when limit exceeded", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 2,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupKey = "limited-check";

      // Exhaust the limit
      await rateLimiter.acquire(groupKey);
      await rateLimiter.acquire(groupKey);

      // Check status - should be limited
      const status = await rateLimiter.check(groupKey);
      expect(status.currentCount).toBe(2);
      expect(status.isLimited).toBe(true);
      expect(status.remainingTokens).toBe(0);
      expect(status.resetInMs).toBeGreaterThan(0);
    });

    it("should reset rate limit for a group", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 2,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupKey = "reset-test";

      // Exhaust the limit
      await rateLimiter.acquire(groupKey);
      await rateLimiter.acquire(groupKey);

      // Verify limited
      let status = await rateLimiter.check(groupKey);
      expect(status.isLimited).toBe(true);

      // Reset
      await rateLimiter.reset(groupKey);

      // Should have fresh tokens
      status = await rateLimiter.check(groupKey);
      expect(status.currentCount).toBe(0);
      expect(status.isLimited).toBe(false);
      expect(status.remainingTokens).toBe(2);
    });

    it("should handle multiple group keys independently", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 2,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupA = "group-a";
      const groupB = "group-b";

      // Exhaust group A's limit
      await rateLimiter.acquire(groupA);
      await rateLimiter.acquire(groupA);

      // Group A should be limited
      const statusA = await rateLimiter.check(groupA);
      expect(statusA.isLimited).toBe(true);

      // Group B should still have full capacity
      const statusB = await rateLimiter.check(groupB);
      expect(statusB.isLimited).toBe(false);
      expect(statusB.remainingTokens).toBe(2);

      // Can still acquire from group B
      const resultB = await rateLimiter.acquire(groupB);
      expect(resultB.acquired).toBe(true);
    });
  });

  describe("Concurrency", () => {
    it("should handle concurrent requests correctly", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 5,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupKey = "concurrent-test";

      // Fire 5 concurrent requests
      const results = await Promise.all([
        rateLimiter.acquire(groupKey),
        rateLimiter.acquire(groupKey),
        rateLimiter.acquire(groupKey),
        rateLimiter.acquire(groupKey),
        rateLimiter.acquire(groupKey),
      ]);

      expect(results.every((r) => r.acquired)).toBe(true);

      const status = await rateLimiter.check(groupKey);
      expect(status.currentCount).toBe(5);
      expect(status.isLimited).toBe(true);
    });

    it("should maintain atomicity under high concurrency", async () => {
      const rateLimiter = new RateLimiter({
        redis,
        limit: 5,
        windowMs: 60000,
        keyPrefix: "test",
      });

      const groupKey = "high-concurrency-test";

      // Fire 10 concurrent requests
      const promises = Array.from({ length: 10 }, () =>
        rateLimiter.acquire(groupKey),
      );
      const results = await Promise.all(promises);

      const successCount = results.filter((r) => r.acquired).length;
      const failureCount = results.filter((r) => !r.acquired).length;

      // 5 should succeed and 5 should
      expect(successCount).toEqual(5);
      expect(failureCount).toEqual(5);
    });
  });
});
