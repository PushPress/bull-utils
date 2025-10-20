import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Job, Queue } from 'bullmq';
import { DistributedBatchProcessor } from '../../src/processors/distributed-batch-processor.js';
import Redis from 'ioredis';

describe('DistributedBatchProcessor Integration Tests', () => {
  let redis: Redis;
  let queue: Queue;
  let processor: DistributedBatchProcessor;

  beforeEach(async () => {
    // Create a new Redis connection for each test
    redis = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379'),
      db: 15, // Use a test database
      maxRetriesPerRequest: null,
    });

    // Clear the test database
    await redis.flushdb();

    // Create a test queue
    queue = new Queue('test-distributed-batch', {
      connection: redis,
    });

    processor = new DistributedBatchProcessor({
      queue,
      id: 'test-distributed-batch',
      cycleTime: 'day',
    });
  });

  afterEach(async () => {
    await queue?.close();
    await redis?.quit();
  });

  it('should update job data to the next slot after processing the first slot', async () => {
    // Test data - simple array of items to process
    const testData = [
      { id: 1, name: 'Item 1' },
      { id: 2, name: 'Item 2' },
      { id: 3, name: 'Item 3' },
      { id: 4, name: 'Item 4' },
      { id: 5, name: 'Item 5' },
    ];

    // Create mock functions to track calls
    const mockProcessCallback = vi.fn();

    // Create the processor
    const fn = await processor.build({
      dataCallback: (slotContext) => {
        // Return items that belong to this slot (using modulo for distribution)
        const slotItems = testData.filter(
          (_, index) =>
            index % slotContext.totalSlots === slotContext.currentSlot,
        );
        return {
          async *[Symbol.asyncIterator]() {
            for (const item of slotItems) {
              yield item;
            }
          },
        };
      },
      processCallback: mockProcessCallback,
    });

    // run the job 1x
    await fn(
      new Job(
        queue,
        'test-job-2',
        { cycleTime: 'day' },
        { repeat: { every: 1000 } },
      ),
    );

    // Verify the job data was updated
    let jobState = await processor.getBatchJobState();
    if (!jobState.success) {
      throw new Error('Job 1 state must be set');
    }
    expect(jobState.data.slot).toEqual(1);
    expect(jobState.data.processedCount).toEqual(1);

    // Verify slotContext was passed correctly to processCallback
    expect(mockProcessCallback).toHaveBeenCalledTimes(1);
    expect(mockProcessCallback).toHaveBeenCalledWith(
      expect.objectContaining({ id: 1, name: 'Item 1' }),
      {
        currentSlot: 0,
        totalSlots: 288, // 24 hours * 60 minutes * 60 seconds * 1000ms / (5 minutes * 60 seconds * 1000ms)
        processedCount: 1,
      },
      expect.any(Object), // Job object
    );

    await fn(
      new Job(
        queue,
        'test-job-2',
        { cycleTime: 'day' },
        { repeat: { every: 1000 } },
      ),
    );

    // Verify the job data was updated again
    jobState = await processor.getBatchJobState();
    if (!jobState.success) {
      throw new Error('Job 2 state must be set');
    }
    expect(jobState.data.slot).toEqual(2);
    expect(jobState.data.processedCount).toEqual(2);

    // Verify slotContext was passed correctly to processCallback for second run
    expect(mockProcessCallback).toHaveBeenCalledTimes(2);
    expect(mockProcessCallback).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ id: 2, name: 'Item 2' }),
      {
        currentSlot: 1,
        totalSlots: 288, // 24 hours * 60 minutes * 60 seconds * 1000ms / (5 minutes * 60 seconds * 1000ms)
        processedCount: 2,
      },
      expect.any(Object), // Job object
    );
  });

  it('should support stop condition with SlotContext', async () => {
    // Test data - simple array of items to process
    const testData = [
      { id: 1, name: 'Item 1' },
      { id: 2, name: 'Item 2' },
      { id: 3, name: 'Item 3' },
      { id: 4, name: 'Item 4' },
      { id: 5, name: 'Item 5' },
    ];

    // Create mock functions to track calls
    const mockProcessCallback = vi.fn();
    const mockStopCondition = vi.fn().mockImplementation((slotContext) => {
      // Stop after processing 2 items
      return slotContext.processedCount >= 2;
    });

    // Create the processor
    const fn = await processor.build({
      dataCallback: (slotContext) => {
        // Return items that belong to this slot (using modulo for distribution)
        const slotItems = testData.filter(
          (_, index) =>
            index % slotContext.totalSlots === slotContext.currentSlot,
        );
        return {
          async *[Symbol.asyncIterator]() {
            for (const item of slotItems) {
              yield item;
            }
          },
        };
      },
      processCallback: mockProcessCallback,
      stopCondition: mockStopCondition,
    });

    // run the job
    await fn(
      new Job(
        queue,
        'test-job-stop',
        { cycleTime: 'day', totalSlots: 2 },
        { repeat: { every: 1000 } },
      ),
    );

    // Verify stop condition was called
    expect(mockStopCondition).toHaveBeenCalled();
    expect(mockProcessCallback).toHaveBeenCalled();

    // Verify slotContext was passed correctly to processCallback
    const processCallbackCalls = mockProcessCallback.mock.calls;
    expect(processCallbackCalls.length).toBeGreaterThan(0);

    processCallbackCalls.forEach((call, index) => {
      const [item, slotContext, job] = call;
      expect(slotContext).toEqual({
        currentSlot: 0, // First slot
        totalSlots: 288, // 24 hours * 60 minutes * 60 seconds * 1000ms / (5 minutes * 60 seconds * 1000ms)
        processedCount: index + 1,
      });
      expect(item).toEqual(expect.objectContaining({ id: index + 1 }));
      expect(job).toBeInstanceOf(Job);
    });
  });
});
