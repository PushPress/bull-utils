/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
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
      options: {
        id: 'test-distributed-batch',
        cycleTime: 'day',
      },
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

    // Track which items were processed in each slot
    const processedItems: { slot: number; items: any[] }[] = [];

    // Create the processor
    const fn = await processor.build({
      dataCallback: ({ currentSlot, totalSlots }) => {
        // Return items that belong to this slot (using modulo for distribution)
        const slotItems = testData.filter(
          (_, index) => index % totalSlots === currentSlot,
        );
        return {
          async *[Symbol.asyncIterator]() {
            for (const item of slotItems) {
              yield item;
            }
          },
        };
      },
      processCallback: async (item, job) => {
        // Track which slot processed this item
        // Note: The slot is managed internally by the processor and available via job.data.slot
        const currentSlot = job.data.slot || 0;
        const existing = processedItems.find((p) => p.slot === currentSlot);
        if (existing) {
          existing.items.push(item);
        } else {
          processedItems.push({ slot: currentSlot, items: [item] });
        }
      },
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
  });
});
