/* eslint-disable require-yield */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Job, Queue } from 'bullmq';
import {
  DistributedBatchProcessor,
  DistributedJobResult,
} from '../../src/processors/distributed-batch-processor';

// Mock dogstatsd
vi.mock('dd-trace', () => ({
  dogstatsd: {
    histogram: vi.fn(),
  },
}));

// Mock BullMQ Queue constructor for stop condition tests
const mockQueueInstance = {
  removeJobScheduler: vi.fn(),
  close: vi.fn(),
};
vi.mock('bullmq', async (importOriginal) => {
  const orig = await importOriginal<typeof import('bullmq')>();
  return {
    ...orig,
    Queue: vi.fn().mockImplementation(() => mockQueueInstance),
  };
});

interface JobConfig {
  cycleTime?: 'hour' | 'day' | 'week';
  slot?: number;
  processedCount?: number;
  queueName?: string;
}

type BatchJob = Job<JobConfig, DistributedJobResult>;

describe('DistributedBatchProcessor', () => {
  const mockDataCallback = vi.fn();
  const mockProcessCallback = vi.fn();
  const mockOnStart = vi.fn();
  const mockOnStartBatch = vi.fn();
  const mockOnComplete = vi.fn();
  const mockOnCompleteBatch = vi.fn();
  const mockOnError = vi.fn();
  const mockStopCondition = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockQueueInstance.removeJobScheduler.mockClear();
    mockQueueInstance.close.mockClear();
  });

  describe('build', () => {
    it('should create a processor function', () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      expect(typeof processor).toBe('function');
    });

    it('should throw error if repeat.every is not provided', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      const mockJob = {
        opts: {},
        data: {},
        updateData: vi.fn(),
      } as unknown as BatchJob;

      await expect(processor(mockJob)).rejects.toThrow(
        'DistributedBatchProcessor:build: Invalid job configuration, must provide `repeat.every` option',
      );
    });

    it('should calculate totalSlots correctly for daily cycle', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* mockGenerator() {
        yield { id: 1 };
        yield { id: 2 };
      }
      mockDataCallback.mockReturnValue(mockGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 300000 } }, // 5 minutes
        data: { cycleTime: 'day' },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      // 24 hours = 86400000ms, divided by 300000ms = 288 slots
      expect(mockDataCallback).toHaveBeenCalledWith(
        {
          currentSlot: 0,
          totalSlots: 288,
        },
        mockJob,
      );
      expect(mockProcessCallback).toHaveBeenCalledTimes(2);
      expect(mockProcessCallback).toHaveBeenNthCalledWith(
        1,
        { id: 1 },
        mockJob,
      );
      expect(mockProcessCallback).toHaveBeenNthCalledWith(
        2,
        { id: 2 },
        mockJob,
      );
      expect(result).toEqual({ processedCount: 2 }); // processedCount from job.data
      expect(mockJob.updateData).toHaveBeenCalledWith({
        cycleTime: 'day',
        slot: 1, // (0 + 1) % 288 = 1
        processedCount: 2,
      });
    });

    it('should calculate totalSlots correctly for hourly cycle', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* emptyGenerator() {
        return;
      }
      mockDataCallback.mockReturnValue(emptyGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 60000 } }, // 1 minute
        data: { cycleTime: 'hour' },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      // 1 hour = 3600000ms, divided by 60000ms = 60 slots
      expect(mockDataCallback).toHaveBeenCalledWith(
        {
          currentSlot: 0,
          totalSlots: 60,
        },
        mockJob,
      );
      expect(mockProcessCallback).not.toHaveBeenCalled();
      expect(result).toEqual({ processedCount: 0 });
      expect(mockJob.updateData).toHaveBeenCalledWith({
        cycleTime: 'hour',
        slot: 1, // (0 + 1) % 60 = 1
        processedCount: 0,
      });
    });

    it('should calculate totalSlots correctly for weekly cycle', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* emptyGenerator() {
        return;
      }
      mockDataCallback.mockReturnValue(emptyGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 3600000 } }, // 1 hour
        data: { cycleTime: 'week' },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      // 1 week = 604800000ms, divided by 3600000ms = 168 slots
      expect(mockDataCallback).toHaveBeenCalledWith(
        {
          currentSlot: 0,
          totalSlots: 168,
        },
        mockJob,
      );
      expect(mockProcessCallback).not.toHaveBeenCalled();
      expect(result).toEqual({ processedCount: 0 });
      expect(mockJob.updateData).toHaveBeenCalledWith({
        cycleTime: 'week',
        slot: 1, // (0 + 1) % 168 = 1
        processedCount: 0,
      });
    });

    it('should use existing slot from job data', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* emptyGenerator() {
        return;
      }
      mockDataCallback.mockReturnValue(emptyGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 300000 } },
        data: { slot: 42, cycleTime: 'day' },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      expect(mockDataCallback).toHaveBeenCalledWith(
        {
          currentSlot: 42,
          totalSlots: 288,
        },
        mockJob,
      );
      expect(result).toEqual({ processedCount: 0 });
      expect(mockJob.updateData).toHaveBeenCalledWith({
        cycleTime: 'day',
        slot: 43, // (42 + 1) % 288 = 43
        processedCount: 0,
      });
    });

    it('should cycle back to slot 0 when reaching totalSlots', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* emptyGenerator() {
        return;
      }
      mockDataCallback.mockReturnValue(emptyGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 3600000 } }, // 1 hour intervals
        data: { slot: 0, cycleTime: 'hour' }, // Current slot for 1-hour cycle (totalSlots = 1)
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      expect(mockDataCallback).toHaveBeenCalledWith(
        {
          currentSlot: 0,
          totalSlots: 1,
        },
        mockJob,
      );
      expect(result).toEqual({ processedCount: 0 });
      expect(mockJob.updateData).toHaveBeenCalledWith({
        cycleTime: 'hour',
        slot: 0, // (0 + 1) % 1 = 0
        processedCount: 0,
      });
    });

    it('should call dataCallback and processCallback with correct parameters', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* mockGenerator() {
        yield { id: 1 };
        yield { id: 2 };
      }
      mockDataCallback.mockReturnValue(mockGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 300000 } },
        data: { slot: 5, cycleTime: 'day' },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      expect(mockDataCallback).toHaveBeenCalledWith(
        {
          currentSlot: 5,
          totalSlots: 288,
        },
        mockJob,
      );
      expect(mockProcessCallback).toHaveBeenCalledTimes(2);
      expect(mockProcessCallback).toHaveBeenNthCalledWith(
        1,
        { id: 1 },
        mockJob,
      );
      expect(mockProcessCallback).toHaveBeenNthCalledWith(
        2,
        { id: 2 },
        mockJob,
      );
      expect(result).toEqual({ processedCount: 2 }); // processed 2 items from the generator
    });

    it('should preserve other job data properties when updating', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* emptyGenerator() {
        return;
      }
      mockDataCallback.mockReturnValue(emptyGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      const mockJob = {
        opts: { repeat: { every: 300000 } },
        data: {
          slot: 10,
          cycleTime: 'day',
          customProperty: 'value',
          anotherProperty: 42,
        },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result = await processor(mockJob);

      expect(result).toEqual({ processedCount: 0 });
      expect(mockJob.updateData).toHaveBeenCalledWith({
        slot: 11,
        cycleTime: 'day',
        customProperty: 'value',
        anotherProperty: 42,
        processedCount: 0,
      });
    });

    it('should track processedCount correctly with empty generators multiple runs', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* emptyGenerator() {
        return;
      }
      mockDataCallback.mockReturnValue(emptyGenerator());
      mockProcessCallback.mockResolvedValue(undefined);

      // First run with no existing processedCount
      const mockJob1 = {
        opts: { repeat: { every: 300000 } },
        data: { cycleTime: 'day' },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result1 = await processor(mockJob1);
      expect(result1).toEqual({ processedCount: 0 });
      expect(mockJob1.updateData).toHaveBeenCalledWith({
        cycleTime: 'day',
        slot: 1,
        processedCount: 0,
      });

      // Second run with existing processedCount
      const mockJob2 = {
        opts: { repeat: { every: 300000 } },
        data: { cycleTime: 'day', slot: 1 },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      const result2 = await processor(mockJob2);
      expect(result2).toEqual({ processedCount: 0 });
      expect(mockJob2.updateData).toHaveBeenCalledWith({
        cycleTime: 'day',
        slot: 2,
        processedCount: 0,
      });
    });

    it('should handle error and still update processedCount', async () => {
      const processor = DistributedBatchProcessor.build({
        dataCallback: mockDataCallback,
        processCallback: mockProcessCallback,
      });

      async function* mockGenerator() {
        yield { id: 1 };
      }
      mockDataCallback.mockReturnValue(mockGenerator());
      mockProcessCallback.mockRejectedValue(new Error('Processing failed'));

      const mockJob = {
        opts: { repeat: { every: 300000 } },
        data: { cycleTime: 'day', processedCount: 3 },
        updateData: vi.fn(),
      } as unknown as BatchJob;

      await expect(processor(mockJob)).rejects.toThrow('Processing failed');

      expect(mockJob.updateData).toHaveBeenCalledWith({
        cycleTime: 'day',
        processedCount: 4,
        slot: 1,
      });
    });

    describe('lifecycle hooks', () => {
      it('should call onStart hook only on first run', async () => {
        const processor = DistributedBatchProcessor.build({
          dataCallback: mockDataCallback,
          processCallback: mockProcessCallback,
          hooks: {
            onStart: mockOnStart,
            onStartBatch: mockOnStartBatch,
          },
        });

        async function* emptyGenerator() {
          return;
        }
        mockDataCallback.mockReturnValue(emptyGenerator());

        // First run - should call onStart
        const mockJob1 = {
          opts: { repeat: { every: 300000 } },
          data: { cycleTime: 'day' },
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await processor(mockJob1);
        expect(mockOnStart).toHaveBeenCalledWith(mockJob1);
        expect(mockOnStartBatch).toHaveBeenCalledWith(mockJob1);

        vi.clearAllMocks();

        // Second run with processedCount - should NOT call onStart
        const mockJob2 = {
          opts: { repeat: { every: 300000 } },
          data: { cycleTime: 'day', processedCount: 1 },
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await processor(mockJob2);
        expect(mockOnStart).not.toHaveBeenCalled();
        expect(mockOnStartBatch).toHaveBeenCalledWith(mockJob2);
      });

      it('should call onCompleteBatch hooks on success', async () => {
        const processor = DistributedBatchProcessor.build({
          dataCallback: mockDataCallback,
          processCallback: mockProcessCallback,
          hooks: {
            onCompleteBatch: mockOnCompleteBatch,
          },
        });

        async function* emptyGenerator() {
          return;
        }
        mockDataCallback.mockReturnValue(emptyGenerator());

        const mockJob = {
          opts: { repeat: { every: 300000 } },
          data: { cycleTime: 'day' },
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await processor(mockJob);
        expect(mockOnCompleteBatch).toHaveBeenCalledWith(mockJob);
      });

      it('should call onError hook on failure', async () => {
        const processor = DistributedBatchProcessor.build({
          dataCallback: mockDataCallback,
          processCallback: mockProcessCallback,
          hooks: {
            onError: mockOnError,
          },
        });

        async function* mockGenerator() {
          yield { id: 1 };
        }
        mockDataCallback.mockReturnValue(mockGenerator());
        mockProcessCallback.mockRejectedValue(new Error('Processing failed'));

        const mockJob = {
          opts: { repeat: { every: 300000 } },
          data: { cycleTime: 'day' },
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await expect(processor(mockJob)).rejects.toThrow('Processing failed');
        expect(mockOnError).toHaveBeenCalledWith(expect.any(Error), mockJob);
      });
    });

    describe('stop condition', () => {
      it('should call stop condition and remove job scheduler when condition is met', async () => {
        mockStopCondition.mockResolvedValue(true);

        const processor = DistributedBatchProcessor.build({
          dataCallback: mockDataCallback,
          processCallback: mockProcessCallback,
          stopCondition: mockStopCondition,
          hooks: {
            onComplete: mockOnComplete,
          },
        });

        async function* emptyGenerator() {
          return;
        }
        mockDataCallback.mockReturnValue(emptyGenerator());

        const mockJob = {
          opts: { repeat: { every: 300000 }, jobId: 'test-job-123' },
          data: { cycleTime: 'day', processedCount: 5 },
          queueName: 'test-queue',
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await processor(mockJob);

        expect(mockStopCondition).toHaveBeenCalledWith(
          {
            processedCount: 5,
            currentSlot: 0,
            totalSlots: 288,
          },
          mockJob,
        );
        expect(mockQueueInstance.removeJobScheduler).toHaveBeenCalledWith(
          'test-job-123',
        );
        expect(mockQueueInstance.close).toHaveBeenCalled();
        expect(mockOnComplete).toHaveBeenCalledWith(mockJob);
      });

      it('should not remove job scheduler when stop condition is false', async () => {
        mockStopCondition.mockResolvedValue(false);

        const processor = DistributedBatchProcessor.build({
          dataCallback: mockDataCallback,
          processCallback: mockProcessCallback,
          stopCondition: mockStopCondition,
        });

        async function* emptyGenerator() {
          return;
        }
        mockDataCallback.mockReturnValue(emptyGenerator());

        const mockJob = {
          opts: { repeat: { every: 300000 }, jobId: 'test-job-123' },
          data: { cycleTime: 'day' },
          queueName: 'test-queue',
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await processor(mockJob);

        expect(mockStopCondition).toHaveBeenCalled();
        expect(mockQueueInstance.removeJobScheduler).not.toHaveBeenCalled();
        expect(mockQueueInstance.close).not.toHaveBeenCalled();
      });

      it('should call onComplete hook when runOnce is true and all slots are processed', async () => {
        const processor = DistributedBatchProcessor.build({
          dataCallback: mockDataCallback,
          processCallback: mockProcessCallback,
          hooks: {
            onComplete: mockOnComplete,
          },
        });

        async function* emptyGenerator() {
          return;
        }
        mockDataCallback.mockReturnValue(emptyGenerator());

        const mockJob = {
          opts: {
            repeat: {
              every: 300000,
              limit: 288, // totalSlots for daily cycle with 5-minute intervals
            },
          },
          data: { cycleTime: 'day' },
          updateData: vi.fn(),
        } as unknown as BatchJob;

        await processor(mockJob);

        expect(mockOnComplete).toHaveBeenCalledWith(mockJob);
      });
    });
  });

  describe('schedule', () => {
    const mockUpsertJobScheduler = vi.fn();
    const mockScheduleQueue = {
      name: 'schedule-test-queue',
      upsertJobScheduler: mockUpsertJobScheduler,
    } as unknown as Queue;

    beforeEach(() => {
      mockUpsertJobScheduler.mockClear();
    });

    it('should call upsertJobScheduler with correct parameters for default options', async () => {
      const opts = {
        id: 'test-job-id',
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, opts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'test-job-id',
        {
          id: 'test-job-id',
          every: 300000,
          limit: undefined,
        },
        {
          data: {
            cycleTime: undefined,
            totalSlots: 288, // 24*60*60*1000 / 300000 = 288
          },
        },
      );
    });

    it('should use custom everyMs when provided', async () => {
      const opts = {
        id: 'test-job-id',
        everyMs: 10000,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, opts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'test-job-id',
        {
          id: 'test-job-id',
          everyMs: 10000,
          every: 10000,
          limit: undefined,
        },
        {
          data: {
            cycleTime: undefined,
            totalSlots: 8640, // 24*60*60*1000 / 10000 = 8640
          },
        },
      );
    });

    it('should use custom cycleTime when provided', async () => {
      const opts = {
        id: 'test-job-id',
        cycleTime: 'hour' as const,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, opts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'test-job-id',
        {
          id: 'test-job-id',
          cycleTime: 'hour',
          every: 300000,
          limit: undefined,
        },
        {
          data: {
            cycleTime: 'hour',
            totalSlots: 12, // 60*60*1000 / 300000 = 12
          },
        },
      );
    });

    it('should use both custom everyMs and cycleTime when provided', async () => {
      const opts = {
        id: 'test-job-id',
        everyMs: 15000,
        cycleTime: 'week' as const,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, opts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'test-job-id',
        {
          id: 'test-job-id',
          everyMs: 15000,
          cycleTime: 'week',
          every: 15000,
          limit: undefined,
        },
        {
          data: {
            cycleTime: 'week',
            totalSlots: 40320, // 7*24*60*60*1000 / 15000 = 40320
          },
        },
      );
    });

    it('should use runOnce when provided', async () => {
      const opts = {
        id: 'test-job-id',
        everyMs: 60000, // 1 minute
        cycleTime: 'hour' as const,
        runOnce: true,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, opts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'test-job-id',
        {
          id: 'test-job-id',
          everyMs: 60000,
          cycleTime: 'hour',
          runOnce: true,
          every: 60000,
          limit: 60, // totalSlots when runOnce is true
        },
        {
          data: {
            cycleTime: 'hour',
            totalSlots: 60,
          },
        },
      );
    });

    it('should not set limit when runOnce is not provided', async () => {
      const opts = {
        id: 'test-job-id',
        everyMs: 300000, // 5 minutes
        cycleTime: 'day' as const,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, opts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'test-job-id',
        {
          id: 'test-job-id',
          everyMs: 300000,
          cycleTime: 'day',
          every: 300000,
          limit: undefined,
        },
        {
          data: {
            cycleTime: 'day',
            totalSlots: 288,
          },
        },
      );
    });

    it('should set correct limit for different cycle times when runOnce is true', async () => {
      // Test daily cycle
      const dailyOpts = {
        id: 'daily-job',
        everyMs: 300000, // 5 minutes
        cycleTime: 'day' as const,
        runOnce: true,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, dailyOpts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'daily-job',
        {
          id: 'daily-job',
          everyMs: 300000,
          cycleTime: 'day',
          runOnce: true,
          every: 300000,
          limit: 288, // totalSlots when runOnce is true
        },
        {
          data: {
            cycleTime: 'day',
            totalSlots: 288,
          },
        },
      );

      mockUpsertJobScheduler.mockClear();

      // Test weekly cycle
      const weeklyOpts = {
        id: 'weekly-job',
        everyMs: 3600000, // 1 hour
        cycleTime: 'week' as const,
        runOnce: true,
      };

      await DistributedBatchProcessor.schedule(mockScheduleQueue, weeklyOpts);

      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'weekly-job',
        {
          id: 'weekly-job',
          everyMs: 3600000,
          cycleTime: 'week',
          runOnce: true,
          every: 3600000,
          limit: 168, // totalSlots when runOnce is true
        },
        {
          data: {
            cycleTime: 'week',
            totalSlots: 168,
          },
        },
      );
    });

    it('should return the result from queue.upsertJobScheduler', () => {
      const expectedResult = { success: true };
      mockUpsertJobScheduler.mockReturnValue(expectedResult);

      const opts = {
        id: 'test-job-id',
      };

      const result = DistributedBatchProcessor.schedule(
        mockScheduleQueue,
        opts,
      );

      expect(result).toBe(expectedResult);
    });
  });
});
