/* eslint-disable @typescript-eslint/no-explicit-any */
import * as z from 'zod/mini';
import { Job, Queue } from 'bullmq';
import type { RepeatOptions, Processor } from 'bullmq';
import {
  calculateTotalSlots,
  DEFAULT_CADENCE,
  type CycleTime,
} from './slots/calculate-slots';
import type { Logger } from '../logger';

type BaseScheduleOptions = Omit<RepeatOptions, 'key' | 'every' | 'repeat'>;

const CacheValue = z.object({
  slot: z.number(),
  processedCount: z.number(),
});

type CacheValue = z.infer<typeof CacheValue>;

/**
 * Options for creating a distributed batch job scheduler.
 * extends the RepeatOptions from bullmq
 * advanced configuration includes a custom start date and a limited
 */
export interface ScheduleOptions extends BaseScheduleOptions {
  /** unique id for the batch job, if running multiple jobs on the same queue you must specify unique
   * ids for each batch processor
   */
  id: string;

  /**
   * How often the complete dataset should be processed. This is not a guarantee that it will be processed
   * but a reasonable upper bound for how long it will take to cycle back to the same item in the processCallback
   * defaults to daily
   */
  cycleTime: CycleTime;

  /** How often the job should run, defaults to every 300000 milliseconds (5 minutes) */
  everyMs?: number;

  /**
   * If true, the job will run through one complete cycle of all slots and then stop.
   * If false or undefined, the job will run indefinitely, continuously cycling through slots.
   * @default false (run indefinitely)
   */
  runOnce?: boolean;
}

/**
 * Configuration for distributed batch job data.
 */
export interface JobConfig extends Pick<ScheduleOptions, 'cycleTime'> {
  /** The current slot number being processed. Automatically managed by the processor. */
  slot?: number;
  /** the number of items processed in this job run. Automatically managed by the processor. */
  processedCount?: number;
  /** The queue name for this job, used for stop condition processing. */
  queueName?: string;
}

/**
 * Lifecycle hooks for distributed batch processor.
 */
interface LifecycleHooks<JobData, JobName extends string = string> {
  /** Called once when the processor starts its very first run (processedCount = 0) */
  onStart?: (job: Job<JobData, unknown, JobName>) => Promise<void> | void;
  /** Called at the beginning of each batch/slot processing cycle */
  onStartBatch?: (job: Job<JobData, unknown, JobName>) => Promise<void> | void;

  /** Called when the entire job scheduler completes (via stop condition or runOnce limit) */
  onComplete?: (job: Job<JobData, unknown, JobName>) => Promise<void> | void;
  /** Called after successfully processing each batch/slot */
  onCompleteBatch?: (
    job: Job<JobData, unknown, JobName>,
  ) => Promise<void> | void;

  /** Called when an error occurs during batch processing */
  onError?: (
    err: unknown,
    job: Job<JobData, unknown, JobName>,
  ) => Promise<void> | void;
}

/**
 * Configuration options for creating a distributed batch processor.
 */
export interface ProcessorConfig<
  ProcessorData,
  JobData,
  JobName extends string,
> {
  /**
   * Function that fetches data for processing based on the current slot.
   * Use currentSlot and totalSlots to partition your data (e.g., with MOD operations)
   * so each job run processes a different subset of the total dataset.
   */
  dataCallback: (
    params: {
      /** The current slot number (0-based) that this job run should process */
      currentSlot: number;
      /** Total number of slots in the cycle - use for partitioning data */
      totalSlots: number;
    },
    job: Job<JobData, unknown, JobName>,
  ) => AsyncIterable<ProcessorData> | Promise<AsyncIterable<ProcessorData>>;
  /** Function that processes each individual data item. */
  processCallback: (
    data: ProcessorData,
    job: Job<JobData, unknown, JobName>,
  ) => Promise<void>;
  /** Optional lifecycle hooks for monitoring processor state. */
  hooks?: LifecycleHooks<JobData, JobName>;
  /** Optional stop condition function that can terminate the job scheduler. */
  stopCondition?: (
    context: {
      processedCount: number;
      currentSlot: number;
      totalSlots: number;
    },
    job: Job<JobData, unknown, JobName>,
  ) => boolean | Promise<boolean>;
}

/**
 * Result returned by a distributed batch processor job.
 */
export interface DistributedJobResult {
  /** The number of items successfully processed in this job run. */
  processedCount: number;
}

interface DistributedBatchProcessorConstructorArgs {
  queue: Queue;
  options: ScheduleOptions;
  logger?: Logger;
}

/**
 * Creates BullMQ processors that distribute work across time slots to prevent overwhelming systems.
 *
 * Works by cycling through numbered slots (0 to totalSlots-1), where each job processes data
 * for its assigned slot, then updates to the next slot for subsequent runs. This allows
 * large datasets to be processed in smaller, distributed batches over time.
 *
 * Example: With totalSlots=4, jobs will process slots 1→2→3→0→1... distributing
 * the workload across 4 time windows.
 */
export class DistributedBatchProcessor {
  #queue: Queue;
  #scheduleOptions: ScheduleOptions;
  #logger?: Logger;
  #cacheKey: string;

  constructor({
    queue,
    options,
    logger,
  }: DistributedBatchProcessorConstructorArgs) {
    this.#queue = queue;
    this.#scheduleOptions = options;
    this.#logger = logger;
    this.#cacheKey = `${this.#scheduleOptions.id}:slot-cache`;
  }

  async getBatchJobState() {
    const redisClient = await this.#queue.client;

    const val = await redisClient.get(this.#cacheKey);
    return CacheValue.safeParse(val === null ? {} : JSON.parse(val));
  }

  async setBatchJobState(state: CacheValue) {
    const redisClient = await this.#queue.client;
    return redisClient.set(this.#cacheKey, JSON.stringify(state));
  }

  /**
   * A helper function to safely call a function and log any errors.
   * will not throw an error if the function fails.
   */
  async #calllWithErrorMsg(
    fn: (...args: any[]) => Promise<any> | any,
    errorMessage: string,
  ) {
    try {
      return await fn();
    } catch (err) {
      this.#logger?.error({ err }, errorMessage);
    }
  }

  /**
   * Creates a BullMQ processor that distributes work across time slots.
   *
   * @param config - Configuration object containing callbacks and options
   * @returns A BullMQ processor function that can be used with fastify.createWorker()
   *
   * @example
   * ```typescript
   * // Example using paginate helper to return an async iterable
   * const processor = DistributedBatchProcessor.build({
   *   dataCallback: ({ currentSlot, totalSlots }) => {
   *     return paginate(
   *       async ({ limit, offset }) => {
   *         const data = await db
   *           .selectFrom("users")
   *           .select(["id", "email", "name"])
   *           .where("status", "=", "active")
   *           .where(sql`MOD(CRC32(id), ${totalSlots})`, "=", currentSlot)
   *           .orderBy("id")
   *           .limit(limit)
   *           .offset(offset ?? 0)
   *           .execute();
   *
   *         return {
   *           items: data,
   *           pageInfo: {
   *             hasNextPage: data.length === limit,
   *           },
   *         };
   *       },
   *       {
   *         strategy: "offset",
   *         limit: 100,
   *         errorPolicy: { type: "throw" },
   *       }
   *     );
   *   },
   *   processCallback: async (user) => {
   *     await sendNotificationEmail(user.email);
   *   },
   *   cycleTime: "day"
   * });
   * ```
   */
  async build<ProcessorData, JobData extends JobConfig, JobName extends string>(
    config: ProcessorConfig<ProcessorData, JobData, JobName>,
  ): Promise<Processor<JobData, DistributedJobResult, JobName>> {
    let slot = 0;
    let processedCount = 0;
    const opts = this.#scheduleOptions;
    const queue = this.#queue;
    const totalSlots = calculateTotalSlots(opts);

    await this.setBatchJobState({ slot: 0, processedCount: 0 });

    await queue.upsertJobScheduler(
      opts.id,
      {
        ...opts,
        every: opts.everyMs ?? DEFAULT_CADENCE,
        limit: opts.runOnce ? totalSlots : undefined,
      },
      {
        data: {
          cycleTime: opts.cycleTime,
          totalSlots,
        },
      },
    );

    return async (job) => {
      const everyMs = job.opts.repeat?.every;
      if (!everyMs) {
        throw new Error(
          'DistributedBatchProcessor:build: Invalid job configuration, must provide `repeat.every` option',
        );
      }

      const result = await this.getBatchJobState();

      // if we can't get the cache value, set to default values so we get values on the next turn
      if (result.success) {
        slot = result.data.slot;
        processedCount = result.data.processedCount;
      } else {
        slot = 0;
        processedCount = 0;
      }

      const totalSlots = calculateTotalSlots({
        cycleTime: job.data.cycleTime,
        everyMs,
      });

      // Call onStart hook only on the first run
      if (!processedCount) {
        await this.#calllWithErrorMsg(
          () => config.hooks?.onStart?.(job),
          'DistributedBatchProcessor onStart hook failed',
        );
      }

      // cache the current slot in redis to preserve state between slots
      const setCache = (nextSlot: number, currentProcessedCount: number) => {
        return this.setBatchJobState({
          processedCount: currentProcessedCount,
          slot: nextSlot,
        });
      };

      try {
        // Call onStartBatch hook
        await this.#calllWithErrorMsg(
          () => config.hooks?.onStartBatch?.(job),
          'DistributedBatchProcessor onStartBatch hook failed',
        );

        const dataIterable = await config.dataCallback(
          {
            currentSlot: slot,
            totalSlots,
          },
          job,
        );

        for await (const record of dataIterable) {
          processedCount++;
          await config.processCallback(record, job);
        }

        await this.#calllWithErrorMsg(
          () => config.hooks?.onCompleteBatch?.(job),
          'DistributedBatchProcessor onCompleteBatch hook failed',
        );
      } catch (err) {
        await this.#calllWithErrorMsg(
          () => config.hooks?.onError?.(err, job),
          'DistributedBatchProcessor onError hook failed',
        );

        // Update cache with next slot after error
        await setCache((slot + 1) % totalSlots, processedCount);
        throw err;
      }

      // Update cache with next slot after successful processing
      await setCache((slot + 1) % totalSlots, processedCount);

      // Some processors may define an arbitrary stop condition
      if (config.stopCondition) {
        const shouldStop = await config.stopCondition(
          {
            processedCount,
            currentSlot: slot,
            totalSlots,
          },
          job,
        );

        if (shouldStop) {
          await this.#queue.removeJobScheduler(job.opts.jobId!);
          this.#calllWithErrorMsg(
            () => config.hooks?.onComplete?.(job),
            'DistributedBatchProcessor onComplete hook failed on stop condition',
          );
          return { processedCount };
        }
      }

      // call at the end of run once
      if (job.opts.repeat?.limit === totalSlots) {
        this.#calllWithErrorMsg(
          () => config.hooks?.onComplete?.(job),
          'DistributedBatchProcessor onComplete hook failed on the last slot',
        );
      }

      return { processedCount };
    };
  }
}
