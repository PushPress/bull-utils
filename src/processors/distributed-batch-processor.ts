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
import { match } from 'ts-pattern';

type BaseScheduleOptions = Omit<RepeatOptions, 'key' | 'every' | 'repeat'>;

const CacheValue = z.object({
  slot: z.number(),
});

type CacheValue = z.infer<typeof CacheValue>;

/**
 * Options for creating a distributed batch job scheduler.
 * Extends the RepeatOptions from BullMQ with additional configuration
 * for distributed processing including cycle time and execution limits.
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
 * Context object containing slot information for distributed batch processing.
 *
 * This interface provides the necessary context for processors to understand
 * which slot they should process and how to partition their data across
 * the total number of available slots. It's used by data callbacks and
 * stop conditions to make decisions about data processing and job termination.
 */
export interface SlotContext {
  /** The current slot number (0-based) that this job run should process */
  currentSlot: number;
  /** Total number of slots in the cycle - use for partitioning data */
  totalSlots: number;
  /** processing count for the current slot */
  processedCount: number;
}

/**
 * Lifecycle hooks for distributed batch processor.
 */
interface LifecycleHooks<JobData, JobName extends string = string> {
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
    slotContext: SlotContext,
    job: Job<JobData, unknown, JobName>,
  ) => AsyncIterable<ProcessorData> | Promise<AsyncIterable<ProcessorData>>;
  /** Function that processes each individual data item. */
  processCallback: (
    data: ProcessorData,
    slotContext: SlotContext,
    job: Job<JobData, unknown, JobName>,
  ) => Promise<void>;
  /** Optional lifecycle hooks for monitoring processor state. */
  hooks?: LifecycleHooks<JobData, JobName>;
  /** Optional stop condition function that can terminate the job scheduler. */
  stopCondition?: (
    context: SlotContext,
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

export type QueueName = string;

interface DistributedBatchProcessorConstructorArgs extends ScheduleOptions {
  queue: Queue | QueueName;
  logger?: Logger;
}

/**
 * Creates BullMQ processors that distribute work across time slots to prevent overwhelming systems.
 *
 * Works by cycling through numbered slots (0 to totalSlots-1), where each job processes data
 * for its assigned slot, then updates to the next slot for subsequent runs. This allows
 * large datasets to be processed in smaller, distributed batches over time.
 *
 * The processor automatically manages slot progression and state persistence via Redis,
 * ensuring reliable distribution even across job restarts.
 *
 * Example: With totalSlots=4, jobs will process slots 0→1→2→3→0→1... distributing
 * the workload across 4 time windows.
 */
export class DistributedBatchProcessor {
  #scheduleOptions: ScheduleOptions;
  #logger?: Logger;
  #cacheKey: string;
  #queue: Queue;

  constructor({
    queue,
    logger,
    ...options
  }: DistributedBatchProcessorConstructorArgs) {
    this.#queue = match(queue)
      .when(
        (q) => q instanceof Queue,
        (q) => q,
      )
      .otherwise((name) => new Queue(name));

    this.#scheduleOptions = options;
    this.#logger = logger;
    this.#cacheKey = `${this.#scheduleOptions.id}:slot-cache`;
  }

  /**
   * Retrieves the current batch job state from Redis cache.
   *
   * @returns A Zod parse result containing the current slot and processed count
   */
  async getBatchJobState() {
    const redisClient = await this.#queue.client;

    const val = await redisClient.get(this.#cacheKey);
    return CacheValue.safeParse(val === null ? {} : JSON.parse(val));
  }

  /**
   * Updates the batch job state in Redis cache.
   *
   * @param state - The new state containing slot and processed count
   * @returns Promise that resolves when the state is saved
   */
  async setBatchJobState(state: CacheValue) {
    const redisClient = await this.#queue.client;
    return redisClient.set(this.#cacheKey, JSON.stringify(state));
  }

  /**
   * Returns a cache value if it exists otherwise sets it to the initial value
   */
  async #setupCacheState() {
    const state = await this.getBatchJobState();
    if (state.success) {
      return state.data;
    }
    this.#logger?.debug({ error: state.error.message }, 'Invalid');

    const initialState: CacheValue = {
      slot: 0,
    };

    await this.setBatchJobState(initialState);
    return initialState;
  }

  /**
   * A helper function to safely call a function and log any errors.
   * will not throw an error if the function fails.
   */
  async #callWithErrorMsg(
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
   * @returns A BullMQ processor function that can be used with queue.add() or worker processors
   *
   * @example
   * ```typescript
   * // Create a processor instance
   * const processor = new DistributedBatchProcessor({
   *   queue,
   *   id: 'user-notifications',
   *   cycleTime: 'day',
   * });
   *
   * // Build the processor function
   * const processorFn = await processor.build({
   *   dataCallback: (slotContext) => {
   *     return fetchDataForSlot(slotContext.currentSlot, slotContext.totalSlots);
   *   },
   *   processCallback: async (user, slotContext) => {
   *     await sendNotificationEmail(user.email);
   *   },
   * });
   *
   * // Use with BullMQ worker
   * worker.add('user-notifications', processorFn);
   * ```
   */
  async build<ProcessorData, JobData, JobName extends string>(
    config: ProcessorConfig<ProcessorData, JobData, JobName>,
  ): Promise<Processor<JobData, DistributedJobResult, JobName>> {
    const opts = this.#scheduleOptions;
    const totalSlots = calculateTotalSlots(opts);
    const queue = this.#queue;
    const everyMs = opts.everyMs ?? DEFAULT_CADENCE;
    const limit = opts.runOnce ? totalSlots : undefined;

    // TODO: this should not overwrite any existing job state
    // when it runs again. all job state needs to be stored in redis

    await queue.upsertJobScheduler(
      opts.id,
      {
        ...opts,
        every: everyMs,
        limit,
      },
      {
        data: {
          cycleTime: opts.cycleTime,
          totalSlots,
        },
      },
    );

    return async (job) => {
      // internal value to track processedCount per batch
      let processedCount = 0;
      const { slot } = await this.#setupCacheState();

      // cache the current slot in redis to preserve state between slots
      const setCache = (nextSlot: number) => {
        return this.setBatchJobState({
          slot: nextSlot,
        });
      };

      try {
        // Call onStartBatch hook
        await this.#callWithErrorMsg(
          () => config.hooks?.onStartBatch?.(job),
          'DistributedBatchProcessor onStartBatch hook failed',
        );

        const dataIterable = await config.dataCallback(
          {
            currentSlot: slot,
            totalSlots,
            processedCount,
          },
          job,
        );

        for await (const record of dataIterable) {
          processedCount++;
          await config.processCallback(
            record,
            {
              currentSlot: slot,
              processedCount,
              totalSlots,
            },
            job,
          );
        }

        await this.#callWithErrorMsg(
          () => config.hooks?.onCompleteBatch?.(job),
          'DistributedBatchProcessor onCompleteBatch hook failed',
        );
      } catch (err) {
        await this.#callWithErrorMsg(
          () => config.hooks?.onError?.(err, job),
          'DistributedBatchProcessor onError hook failed',
        );

        // Update cache with next slot after error
        await setCache((slot + 1) % totalSlots);
        throw err;
      }

      // Update cache with next slot after successful processing
      await setCache((slot + 1) % totalSlots);

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
          this.#callWithErrorMsg(
            () => config.hooks?.onComplete?.(job),
            'DistributedBatchProcessor onComplete hook failed on stop condition',
          );
          return { processedCount };
        }
      }

      // call at the end of run once
      if (job.opts.repeat?.limit === totalSlots) {
        this.#callWithErrorMsg(
          () => config.hooks?.onComplete?.(job),
          'DistributedBatchProcessor onComplete hook failed on the last slot',
        );
      }

      return { processedCount };
    };
  }
}
