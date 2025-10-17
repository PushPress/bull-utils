import { Job, Queue } from 'bullmq';
import type { RepeatOptions, Processor } from 'bullmq';
import { match } from 'ts-pattern';
import { dogstatsd } from 'dd-trace';

type BaseScheduleOptions = Omit<RepeatOptions, 'key' | 'every' | 'repeat'>;

type CycleTime = 'hour' | 'day' | 'week';

const DEFAULT_CYCLE_TIME = 'day';

const DEFAULT_CADENCE = 1000 * 60 * 5; // 5 minutes

/**
 * Options for creating a distributed batch job scheduler.
 * extends the RepeatOptions from bullmq
 * advanced configuration includes a custom start date and a limited
 */
export interface ScheduleOptions extends BaseScheduleOptions {
  /** unique id for the batch job, if running multiple jobs on the same queue you must specify unique
   * ids for each job
   */
  id: string;

  /** How often the job should run, defaults to every 300000 milliseconds (5 minutes) */
  everyMs?: number;

  /**
   * How often the complete dataset should be processed. This is not a guarantee that it will be processed
   * but a reasonable upper bound for how long it will take to cycle back to the same item in the processCallback
   * defaults to daily
   */
  cycleTime?: CycleTime;

  /**
   * If true, the job will run through one complete cycle of all slots and then stop.
   * If false or undefined, the job will run indefinitely, continuously cycling through slots.
   * @default false (run indefinitely)
   */
  runOnce?: boolean;

  /** The queue name for this job, used for stop condition processing. */
  queueName?: string;
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
interface LifecycleHooks<JobData, JobName extends string> {
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
  ) => AsyncIterable<ProcessorData>;
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
  static build<
    ProcessorData,
    JobData extends JobConfig,
    JobName extends string,
  >(
    config: ProcessorConfig<ProcessorData, JobData, JobName>,
  ): Processor<JobData, DistributedJobResult, JobName> {
    let processedCount = 0;
    return async (job) => {
      const startTime = Date.now();

      const everyMs = job.opts.repeat?.every;
      if (!everyMs) {
        throw new Error(
          'DistributedBatchProcessor:build: Invalid job configuration, must provide `repeat.every` option',
        );
      }

      processedCount = job.data.processedCount ?? processedCount;

      const totalSlots = calculateTotalSlots({
        cycleTime: job.data.cycleTime,
        everyMs,
      });

      // Start from slot 0, cycle through all slots over time
      const slot = job.data.slot ?? 0;

      let processingTimeMs = 0;

      // Call onStart hook only on the first run
      if (!processedCount) {
        try {
          await config.hooks?.onStart?.(job);
        } catch (err) {
          console.error('DistributedBatchProcessor onStart hook failed:', err);
        }
      }

      let processingError: Error | null = null;

      try {
        // Call onStartBatch hook
        try {
          await config.hooks?.onStartBatch?.(job);
        } catch (err) {
          console.error(
            'DistributedBatchProcessor onStartBatch hook failed:',
            err,
          );
        }

        for await (const record of config.dataCallback(
          {
            currentSlot: slot,
            totalSlots,
          },
          job,
        )) {
          processedCount++;
          await config.processCallback(record, job);
        }

        // Track processing time on success
        processingTimeMs = Date.now() - startTime;

        // Call onComplete hook - don't let hook errors fail the job
        try {
          await config.hooks?.onCompleteBatch?.(job);
        } catch (err) {
          console.error(
            'DistributedBatchProcessor onCompleteBatch hook failed:',
            err,
          );
        }
      } catch (err) {
        processingError = err as Error;
        processingTimeMs = Date.now() - startTime;

        // ensure to bump slot even in case of an error
        await job.updateData({
          ...job.data,
          processedCount,
          slot: (slot + 1) % totalSlots,
        });

        await config.hooks?.onError?.(err, job);

        throw processingError;
      }

      // ensure to bump slot even in case of an error
      await job.updateData({
        ...job.data,
        processedCount,
        slot: (slot + 1) % totalSlots,
      });

      // Check stop condition
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
          // Create a new queue instance to remove the job scheduler
          const queue = new Queue(job.queueName);
          await queue.removeJobScheduler(job.opts.jobId!);
          await queue.close();

          // Call onComplete hook - don't let hook errors fail anything
          try {
            await config.hooks?.onComplete?.(job);
          } catch (err) {
            console.error(
              'DistributedBatchProcessor onComplete hook failed:',
              err,
            );
          }
        }
      }

      // call at the end of run once
      if (job.opts.repeat?.limit === totalSlots) {
        // Call onComplete hook - don't let hook errors fail anything
        try {
          await config.hooks?.onComplete?.(job);
        } catch (err) {
          console.error(
            'DistributedBatchProcessor onComplete hook failed:',
            err,
          );
        }
      }

      // Track processing time with histogram metric
      dogstatsd.histogram(
        'distributed_batch_processor.processing_time',
        processingTimeMs,
        {
          job_name: job.name || 'unknown',
          slot: slot.toString(),
          total_slots: totalSlots.toString(),
          processed_count: processedCount.toString(),
        },
      );

      return { processedCount };
    };
  }

  /**
   * initializes a job scheduler for the distributed batch processer
   */
  static schedule(queue: Queue, opts: ScheduleOptions) {
    const totalSlots = calculateTotalSlots(opts);
    return queue.upsertJobScheduler(
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
  }
}

// Calculate how many job executions fit in the cycle time
// This ensures every record is processed at least once per cycle
function calculateTotalSlots({
  cycleTime = DEFAULT_CYCLE_TIME,
  everyMs = DEFAULT_CADENCE,
}: {
  cycleTime?: CycleTime;
  everyMs?: number;
}) {
  const cycleTimeHours = match(cycleTime)
    .with('hour', () => 1)
    .with('day', () => 24)
    .with('week', () => 24 * 7)
    .exhaustive();

  // Calculate how many job executions fit in the cycle time
  // This ensures every record is processed at least once per cycle
  return Math.ceil((cycleTimeHours * 60 * 60 * 1000) / everyMs);
}
