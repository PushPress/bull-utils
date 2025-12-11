import { DelayedError, type Job, type Processor } from "bullmq";
import type { RateLimiterConfig } from "./rate-limiter";
import { RateLimiter } from "./rate-limiter";
import { dogstatsd } from "dd-trace";

/**
 * Configuration for creating a rate-limited processor wrapper.
 *
 * @typeParam JobData - The type of data stored in the job
 * @typeParam ProcessorData - The type of data passed to the processor callback
 * @typeParam JobName - The job name type (extends string)
 */
export interface RateLimitedProcessorConfig<
  JobData,
  JobName extends string = string,
> extends RateLimiterConfig {
  /**
   * Function to extract the group key from job and processor data.
   * The group key determines which rate limit bucket is used.
   *
   * @param data - The data item being processed
   * @param job - The BullMQ job instance
   * @returns The group key string for rate limiting
   *
   * @example
   * ```typescript
   * // Rate limit by tenant ID from job data
   * groupKeyFn: ( job) => job.data.tenantId
   * ```
   */
  groupKeyFn: (job: Job<JobData, unknown, JobName>) => string;
}

/**
 * A rate-limited processor callback function type.
 * This wraps your actual processing logic with rate limiting.
 */
export type RateLimitedCallback<JobData, JobName extends string> = (
  job: Job<JobData, unknown, JobName>,
  token?: string,
) => Promise<void>;

/**
 * Creates a rate-limited wrapper around a processor callback.
 *
 * This function wraps your processing callback with rate limiting,
 * ensuring that the rate limit is acquired before each item is processed.
 * If the rate limit is exceeded, the job is moved to the delayed queue
 * and returns to the caller. The processor will retry the job when the
 * rate limit window resets.
 *
 * This is designed to work with the `DistributedBatchProcessor` processCallback,
 * but can be used with any async processing function that follows the same signature.
 *
 * @typeParam JobData - The type of data stored in the job (e.g., ScheduleJobData)
 * @typeParam ProcessorData - The type of individual items being processed
 * @typeParam SlotContext - The context type passed to the processor (e.g., SlotContext)
 * @typeParam JobName - The job name type (extends string)
 *
 * @param config - Configuration including rate limiter and group key function
 * @param callback - The actual processing callback to wrap
 * @returns A wrapped callback that enforces rate limiting
 *
 * @example
 * ```typescript
 * const rateLimiter = new RateLimiter({
 *   redis,
 *   limit: 100,
 *   windowMs: 60000,
 * });
 *
 * // Create a rate-limited processor callback
 * const rateLimitedProcess = createRateLimitedProcessor(
 *   {
 *     rateLimiter,
 *     groupKeyFn: (job) => job.data.tenantId ?? 'default',
 *   },
 *   async (job) => {
 *     await sendNotificationEmail(user.email);
 *   },
 * );
 *
 * new Worker("my-queue", processorFn);
 *
 * ```
 */
export function createRateLimitedProcessor<
  JobData,
  Result,
  JobName extends string = string,
>(
  config: RateLimitedProcessorConfig<JobData, JobName>,
  callback: Processor<JobData, Result, JobName>,
): Processor<JobData, Result, JobName> {
  const { groupKeyFn, ..._config } = config;
  const rateLimiter = new RateLimiter(_config);

  return async (job, token) => {
    // acquire a lock by group key
    const result = await rateLimiter.acquire(groupKeyFn(job));

    //docs.bullmq.io/patterns/process-step-jobs#delaying
    if (!result.acquired) {
      const ttlSeconds = result.ttlSeconds;
      const waitMs =
        ttlSeconds > 0
          ? ttlSeconds * 1000
          : rateLimiter.getTimeUntilNextWindow();
      await job.moveToDelayed(Date.now() + waitMs, token);

      // increment dogstatsd metric
      dogstatsd.increment("bullmq.worker.rate-limited", 1, {
        jobName: job.name,
        queuName: job.queueName,
        delay: waitMs,
      });

      throw new DelayedError();
    }

    // Process the item
    return callback(job, token);
  };
}
