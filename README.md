# Bull Utils

A collection of utilities for [BullMQ](https://github.com/OptimalBits/bullmq) that provides powerful distributed batch processing and rate limiting capabilities.

## Overview

Bull Utils provides:

- **DistributedBatchProcessor**: Process large datasets in smaller, distributed batches over time, preventing system overload by cycling through numbered slots.
- **RateLimiter**: A Redis-backed distributed rate limiter using a fixed window algorithm, with support for grouped rate limiting by tenant, API key, or any custom key.

## Key Features

- **Distributed Processing**: Automatically distributes work across time slots to prevent system overload
- **State Persistence**: Uses Redis to maintain slot state across job restarts
- **Flexible Scheduling**: Supports hourly, daily, and weekly cycle times
- **Lifecycle Hooks**: Built-in hooks for monitoring and error handling
- **TypeScript Support**: Full TypeScript definitions and type safety
- **Stop Conditions**: Custom logic to terminate job schedulers when needed
- **Grouped Rate Limiting**: Redis-backed rate limiting with support for independent rate limits per group key (e.g., per tenant)
- **BullMQ Integration**: Rate-limited processor wrapper that automatically delays jobs when rate limited

## Installation

```bash
pnpm install bull-utils
```

## Usage

### Basic Example

```typescript
import { DistributedBatchProcessor } from 'bull-utils';
import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';

// Create Redis connection
const redis = new Redis();

// Create a queue
const queue = new Queue('user-notifications', { connection: redis });

// Create the processor
const processor = new DistributedBatchProcessor({
  queue,
  id: 'user-notifications',
  cycleTime: 'day', // Process all users once per day
  everyMs: 5 * 60 * 1000, // Run every 5 minutes
});

// Build the processor function
const processorFn = await processor.build({
  dataCallback: async (slotContext, job) => {
    // Fetch users for this specific slot
    // Use modulo to distribute users across slots
    const users = await fetchUsersForSlot(
      slotContext.currentSlot,
      slotContext.totalSlots,
    );
    return users;
  },
  processCallback: async (user, slotContext, job) => {
    // Process each user
    await sendNotificationEmail(user.email);
    console.log(`Processed user ${user.id} in slot ${slotContext.currentSlot}`);
  },
  hooks: {
    onStartBatch: async (job) => {
      console.log('Starting batch processing...');
    },
    onCompleteBatch: async (job) => {
      console.log('Batch completed successfully');
    },
    onError: async (err, job) => {
      console.error('Batch processing failed:', err);
    },
  },
});

// Use with BullMQ worker
const worker = new Worker('user-notifications', processorFn, {
  connection: redis,
});
```

### Advanced Example with Stop Condition

```typescript
const processor = new DistributedBatchProcessor({
  queue,
  id: 'data-migration',
  cycleTime: 'week',
  runOnce: true, // Run through all slots once and stop
});

const processorFn = await processor.build({
  dataCallback: async (slotContext) => {
    return fetchDataForSlot(slotContext.currentSlot, slotContext.totalSlots);
  },
  processCallback: async (record, slotContext) => {
    await processRecord(record);
  },
  stopCondition: (slotContext, job) => {
    // Stop if we've processed enough records
    return slotContext.processedCount >= 1000;
  },
  hooks: {
    onComplete: async (job) => {
      console.log('Migration completed!');
    },
  },
});
```

### Data Distribution Strategy

The processor uses a simple but effective distribution strategy:

```typescript
// In your dataCallback, partition data by slot
dataCallback: async (slotContext) => {
  const allRecords = await fetchAllRecords();

  // Distribute records across slots using modulo
  const recordsForThisSlot = allRecords.filter(
    (record, index) =>
      index % slotContext.totalSlots === slotContext.currentSlot,
  );

  return recordsForThisSlot;
};
```

## How It Works

1. **Initialization**: The processor calculates total slots based on cycle time and execution frequency
2. **Slot Management**: Each job run processes data for the current slot, then advances to the next slot
3. **State Persistence**: Current slot is stored in Redis and survives application restarts
4. **Data Distribution**: Your `dataCallback` uses the slot information to partition data
5. **Cycling**: After processing all slots, the cycle starts over from slot 0

## Example Scenarios

### User Notification System

- **Goal**: Send notifications to all users once per day
- **Setup**: 288 slots (5-minute intervals over 24 hours)
- **Result**: Each user gets processed once per day, spread across time

### Data Migration

- **Goal**: Migrate large dataset without overwhelming the system
- **Setup**: 1008 slots (5-minute intervals over a week)
- **Result**: Data is migrated gradually over a week

### Report Generation

- **Goal**: Generate reports for all customers weekly
- **Setup**: 2016 slots (5-minute intervals over a week)
- **Result**: Each customer gets a report once per week

## Rate Limiting

Bull Utils includes a distributed rate limiter that uses Redis for shared state, allowing you to enforce rate limits across multiple processes and servers.

### RateLimiter

The `RateLimiter` class implements a fixed window rate limiting algorithm. It uses Lua scripts for atomic operations, ensuring no race conditions when multiple processes attempt to acquire tokens simultaneously.

```typescript
import { RateLimiter } from 'bull-utils';
import Redis from 'ioredis';

const redis = new Redis();

const rateLimiter = new RateLimiter({
  redis,
  limit: 100, // 100 requests
  windowMs: 60000, // per minute
  keyPrefix: 'myapp',
});

// Acquire a token (non-blocking)
const result = await rateLimiter.acquire('tenant-123');
if (result.acquired) {
  console.log(`Token acquired! ${result.remainingTokens} tokens remaining`);
  // Proceed with rate-limited operation
} else {
  console.log(`Rate limited. Retry in ${result.ttlSeconds} seconds`);
  // Handle rate limit - move to delayed queue or reject
}

// Check status without consuming a token
const status = await rateLimiter.check('tenant-123');
if (status.isLimited) {
  console.log(`Rate limited, resets in ${status.resetInMs}ms`);
} else {
  console.log(`${status.remainingTokens} tokens available`);
}

// Reset a specific group's rate limit
await rateLimiter.reset('tenant-123');
```

### Rate-Limited BullMQ Processor

The `createRateLimitedProcessor` function wraps a BullMQ processor with rate limiting. When the rate limit is exceeded, the job is automatically moved to a delayed state and will be retried when the rate limit window resets.

```typescript
import { RateLimiter, createRateLimitedProcessor } from 'bull-utils';
import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';

const redis = new Redis();

// Create the rate limiter
const rateLimiter = new RateLimiter({
  redis,
  limit: 10, // 10 API calls
  windowMs: 1000, // per second
});

// Create a rate-limited processor
const rateLimitedProcessor = createRateLimitedProcessor(
  {
    rateLimiter,
    // Extract the group key from the job - rate limits are applied per group
    groupKeyFn: (job) => job.data.tenantId ?? 'default',
  },
  // Your actual processing logic
  async (job) => {
    await callExternalAPI(job.data);
    return { success: true };
  },
);

// Use with a BullMQ worker
const worker = new Worker('api-calls', rateLimitedProcessor, {
  connection: redis,
});
```

### Rate Limiting Features

- **Fixed Window Algorithm**: Counts requests within discrete time windows
- **Grouped Rate Limits**: Each group key (e.g., tenant ID) has independent rate limits
- **Atomic Operations**: Uses Lua scripts to prevent race conditions under high concurrency
- **BullMQ Integration**: Automatically delays and retries jobs when rate limited
- **Status Checking**: Check rate limit status without consuming tokens
- **Manual Reset**: Reset rate limits for specific groups when needed
