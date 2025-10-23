# Bull Utils

A collection of utilities for [BullMQ](https://github.com/OptimalBits/bullmq) that provides powerful distributed batch processing capabilities.

## Overview

Bull Utils provides a `DistributedBatchProcessor` that allows you to process large datasets in smaller, distributed batches over time. This prevents overwhelming your systems by cycling through numbered slots, where each job processes a different subset of your data.

## Key Features

- **Distributed Processing**: Automatically distributes work across time slots to prevent system overload
- **State Persistence**: Uses Redis to maintain slot state across job restarts
- **Flexible Scheduling**: Supports hourly, daily, and weekly cycle times
- **Lifecycle Hooks**: Built-in hooks for monitoring and error handling
- **TypeScript Support**: Full TypeScript definitions and type safety
- **Stop Conditions**: Custom logic to terminate job schedulers when needed

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

