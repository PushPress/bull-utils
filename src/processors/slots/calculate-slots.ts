import { match } from "ts-pattern";

export type CycleTime = "hour" | "day" | "week";

export const DEFAULT_CYCLE_TIME = "day";
export const DEFAULT_CADENCE = 1000 * 60 * 5; // 5 minutes

// Calculate how many job executions fit in the cycle time
// This ensures every record is processed at least once per cycle
export function calculateTotalSlots({
  cycleTime = DEFAULT_CYCLE_TIME,
  everyMs = DEFAULT_CADENCE,
}: {
  cycleTime?: CycleTime;
  everyMs?: number;
}) {
  const cycleTimeHours = match(cycleTime)
    .with("hour", () => 1)
    .with("day", () => 24)
    .with("week", () => 24 * 7)
    .exhaustive();

  // Calculate how many job executions fit in the cycle time
  // This ensures every record is processed at least once per cycle
  return Math.ceil((cycleTimeHours * 60 * 60 * 1000) / everyMs);
}
