/**
 * Creates a PostgreSQL SQL expression string that assigns a slot number based on a hashed column value.
 *
 * This is useful for partitioning data across multiple slots in a distributed batch processor.
 * It hashes the first 8 characters of the column value and maps it to a slot number in the range [0, totalSlots - 1].
 *
 * @param columnId - The name of the column to hash (typically a UUID or string ID column)
 * @param totalSlots - The total number of slots to distribute across
 * @returns A SQL string expression that evaluates to a slot number (0 to totalSlots - 1)
 *
 * @example
 * ```typescript
 * // Use in a raw SQL query or with your ORM:
 * const slotContext = { currentSlot: 2, totalSlots: 288 };
 *
 * const sql = `SELECT * FROM users WHERE ${psqlSlot('id', slotContext.totalSlots)} = ${slotContext.currentSlot}`;
 *
 * // With Kysely:
 * const users = await db
 *   .selectFrom('users')
 *   .selectAll()
 *   .where(sql`${sql.raw(psqlSlot('id', slotContext.totalSlots))}`, '=', slotContext.currentSlot)
 *   .execute();
 * ```
 */
export function psqlSlot(columnId: string, totalSlots: number): string {
  // Normalize to [0, totalSlots - 1]. Postgres `%` keeps the sign of the left operand,
  // so signed 32-bit hashes can produce negative buckets without this adjustment.
  return `(
    (
      (('x' || substr(${columnId}::text, 1, 8))::bit(32)::int % ${totalSlots})
      + ${totalSlots}
    ) % ${totalSlots}
  )`;
}
