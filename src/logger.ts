/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Generic logger interface that satisfies Pino logger requirements
 * Provides error logging method for hook failures
 */

export interface Logger {
  /**
   * Log error level messages
   * @param obj - Object to log (optional)
   * @param msg - Message to log
   * @param ...args - Additional arguments
   */
  error(obj?: object, msg?: string, ...args: any[]): void;
  error(msg: string, ...args: any[]): void;

  /**
   * Log info level messages
   * @param obj - Object to log (optional)
   * @param msg - Message to log
   * @param ...args - Additional arguments
   */
  info(obj?: object, msg?: string, ...args: any[]): void;
  info(msg: string, ...args: any[]): void;

  /**
   * Log debug level messages
   * @param obj - Object to log (optional)
   * @param msg - Message to log
   * @param ...args - Additional arguments
   */
  debug(obj?: object, msg?: string, ...args: any[]): void;
  debug(msg: string, ...args: any[]): void;
}
