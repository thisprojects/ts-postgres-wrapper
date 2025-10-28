/**
 * Error handling for database operations
 */

import { ErrorContext } from './types';

/**
 * Custom error class for database errors
 */
export class DatabaseError extends Error {
  constructor(
    message: string,
    public readonly code?: string,
    public readonly context?: ErrorContext,
    public readonly originalError?: Error
  ) {
    super(message);
    this.name = "DatabaseError";
    Object.setPrototypeOf(this, DatabaseError.prototype);
  }
}

/**
 * Check if an error is a transient error that can be retried
 */
export function isTransientError(error: any): boolean {
  if (!error) return false;

  // PostgreSQL error codes for transient errors
  const transientCodes = new Set([
    '40001', // serialization_failure
    '40P01', // deadlock_detected
    '53000', // insufficient_resources
    '53100', // disk_full
    '53200', // out_of_memory
    '53300', // too_many_connections
    '57P03', // cannot_connect_now
    '58000', // system_error
    '58030', // io_error
    'ECONNRESET', // Connection reset
    'ETIMEDOUT', // Connection timeout
    'ENOTFOUND', // DNS lookup failed
    'ECONNREFUSED', // Connection refused
  ]);

  return transientCodes.has(error.code);
}
