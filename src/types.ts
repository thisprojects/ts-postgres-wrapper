/**
 * Type definitions for postgres-query-builder-ts
 */

/**
 * JSON operators supported by PostgreSQL
 */
export type JSONOperator =
  | "->"   // Get JSON object field as JSON
  | "->>"  // Get JSON object field as text
  | "#>"   // Get JSON object at specified path as JSON
  | "#>>"  // Get JSON object at specified path as text
  | "@>"   // Contains JSON
  | "<@"   // Is contained by JSON
  | "?"    // Does key/element exist
  | "?|"   // Do any of these array strings exist as top-level keys
  | "?&"   // Do all of these array strings exist as top-level keys
  | "@?"   // JSONPath match
  | "@@";  // JSONPath predicate match

/**
 * JSON path expression (e.g. '{a,b,c}' or '$.a.b.c')
 */
export type JSONPath = string[] | string;

/**
 * JSON value types that can be used in queries
 */
export type JSONValue = Record<string, any> | any[] | string | number | boolean | null;

/**
 * Type-safe JSON path builder for nested object access
 * Extracts the type at a given path in a JSON object
 */
export type JSONPathType<T, Path extends readonly (string | number)[]> =
  Path extends readonly [infer First, ...infer Rest] ?
    First extends keyof T ?
      Rest extends readonly (string | number)[] ?
        JSONPathType<T[First], Rest> :
        T[First] :
      any :
    T;

/**
 * Type-safe JSON field accessor
 */
export type JSONField<T> = T extends Record<string, any> ? keyof T : never;

/**
 * Column names as strings
 */
export type ColumnNames<T> = keyof T & string;

/**
 * Column alias configuration (strongly typed)
 */
export type ColumnAlias<T, K extends keyof T = keyof T> = {
  column: K;
  as: string;
};

/**
 * Expression alias for aggregate functions, calculations, etc.
 * These expressions return 'any' type since we can't infer their result type
 */
export type ExpressionAlias = {
  column: string;
  as: string;
  __isExpression?: true; // Marker to identify safe expressions from expr() helper
};

/**
 * Column specification that allows both simple column names and complex expressions
 */
export type ColumnSpec<T> = keyof T | string | ColumnAlias<T, keyof T> | ExpressionAlias;

/**
 * Helper type to distinguish between typed column aliases and expression aliases
 */
export type IsTypedAlias<T, A> = A extends { column: keyof T } ? true : false;

/**
 * Extract the result type from a mix of column names and aliases
 * Improved to better handle string expressions while maintaining type safety
 */
export type ResultColumns<T, S extends ColumnSpec<T>[]> = {
  [K in S[number] as
    K extends { as: infer A extends string } ? A :
    K extends keyof T ? K :
    K extends string ? K :
    never
  ]:
    K extends { column: infer C } ?
      C extends keyof T ? T[C] :
      C extends string ? any :
      never :
    K extends keyof T ? T[K] :
    K extends string ? any :
    never
};

/**
 * Pick specific columns from a table
 */
export type SelectColumns<T, K extends keyof T & string> = Pick<T, K>;

/**
 * Join condition configuration
 */
export interface JoinCondition {
  leftColumn: string;
  rightColumn: string;
}

/**
 * Join configuration
 */
export interface JoinConfig {
  type: "INNER" | "LEFT" | "RIGHT" | "FULL";
  table: string;
  conditions: JoinCondition[];
  alias?: string;
}

/**
 * Security options for query validation
 */
export interface SecurityOptions {
  /**
   * Maximum number of WHERE conditions allowed in a single query
   * Helps prevent query complexity attacks
   */
  maxWhereConditions?: number;

  /**
   * Maximum number of JOIN clauses allowed in a single query
   */
  maxJoins?: number;

  /**
   * Maximum batch operation size
   */
  maxBatchSize?: number;

  /**
   * Maximum SQL query length in characters
   */
  maxQueryLength?: number;

  /**
   * Allow SQL comments in queries
   */
  allowComments?: boolean;

  /**
   * Enable rate limiting for batch operations
   */
  rateLimitBatch?: boolean;

  /**
   * Max batch operations per second
   */
  batchRateLimit?: number;
}

/**
 * TypedPg configuration options
 */
export interface TypedPgOptions {
  /**
   * Optional custom logger for query logging
   */
  logger?: QueryLogger;

  /**
   * Log level for query logging
   */
  logLevel?: LogLevel;

  /**
   * Security validation options
   */
  security?: SecurityOptions;

  /**
   * Query timeout in milliseconds
   */
  timeout?: number;

  /**
   * Number of retry attempts for transient errors
   */
  retryAttempts?: number;

  /**
   * Delay between retries in milliseconds
   */
  retryDelay?: number;

  /**
   * Custom error handler
   */
  onError?: (error: Error, context: ErrorContext) => void;
}

/**
 * Query log entry structure
 */
export interface QueryLogEntry {
  query: string;
  params?: any[];
  duration?: number;
  error?: Error;
  timestamp: Date;
}

/**
 * Log levels for query logging
 */
export type LogLevel = "debug" | "info" | "warn" | "error";

/**
 * Query logger interface
 */
export interface QueryLogger {
  log(level: LogLevel, entry: QueryLogEntry): void;
  debug?(entry: QueryLogEntry): void;
  info?(entry: QueryLogEntry): void;
  warn?(entry: QueryLogEntry): void;
  error?(entry: QueryLogEntry): void;
}

/**
 * Error context for database errors
 */
export interface ErrorContext {
  query: string;
  params: any[];
  attempt?: number;
  operation?: string;
  table?: string;
  detail?: string;
}

/**
 * Helper functions for creating typed column specifications
 */

/**
 * Create a typed column alias
 * Usage: select(col("firstName", "name"), col("lastName", "surname"))
 */
export function col<T, K extends keyof T>(column: K, as: string): ColumnAlias<T, K> {
  return { column, as };
}

/**
 * Create an expression alias for aggregate functions or calculations
 * Usage: select(expr("COUNT(*)", "total"), expr("AVG(age)", "averageAge"))
 */
export function expr(expression: string, as: string): ExpressionAlias {
  return { column: expression, as, __isExpression: true };
}

/**
 * PostgreSQL transaction isolation levels
 * https://www.postgresql.org/docs/current/transaction-iso.html
 */
export type IsolationLevel =
  | 'READ UNCOMMITTED'
  | 'READ COMMITTED'
  | 'REPEATABLE READ'
  | 'SERIALIZABLE';

/**
 * Options for transaction execution
 */
export interface TransactionOptions {
  /**
   * Transaction isolation level
   * @default 'READ COMMITTED'
   */
  isolationLevel?: IsolationLevel;

  /**
   * Whether the transaction should be read-only
   * @default false
   */
  readOnly?: boolean;

  /**
   * Whether the transaction should be deferrable (only meaningful for SERIALIZABLE and READ ONLY)
   * @default false
   */
  deferrable?: boolean;
}
