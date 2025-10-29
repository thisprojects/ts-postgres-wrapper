import { Pool } from "pg";
import type { QueryLogger, TypedPgOptions, ErrorContext, TransactionOptions } from "./types";
import { sanitizeSqlIdentifier, validateQueryComplexity } from "./utils";
import { DatabaseError, isTransientError } from "./errors";
import { TypedQuery } from "./TypedQuery";

/**
 * Main wrapper for type-safe queries
 */
export class TypedPg<Schema extends Record<string, any> = Record<string, any>> {
  private pool: Pool;
  private schema: Schema;
  private logger?: QueryLogger;
  private options: TypedPgOptions;
  private batchOperationTimestamps: number[] = [];

  constructor(
    pool: Pool,
    schema?: Schema,
    optionsOrLogger?: QueryLogger | TypedPgOptions
  ) {
    this.pool = pool;
    this.schema = schema || ({} as Schema);

    // Handle backward compatibility: support both logger and options
    if (optionsOrLogger && "log" in optionsOrLogger) {
      // Old API: logger passed directly
      this.logger = optionsOrLogger;
      this.options = {};
    } else {
      // New API: options object
      this.options = optionsOrLogger || {};
      this.logger = this.options.logger;
    }

    // Set default options
    this.options = {
      timeout: 30000, // 30 seconds default
      retryAttempts: 0, // No retries by default
      retryDelay: 1000, // 1 second between retries
      ...this.options,
    };

    // Set up pool error handler (if pool supports event emitters)
    if (this.pool.on) {
      this.pool.on("error", (err) => {
        const context: ErrorContext = {
          query: "pool error",
          params: [],
          operation: "connection",
        };

        if (this.logger) {
          this.logger.log("error", {
            query: "Pool error",
            params: [],
            duration: 0,
            timestamp: new Date(),
            error: err,
          });
        }

        if (this.options.onError) {
          this.options.onError(err, context);
        }
      });
    }
  }

  /**
   * Set or update the query logger
   */
  setLogger(logger: QueryLogger | undefined): void {
    this.logger = logger;
  }

  /**
   * Get the current logger
   */
  getLogger(): QueryLogger | undefined {
    return this.logger;
  }

  /**
   * Set options
   */
  setOptions(options: Partial<TypedPgOptions>): void {
    this.options = { ...this.options, ...options };
    if (options.logger !== undefined) {
      this.logger = options.logger;
    }
  }

  /**
   * Get current options
   */
  getOptions(): TypedPgOptions {
    return { ...this.options };
  }

  /**
   * Sleep for a given duration
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, ms);
      // Prevent the timer from keeping the process alive
      if (timer.unref) {
        timer.unref();
      }
    });
  }

  /**
   * Rate limit batch operations to prevent overwhelming the database
   */
  private async rateLimitBatchOperation(): Promise<void> {
    if (!this.options.security?.rateLimitBatch) {
      return;
    }

    const maxOpsPerSecond = this.options.security.batchRateLimit || 10;
    const now = Date.now();
    const oneSecondAgo = now - 1000;

    // Remove timestamps older than 1 second
    this.batchOperationTimestamps = this.batchOperationTimestamps.filter(
      (ts) => ts > oneSecondAgo
    );

    // If we're at the limit, wait until the oldest timestamp expires
    if (this.batchOperationTimestamps.length >= maxOpsPerSecond) {
      const oldestTimestamp = this.batchOperationTimestamps[0];
      const waitTime = 1000 - (now - oldestTimestamp);
      if (waitTime > 0) {
        await this.sleep(waitTime);
      }
      // Clean up again after waiting
      this.batchOperationTimestamps = this.batchOperationTimestamps.filter(
        (ts) => ts > Date.now() - 1000
      );
    }

    // Record this operation
    this.batchOperationTimestamps.push(Date.now());
  }

  /**
   * Execute a query with logging, timeout, and retry logic
   */
  private async executeWithLogging<T = any>(
    query: string,
    params: any[],
    operation: string = "query"
  ): Promise<{ rows: T[] }> {
    // Validate query complexity if security options are configured
    if (this.options.security) {
      validateQueryComplexity(query, this.options.security);
    }

    const maxAttempts = (this.options.retryAttempts || 0) + 1;
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const startTime = Date.now();
      const timestamp = new Date();
      const context: ErrorContext = { query, params, attempt, operation };

      try {
        // Apply query timeout if configured
        let result: any;
        if (this.options.timeout && this.options.timeout > 0) {
          result = await Promise.race([
            this.pool.query(query, params),
            new Promise((_, reject) => {
              const timer = setTimeout(
                () =>
                  reject(
                    new DatabaseError(
                      "Query timeout exceeded",
                      "TIMEOUT",
                      context
                    )
                  ),
                this.options.timeout
              );
              // Prevent the timer from keeping the process alive
              if (timer.unref) {
                timer.unref();
              }
            }),
          ]);
        } else {
          result = await this.pool.query(query, params);
        }

        const duration = Date.now() - startTime;

        if (this.logger) {
          this.logger.log("debug", {
            query,
            params,
            duration,
            timestamp,
          });
        }

        return result as { rows: T[] };
      } catch (error) {
        const duration = Date.now() - startTime;
        lastError = error as Error;

        if (this.logger) {
          this.logger.log("error", {
            query,
            params,
            duration,
            timestamp,
            error: lastError,
          });
        }

        // Check if we should retry
        const shouldRetry = attempt < maxAttempts && isTransientError(error);

        if (shouldRetry) {
          if (this.logger) {
            this.logger.log("warn", {
              query: `Retrying query (attempt ${attempt}/${maxAttempts})`,
              params: [],
              duration: 0,
              timestamp: new Date(),
            });
          }

          // Wait before retrying
          await this.sleep(this.options.retryDelay || 1000);
          continue;
        }

        // Call custom error handler
        if (this.options.onError) {
          this.options.onError(lastError, context);
        }

        // Wrap error in DatabaseError if not already
        if (!(error instanceof DatabaseError)) {
          throw new DatabaseError(
            lastError.message,
            (error as any).code,
            context,
            lastError
          );
        }

        throw error;
      }
    }

    // This should never be reached, but TypeScript doesn't know that
    throw (
      lastError ||
      new DatabaseError("Query failed", undefined, { query, params, operation })
    );
  }

  /**
   * Start a type-safe query on a table
   */
  table<T extends keyof Schema & string>(
    tableName: T,
    alias?: string
  ): TypedQuery<T, Schema[T], Schema> {
    return new TypedQuery<T, Schema[T], Schema>(
      this.pool,
      tableName,
      this.schema,
      alias,
      this.logger,
      this.executeWithLogging.bind(this)
    );
  }

  /**
   * Insert data into a table
   */
  async insert<T extends keyof Schema & string>(
    tableName: T,
    data: Partial<Schema[T]> | Partial<Schema[T]>[]
  ): Promise<Schema[T][]> {
    const records = Array.isArray(data) ? data : [data];

    if (records.length === 0) return [];

    const columns = Object.keys(records[0]);

    // Sanitize all column names to prevent SQL injection
    const sanitizedColumns = columns.map((col) => sanitizeSqlIdentifier(col));

    const values = records
      .map((record, recordIndex) => {
        return columns
          .map(
            (_, colIndex) => `$${recordIndex * columns.length + colIndex + 1}`
          )
          .join(", ");
      })
      .map((row) => `(${row})`)
      .join(", ");

    const params = records.flatMap((record) =>
      columns.map((col) => record[col])
    );

    const query = `INSERT INTO ${sanitizeSqlIdentifier(
      String(tableName)
    )} (${sanitizedColumns.join(", ")}) VALUES ${values} RETURNING *`;

    const result = await this.executeWithLogging<Schema[T]>(query, params);
    return result.rows;
  }

  /**
   * Update data in a table
   */
  async update<T extends keyof Schema & string>(
    tableName: T,
    data: Partial<Schema[T]>,
    where: Partial<Schema[T]>
  ): Promise<Schema[T][]> {
    const setColumns = Object.keys(data);
    const whereColumns = Object.keys(where);

    // Validation: Prevent empty WHERE clauses to avoid updating all rows
    if (whereColumns.length === 0) {
      throw new Error(
        `Update operation requires at least one WHERE condition. ` +
          `To update all rows intentionally, use raw SQL: ` +
          `db.raw("UPDATE ${String(tableName)} SET ... WHERE true")`
      );
    }

    // Validation: Ensure WHERE values are not null/undefined
    const invalidWhereConditions = whereColumns.filter(
      (col) => where[col] === null || where[col] === undefined
    );
    if (invalidWhereConditions.length > 0) {
      throw new Error(
        `WHERE conditions cannot have null or undefined values. ` +
          `Invalid columns: ${invalidWhereConditions.join(", ")}`
      );
    }

    // Validation: Ensure we have data to set
    if (setColumns.length === 0) {
      throw new Error(
        "Update operation requires at least one column to update"
      );
    }

    // Sanitize all column names to prevent SQL injection
    const setClause = setColumns
      .map((col, index) => `${sanitizeSqlIdentifier(col)} = $${index + 1}`)
      .join(", ");
    const whereClause = whereColumns
      .map(
        (col, index) =>
          `${sanitizeSqlIdentifier(col)} = $${setColumns.length + index + 1}`
      )
      .join(" AND ");

    const params = [
      ...setColumns.map((col) => data[col]),
      ...whereColumns.map((col) => where[col]),
    ];

    const query = `UPDATE ${sanitizeSqlIdentifier(
      String(tableName)
    )} SET ${setClause} WHERE ${whereClause} RETURNING *`;

    const result = await this.executeWithLogging<Schema[T]>(query, params);
    return result.rows;
  }

  /**
   * Delete data from a table
   */
  async delete<T extends keyof Schema & string>(
    tableName: T,
    where: Partial<Schema[T]>
  ): Promise<Schema[T][]> {
    const whereColumns = Object.keys(where);

    // Validation: Prevent empty WHERE clauses to avoid deleting all rows
    if (whereColumns.length === 0) {
      throw new Error(
        `Delete operation requires at least one WHERE condition. ` +
          `To delete all rows intentionally, use raw SQL: ` +
          `db.raw("DELETE FROM ${String(tableName)} WHERE true")`
      );
    }

    // Validation: Ensure WHERE values are not null/undefined
    const invalidWhereConditions = whereColumns.filter(
      (col) => where[col] === null || where[col] === undefined
    );
    if (invalidWhereConditions.length > 0) {
      throw new Error(
        `WHERE conditions cannot have null or undefined values. ` +
          `Invalid columns: ${invalidWhereConditions.join(", ")}`
      );
    }

    // Sanitize all column names to prevent SQL injection
    const whereClause = whereColumns
      .map((col, index) => `${sanitizeSqlIdentifier(col)} = $${index + 1}`)
      .join(" AND ");
    const params = whereColumns.map((col) => where[col]);

    const query = `DELETE FROM ${sanitizeSqlIdentifier(
      String(tableName)
    )} WHERE ${whereClause} RETURNING *`;

    const result = await this.executeWithLogging<Schema[T]>(query, params);
    return result.rows;
  }

  /**
   * Insert or update data based on conflict columns
   */
  async upsert<T extends keyof Schema & string>(
    tableName: T,
    data: Partial<Schema[T]> | Partial<Schema[T]>[],
    conflictColumns: (keyof Schema[T] & string)[],
    updateColumns?: (keyof Schema[T] & string)[]
  ): Promise<Schema[T][]> {
    const records = Array.isArray(data) ? data : [data];
    if (records.length === 0) return [];

    // Validate conflict columns exist
    if (conflictColumns.length === 0) {
      throw new Error("At least one conflict column must be specified");
    }

    const columns = Object.keys(records[0]);
    const columnsToUpdate =
      updateColumns ||
      columns.filter((col) => !conflictColumns.includes(col as any));

    // Validate we have columns to update
    if (columnsToUpdate.length === 0) {
      throw new Error("No columns to update in UPSERT operation");
    }

    // Sanitize all identifiers to prevent SQL injection
    const sanitizedColumns = columns.map((col) => sanitizeSqlIdentifier(col));
    const sanitizedConflictColumns = conflictColumns.map((col) =>
      sanitizeSqlIdentifier(String(col))
    );

    // Build VALUES expression
    const values = records
      .map((record, recordIndex) => {
        return columns
          .map(
            (_, colIndex) => `$${recordIndex * columns.length + colIndex + 1}`
          )
          .join(", ");
      })
      .map((row) => `(${row})`)
      .join(", ");

    // Build UPDATE SET expression with sanitized identifiers
    const setClause = columnsToUpdate
      .map((col) => {
        const sanitizedCol = sanitizeSqlIdentifier(String(col));
        return `${sanitizedCol} = EXCLUDED.${sanitizedCol}`;
      })
      .join(", ");

    // Build query
    const query = `
      INSERT INTO ${sanitizeSqlIdentifier(
        String(tableName)
      )} (${sanitizedColumns.join(", ")})
      VALUES ${values}
      ON CONFLICT (${sanitizedConflictColumns.join(", ")})
      DO UPDATE SET ${setClause}
      RETURNING *
    `;

    const params = records.flatMap((record) =>
      columns.map((col) => record[col])
    );

    const result = await this.executeWithLogging<Schema[T]>(query, params);
    return result.rows;
  }

  /**
   * Raw query with manual type annotation
   */
  async raw<T extends Record<string, any> = Record<string, any>>(
    query: string,
    params?: any[]
  ): Promise<T[]> {
    const result = await this.executeWithLogging<T>(query, params || []);
    return result.rows;
  }

  /**
   * Begin a transaction
   */
  async transaction<T>(
    callback: (client: TypedPg<Schema>) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    const client = await this.pool.connect();

    try {
      // Build BEGIN statement with transaction options
      let beginStatement = "BEGIN";

      if (options) {
        const parts: string[] = [];

        // Add isolation level
        if (options.isolationLevel) {
          parts.push(`ISOLATION LEVEL ${options.isolationLevel}`);
        }

        // Add read-only mode
        if (options.readOnly) {
          parts.push("READ ONLY");
        }

        // Add deferrable mode (only valid with SERIALIZABLE and READ ONLY)
        if (options.deferrable) {
          parts.push("DEFERRABLE");
        }

        if (parts.length > 0) {
          beginStatement += " " + parts.join(" ");
        }
      }

      await client.query(beginStatement);
      const txPg = new TypedPg<Schema>(client as any, this.schema, this.options);
      const result = await callback(txPg);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Get the underlying pool
   */
  getPool(): Pool {
    return this.pool;
  }

  /**
   * Close the connection pool
   */
  async close(): Promise<void> {
    await this.pool.end();
  }

  /**
   * Execute multiple operations in a single batch
   */
  async batch<T extends keyof Schema & string>(operations: {
    insert?: Array<{
      table: T;
      data: Partial<Schema[T]> | Partial<Schema[T]>[];
    }>;
    update?: Array<{
      table: T;
      data: Partial<Schema[T]>;
      where: Partial<Schema[T]>;
    }>;
    delete?: Array<{
      table: T;
      where: Partial<Schema[T]>;
    }>;
  }): Promise<{
    inserted: Record<T, Schema[T][]>;
    updated: Record<T, Schema[T][]>;
    deleted: Record<T, Schema[T][]>;
  }> {
    return this.transaction(async (tx) => {
      const result = {
        inserted: {} as Record<T, Schema[T][]>,
        updated: {} as Record<T, Schema[T][]>,
        deleted: {} as Record<T, Schema[T][]>,
      };

      // Process inserts
      if (operations.insert) {
        for (const op of operations.insert) {
          const rows = await tx.insert(op.table, op.data);
          if (!result.inserted[op.table]) {
            result.inserted[op.table] = [];
          }
          result.inserted[op.table].push(...rows);
        }
      }

      // Process updates
      if (operations.update) {
        for (const op of operations.update) {
          const rows = await tx.update(op.table, op.data, op.where);
          if (!result.updated[op.table]) {
            result.updated[op.table] = [];
          }
          result.updated[op.table].push(...rows);
        }
      }

      // Process deletes
      if (operations.delete) {
        for (const op of operations.delete) {
          const rows = await tx.delete(op.table, op.where);
          if (!result.deleted[op.table]) {
            result.deleted[op.table] = [];
          }
          result.deleted[op.table].push(...rows);
        }
      }

      return result;
    });
  }

  /**
   * Insert multiple rows in chunks to avoid parameter limits
   */
  async batchInsert<T extends keyof Schema & string>(
    table: T,
    data: Partial<Schema[T]>[],
    chunkSize: number = 1000
  ): Promise<Schema[T][]> {
    if (data.length === 0) return [];

    // Validate batch size if security options are configured
    if (
      this.options.security?.maxBatchSize &&
      data.length > this.options.security.maxBatchSize
    ) {
      throw new DatabaseError(
        `Batch insert size ${data.length} exceeds maximum allowed ${this.options.security.maxBatchSize}`,
        "BATCH_TOO_LARGE",
        { query: `INSERT INTO ${table}`, params: [], operation: "batchInsert" }
      );
    }

    // If data fits in a single chunk, insert all at once
    if (data.length <= chunkSize) {
      await this.rateLimitBatchOperation();
      return this.insert(table, data);
    }

    // Split data into chunks and insert each chunk
    const results: Schema[T][] = [];
    for (let i = 0; i < data.length; i += chunkSize) {
      await this.rateLimitBatchOperation();
      const chunk = data.slice(i, i + chunkSize);
      const chunkResults = await this.insert(table, chunk);
      results.push(...chunkResults);
    }

    return results;
  }

  /**
   * Update multiple rows with different values in a single query
   */
  async batchUpdate<T extends keyof Schema & string>(
    table: T,
    updates: Array<{
      set: Partial<Schema[T]>;
      where: Partial<Schema[T]>;
    }>
  ): Promise<Schema[T][]> {
    if (updates.length === 0) return [];

    // Validate batch size if security options are configured
    if (
      this.options.security?.maxBatchSize &&
      updates.length > this.options.security.maxBatchSize
    ) {
      throw new DatabaseError(
        `Batch update size ${updates.length} exceeds maximum allowed ${this.options.security.maxBatchSize}`,
        "BATCH_TOO_LARGE",
        { query: `UPDATE ${table}`, params: [], operation: "batchUpdate" }
      );
    }

    await this.rateLimitBatchOperation();

    // Execute all updates in a single transaction
    return this.transaction(async (tx) => {
      const promises = updates.map((update) =>
        tx.update(table, update.set, update.where)
      );
      const results = await Promise.all(promises);
      return results.flat();
    });
  }

  /**
   * Delete multiple rows with different conditions in a single transaction
   */
  async batchDelete<T extends keyof Schema & string>(
    table: T,
    conditions: Partial<Schema[T]>[]
  ): Promise<Schema[T][]> {
    if (conditions.length === 0) return [];

    // Validate batch size if security options are configured
    if (
      this.options.security?.maxBatchSize &&
      conditions.length > this.options.security.maxBatchSize
    ) {
      throw new DatabaseError(
        `Batch delete size ${conditions.length} exceeds maximum allowed ${this.options.security.maxBatchSize}`,
        "BATCH_TOO_LARGE",
        { query: `DELETE FROM ${table}`, params: [], operation: "batchDelete" }
      );
    }

    await this.rateLimitBatchOperation();

    // Execute all deletes in a single transaction
    return this.transaction(async (tx) => {
      const promises = conditions.map((where) => tx.delete(table, where));
      const results = await Promise.all(promises);
      return results.flat();
    });
  }
}

/**
 * Create a typed wrapper around a pg Pool
 * Can be used with or without a schema type
 *
 * @param poolOrConfig - Pool instance, connection string, or Pool config object
 * @param schema - Optional schema definition for type safety
 * @param optionsOrLogger - Options object or legacy logger (for backward compatibility)
 *
 * @example
 * // With connection string
 * const db = createTypedPg<MySchema>('postgresql://localhost/mydb');
 *
 * @example
 * // With options
 * const db = createTypedPg<MySchema>('postgresql://localhost/mydb', schema, {
 *   logger: new ConsoleLogger('debug'),
 *   timeout: 5000,
 *   retryAttempts: 3,
 *   onError: (err, ctx) => console.error('DB Error:', err)
 * });
 */
export function createTypedPg<
  Schema extends Record<string, any> = Record<string, any>
>(
  poolOrConfig: Pool | string | object,
  schema?: Schema,
  optionsOrLogger?: QueryLogger | TypedPgOptions
): TypedPg<Schema> {
  let pool: Pool;

  try {
    pool =
      poolOrConfig instanceof Pool
        ? poolOrConfig
        : new Pool(
            typeof poolOrConfig === "string"
              ? { connectionString: poolOrConfig }
              : poolOrConfig
          );

    // Test connection if it's a new pool
    if (!(poolOrConfig instanceof Pool)) {
      // Set up connection error handling
      if (pool.on) {
        pool.on("error", (err) => {
          // Error will be handled by TypedPg instance
          // This is just to prevent unhandled promise rejections
        });
      }

      // Try to connect to validate configuration (optional, for real pools)
      if (pool.connect) {
        const connectPromise = pool.connect();
        if (connectPromise && connectPromise.then) {
          connectPromise
            .then((client) => {
              if (client && client.release) {
                client.release();
              }
            })
            .catch((err) => {
              const options =
                optionsOrLogger && "log" in optionsOrLogger
                  ? {}
                  : optionsOrLogger || {};
              if (options.onError) {
                options.onError(err, {
                  query: "connection test",
                  params: [],
                  operation: "connect",
                });
              }
              // Don't throw here - let queries fail naturally with proper error handling
            });
        }
      }
    }

    return new TypedPg<Schema>(pool, schema, optionsOrLogger);
  } catch (error) {
    const options =
      optionsOrLogger && "log" in optionsOrLogger ? {} : optionsOrLogger || {};

    if (options.onError) {
      options.onError(error as Error, {
        query: "pool creation",
        params: [],
        operation: "init",
      });
    }

    throw new DatabaseError(
      `Failed to create database pool: ${(error as Error).message}`,
      (error as any).code,
      { query: "pool creation", params: [], operation: "init" },
      error as Error
    );
  }
}
