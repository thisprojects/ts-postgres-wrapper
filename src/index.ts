import { Pool, QueryResultRow } from "pg";

/**
 * Extract column names from a table type
 */
type ColumnNames<T> = keyof T & string;

/**
 * Pick specific columns from a table
 */
type SelectColumns<T, K extends keyof T & string> = Pick<T, K>;

/**
 * Type-safe query builder for a specific table
 */
export class TypedQuery<
  TableName extends string = string,
  Row extends Record<string, any> = Record<string, any>
> {
  private pool: Pool;
  private tableName: TableName;
  private selectedColumns: string[] = [];
  private whereClause: string = "";
  private whereParams: any[] = [];
  private orderByClause: string = "";
  private limitClause: string = "";
  private offsetClause: string = "";
  private paramCounter: number = 1;

  constructor(pool: Pool, tableName: TableName) {
    this.pool = pool;
    this.tableName = tableName;
  }

  /**
   * Select specific columns (type-safe!)
   */
  select<K extends keyof Row & string>(
    ...columns: K[]
  ): TypedQuery<TableName, SelectColumns<Row, K>> {
    const newQuery = new TypedQuery<TableName, SelectColumns<Row, K>>(
      this.pool,
      this.tableName
    );
    newQuery.selectedColumns = columns;
    newQuery.whereClause = this.whereClause;
    newQuery.whereParams = [...this.whereParams];
    newQuery.orderByClause = this.orderByClause;
    newQuery.limitClause = this.limitClause;
    newQuery.offsetClause = this.offsetClause;
    newQuery.paramCounter = this.paramCounter;
    return newQuery;
  }

  /**
   * Add WHERE clause
   */
  where<K extends ColumnNames<Row>>(
    column: K,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: Row[K] | Row[K][]
  ): this {
    if (this.whereClause) {
      this.whereClause += " AND ";
    } else {
      this.whereClause = " WHERE ";
    }

    if (operator === "IN" && Array.isArray(value)) {
      const placeholders = value
        .map(() => `$${this.paramCounter++}`)
        .join(", ");
      this.whereClause += `${String(column)} IN (${placeholders})`;
      this.whereParams.push(...value);
    } else {
      this.whereClause += `${String(column)} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    }
    return this;
  }

  /**
   * Add OR WHERE clause
   */
  orWhere<K extends ColumnNames<Row>>(
    column: K,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE",
    value: Row[K]
  ): this {
    if (this.whereClause) {
      this.whereClause += " OR ";
    } else {
      this.whereClause = " WHERE ";
    }
    this.whereClause += `${String(column)} ${operator} $${this.paramCounter}`;
    this.whereParams.push(value);
    this.paramCounter++;
    return this;
  }

  /**
   * Add ORDER BY clause
   */
  orderBy<K extends ColumnNames<Row>>(
    column: K,
    direction: "ASC" | "DESC" = "ASC"
  ): this {
    if (this.orderByClause) {
      this.orderByClause += `, ${String(column)} ${direction}`;
    } else {
      this.orderByClause = ` ORDER BY ${String(column)} ${direction}`;
    }
    return this;
  }

  /**
   * Add LIMIT clause
   */
  limit(count: number): this {
    this.limitClause = ` LIMIT ${count}`;
    return this;
  }

  /**
   * Add OFFSET clause
   */
  offset(count: number): this {
    this.offsetClause = ` OFFSET ${count}`;
    return this;
  }

  /**
   * Execute the query and return typed results
   */
  async execute(): Promise<Row[]> {
    const columns = this.selectedColumns.length
      ? this.selectedColumns.join(", ")
      : "*";

    const query = `SELECT ${columns} FROM ${String(this.tableName)}${
      this.whereClause
    }${this.orderByClause}${this.limitClause}${this.offsetClause}`;

    const result = await this.pool.query<Row>(query, this.whereParams);
    return result.rows;
  }

  /**
   * Execute and return first result or null
   */
  async first(): Promise<Row | null> {
    const results = await this.limit(1).execute();
    return results[0] || null;
  }

  /**
   * Get count of matching rows
   */
  async count(): Promise<number> {
    const query = `SELECT COUNT(*) as count FROM ${String(this.tableName)}${
      this.whereClause
    }`;
    const result = await this.pool.query<{ count: string }>(
      query,
      this.whereParams
    );
    return parseInt(result.rows[0].count, 10);
  }
}

/**
 * Main wrapper for type-safe queries
 */
export class TypedPg<Schema extends Record<string, any> = Record<string, any>> {
  private pool: Pool;

  constructor(pool: Pool) {
    this.pool = pool;
  }

  /**
   * Start a type-safe query on a table
   */
  table<T extends keyof Schema & string>(
    tableName: T
  ): TypedQuery<T, Schema[T]> {
    return new TypedQuery<T, Schema[T]>(this.pool, tableName);
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

    const query = `INSERT INTO ${String(tableName)} (${columns.join(
      ", "
    )}) VALUES ${values} RETURNING *`;

    const result = await this.pool.query<Schema[T]>(query, params);
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

    const setClause = setColumns
      .map((col, index) => `${col} = $${index + 1}`)
      .join(", ");
    const whereClause = whereColumns
      .map((col, index) => `${col} = $${setColumns.length + index + 1}`)
      .join(" AND ");

    const params = [
      ...setColumns.map((col) => data[col]),
      ...whereColumns.map((col) => where[col]),
    ];

    const query = `UPDATE ${String(
      tableName
    )} SET ${setClause} WHERE ${whereClause} RETURNING *`;

    const result = await this.pool.query<Schema[T]>(query, params);
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
    const whereClause = whereColumns
      .map((col, index) => `${col} = $${index + 1}`)
      .join(" AND ");
    const params = whereColumns.map((col) => where[col]);

    const query = `DELETE FROM ${String(
      tableName
    )} WHERE ${whereClause} RETURNING *`;

    const result = await this.pool.query<Schema[T]>(query, params);
    return result.rows;
  }

  /**
   * Raw query with manual type annotation
   */
  async raw<T extends Record<string, any> = Record<string, any>>(
    query: string,
    params?: any[]
  ): Promise<T[]> {
    const result = await this.pool.query<T>(query, params);
    return result.rows;
  }

  /**
   * Begin a transaction
   */
  async transaction<T>(
    callback: (client: TypedPg<Schema>) => Promise<T>
  ): Promise<T> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");
      const txPg = new TypedPg<Schema>(client as any);
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
}

/**
 * Create a typed wrapper around a pg Pool
 * Can be used with or without a schema type
 */
export function createTypedPg<
  Schema extends Record<string, any> = Record<string, any>
>(poolOrConfig: Pool | string | object): TypedPg<Schema> {
  const pool =
    poolOrConfig instanceof Pool
      ? poolOrConfig
      : new Pool(
          typeof poolOrConfig === "string"
            ? { connectionString: poolOrConfig }
            : poolOrConfig
        );

  return new TypedPg<Schema>(pool);
}

// Re-export Pool type for convenience
export { Pool } from "pg";
