import { Pool } from "pg";

/**
 * Extract column names from a table type
 */
type ColumnNames<T> = keyof T & string;

/**
 * Pick specific columns from a table
 */
type SelectColumns<T, K extends keyof T & string> = Pick<T, K>;

/**
 * Join configuration interface
 */
interface JoinConfig {
  type: "INNER" | "LEFT" | "RIGHT" | "FULL";
  table: string;
  leftColumn: string;
  rightColumn: string;
  alias?: string;
}

/**
 * Type-safe query builder for a specific table
 */
export class TypedQuery<
  TableName extends string = string,
  Row extends Record<string, any> = Record<string, any>,
  Schema extends Record<string, any> = Record<string, any>
> {
  private pool: Pool;
  private tableName: TableName;
  private tableAlias?: string;
  private selectedColumns: string[] = [];
  private whereClause: string = "";
  private whereParams: any[] = [];
  private orderByClause: string = "";
  private limitClause: string = "";
  private offsetClause: string = "";
  private paramCounter: number = 1;
  private joins: JoinConfig[] = [];
  private schema: Schema;
  private joinedTables: Set<string> = new Set();
  private groupByColumns: string[] = [];
  private havingClause: string = "";
  private havingParams: any[] = [];

  constructor(
    pool: Pool,
    tableName: TableName,
    schema?: Schema,
    tableAlias?: string
  ) {
    this.pool = pool;
    this.tableName = tableName;
    this.tableAlias = tableAlias;
    this.schema = schema || ({} as Schema);
    this.joinedTables.add(String(tableName));
  }

  /**
   * Clone the current query with all its state
   */
  private clone<NewRow extends Record<string, any> = Row>(): TypedQuery<
    TableName,
    NewRow,
    Schema
  > {
    const newQuery = new TypedQuery<TableName, NewRow, Schema>(
      this.pool,
      this.tableName,
      this.schema,
      this.tableAlias
    );
    newQuery.selectedColumns = [...this.selectedColumns];
    newQuery.whereClause = this.whereClause;
    newQuery.whereParams = [...this.whereParams];
    newQuery.orderByClause = this.orderByClause;
    newQuery.limitClause = this.limitClause;
    newQuery.offsetClause = this.offsetClause;
    newQuery.paramCounter = this.paramCounter;
    newQuery.joins = [...this.joins];
    newQuery.joinedTables = new Set(this.joinedTables);
    newQuery.groupByColumns = [...this.groupByColumns];
    newQuery.havingClause = this.havingClause;
    newQuery.havingParams = [...this.havingParams];
    return newQuery;
  }

  /**
   * Select specific columns (type-safe for known schemas, string-based for flexibility)
   */
  select<K extends keyof Row & string>(
    ...columns: K[]
  ): TypedQuery<TableName, SelectColumns<Row, K>, Schema>;
  select(...columns: string[]): TypedQuery<TableName, any, Schema>;
  select(...columns: any[]): TypedQuery<TableName, any, Schema> {
    const newQuery = this.clone<any>();
    newQuery.selectedColumns = columns;
    return newQuery;
  }

  /**
   * INNER JOIN with another table
   */
  innerJoin(
    joinedTable: string,
    leftColumn: string,
    rightColumn: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    return this.addJoin("INNER", joinedTable, leftColumn, rightColumn, alias);
  }

  /**
   * LEFT JOIN with another table
   */
  leftJoin(
    joinedTable: string,
    leftColumn: string,
    rightColumn: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    return this.addJoin("LEFT", joinedTable, leftColumn, rightColumn, alias);
  }

  /**
   * RIGHT JOIN with another table
   */
  rightJoin(
    joinedTable: string,
    leftColumn: string,
    rightColumn: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    return this.addJoin("RIGHT", joinedTable, leftColumn, rightColumn, alias);
  }

  /**
   * FULL OUTER JOIN with another table
   */
  fullJoin(
    joinedTable: string,
    leftColumn: string,
    rightColumn: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    return this.addJoin("FULL", joinedTable, leftColumn, rightColumn, alias);
  }

  /**
   * Internal method to add joins
   */
  private addJoin(
    type: "INNER" | "LEFT" | "RIGHT" | "FULL",
    joinedTable: string,
    leftColumn: string,
    rightColumn: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    const newQuery = this.clone<any>();

    newQuery.joins.push({
      type,
      table: joinedTable,
      leftColumn,
      rightColumn,
      alias,
    });

    newQuery.joinedTables.add(alias || joinedTable);

    return newQuery;
  }

  /**
   * Add WHERE clause (supports both typed columns and string-based columns)
   */
  where<K extends ColumnNames<Row>>(
    column: K,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: Row[K] | Row[K][]
  ): this;
  where(
    column: string,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: any
  ): this;
  where(
    column: any,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: any
  ): this {
    if (this.whereClause) {
      this.whereClause += " AND ";
    } else {
      this.whereClause = " WHERE ";
    }

    // Auto-qualify column if not already qualified and no ambiguity
    const qualifiedColumn = this.qualifyColumnName(String(column));

    if (operator === "IN" && Array.isArray(value)) {
      const placeholders = value
        .map(() => `$${this.paramCounter++}`)
        .join(", ");
      this.whereClause += `${qualifiedColumn} IN (${placeholders})`;
      this.whereParams.push(...value);
    } else {
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    }
    return this;
  }

  /**
   * Add OR WHERE clause (supports both typed columns and string-based columns)
   */
  orWhere<K extends ColumnNames<Row>>(
    column: K,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE",
    value: Row[K]
  ): this;
  orWhere(
    column: string,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: any
  ): this;
  orWhere(
    column: any,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: any
  ): this {
    if (this.whereClause) {
      this.whereClause += " OR ";
    } else {
      this.whereClause = " WHERE ";
    }

    const qualifiedColumn = this.qualifyColumnName(String(column));

    if (operator === "IN" && Array.isArray(value)) {
      const placeholders = value
        .map(() => `$${this.paramCounter++}`)
        .join(", ");
      this.whereClause += `${qualifiedColumn} IN (${placeholders})`;
      this.whereParams.push(...value);
    } else {
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    }
    return this;
  }

  /**
   * Add ORDER BY clause (supports both typed columns and string-based columns)
   */
  orderBy<K extends ColumnNames<Row>>(
    column: K,
    direction?: "ASC" | "DESC"
  ): this;
  orderBy(column: string, direction?: "ASC" | "DESC"): this;
  orderBy(column: any, direction: "ASC" | "DESC" = "ASC"): this {
    const qualifiedColumn = this.qualifyColumnName(String(column));

    if (this.orderByClause) {
      this.orderByClause += `, ${qualifiedColumn} ${direction}`;
    } else {
      this.orderByClause = ` ORDER BY ${qualifiedColumn} ${direction}`;
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
   * Add GROUP BY clause (supports both typed columns and string-based columns)
   */
  groupBy<K extends ColumnNames<Row>>(...columns: K[]): this;
  groupBy(...columns: string[]): this;
  groupBy(...columns: any[]): this {
    const qualifiedColumns = columns.map(col => this.qualifyColumnName(String(col)));
    this.groupByColumns.push(...qualifiedColumns);
    return this;
  }

  /**
   * Add HAVING clause for aggregate conditions
   */
  having(
    column: string,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
    value: any
  ): this {
    if (this.havingClause) {
      this.havingClause += " AND ";
    } else {
      this.havingClause = " HAVING ";
    }

    const qualifiedColumn = this.qualifyColumnName(String(column));

    if (operator === "IN" && Array.isArray(value)) {
      const placeholders = value
        .map(() => `$${this.paramCounter++}`)
        .join(", ");
      this.havingClause += `${qualifiedColumn} IN (${placeholders})`;
      this.havingParams.push(...value);
    } else {
      this.havingClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.havingParams.push(value);
      this.paramCounter++;
    }
    return this;
  }

  /**
   * Qualify column name if needed
   */
  private qualifyColumnName(column: string): string {
    // If column is already qualified (contains a dot), return as-is
    if (column.includes(".")) {
      return column;
    }

    // Only qualify columns when we have JOINs (to avoid ambiguity)
    // For single table queries, use unqualified names for backward compatibility
    if (this.joins.length === 0) {
      return column;
    }

    // For queries with JOINs, qualify with main table reference to avoid ambiguity
    return `${this.getTableReference()}.${column}`;
  }

  /**
   * Get the table reference (with alias if applicable)
   */
  private getTableReference(): string {
    return this.tableAlias || String(this.tableName);
  }

  /**
   * Build the FROM clause with joins
   */
  private buildFromClause(): string {
    let fromClause = `FROM ${String(this.tableName)}`;
    if (this.tableAlias) {
      fromClause += ` AS ${this.tableAlias}`;
    }

    for (const join of this.joins) {
      const joinTableRef = join.alias
        ? `${join.table} AS ${join.alias}`
        : join.table;
      fromClause += ` ${join.type} JOIN ${joinTableRef} ON ${join.leftColumn} = ${join.rightColumn}`;
    }

    return fromClause;
  }

  /**
   * Execute the query and return typed results
   */
  async execute(): Promise<Row[]> {
    // Validate HAVING is only used with GROUP BY
    if (this.havingClause && !this.groupByColumns.length) {
      throw new Error("HAVING clause requires GROUP BY");
    }

    const columns = this.selectedColumns.length
      ? this.selectedColumns.join(", ")
      : "*";

    const query = `SELECT ${columns} ${this.buildFromClause()}${
      this.whereClause
    }${this.groupByColumns.length ? ` GROUP BY ${this.groupByColumns.join(", ")}` : ""}${
      this.havingClause
    }${this.orderByClause}${this.limitClause}${this.offsetClause}`;

    const result = await this.pool.query<Row>(query, [...this.whereParams, ...this.havingParams]);
    return result.rows;
  }

  /**
   * Execute and return first result or null
   */
  async first(): Promise<Row | null> {
    const results = await this.clone().limit(1).execute();
    return results[0] || null;
  }

  /**
   * Get count of matching rows
   */
  async count(): Promise<number> {
    const query = `SELECT COUNT(*) as count ${this.buildFromClause()}${
      this.whereClause
    }`;
    const result = await this.pool.query<{ count: string }>(
      query,
      this.whereParams
    );
    return parseInt(result.rows[0].count, 10);
  }

  /**
   * Get the SQL query string (for debugging)
   */
  toSQL(): { query: string; params: any[] } {
    const columns = this.selectedColumns.length
      ? this.selectedColumns.join(", ")
      : "*";

    const query = `SELECT ${columns} ${this.buildFromClause()}${
      this.whereClause
    }${this.groupByColumns.length ? ` GROUP BY ${this.groupByColumns.join(", ")}` : ""}${
      this.havingClause
    }${this.orderByClause}${this.limitClause}${this.offsetClause}`;

    return { query, params: [...this.whereParams, ...this.havingParams] };
  }
}

/**
 * Main wrapper for type-safe queries
 */
export class TypedPg<Schema extends Record<string, any> = Record<string, any>> {
  private pool: Pool;
  private schema: Schema;

  constructor(pool: Pool, schema?: Schema) {
    this.pool = pool;
    this.schema = schema || ({} as Schema);
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
      alias
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
      const txPg = new TypedPg<Schema>(client as any, this.schema);
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
>(poolOrConfig: Pool | string | object, schema?: Schema): TypedPg<Schema> {
  const pool =
    poolOrConfig instanceof Pool
      ? poolOrConfig
      : new Pool(
          typeof poolOrConfig === "string"
            ? { connectionString: poolOrConfig }
            : poolOrConfig
        );

  return new TypedPg<Schema>(pool, schema);
}

// Re-export Pool type for convenience
export { Pool } from "pg";
