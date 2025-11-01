import { Pool } from "pg";
import type {
  QueryLogger,
  JSONOperator,
  JSONPath,
  JSONValue,
  ColumnNames,
  ColumnAlias,
  ColumnSpec,
  ResultColumns,
  JoinCondition,
  JoinConfig,
} from "./types";
import { sanitizeSqlIdentifier } from "./utils";
import { DatabaseError } from "./errors";

/**
 * Type-safe query builder for a specific table
 */
export type QueryExecutor = <T = any>(
  query: string,
  params: any[],
  operation?: string
) => Promise<{ rows: T[] }>;

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
  private caseInsensitive: boolean = false;
  private joins: JoinConfig[] = [];
  private logger?: QueryLogger;
  private queryExecutor?: QueryExecutor;

  /**
   * Make text comparisons case insensitive
   */
  ignoreCase(): this {
    this.caseInsensitive = true;
    return this;
  }

  /**
   * Make text comparisons case sensitive (default)
   */
  matchCase(): this {
    this.caseInsensitive = false;
    return this;
  }
  private schema: Schema;
  private joinedTables: Set<string> = new Set();
  private groupByColumns: string[] = [];
  private havingClause: string = "";
  private havingParams: any[] = [];
  private windowFunctions: string[] = [];
  private isDistinct: boolean = false;

  constructor(
    pool: Pool,
    tableName: TableName,
    schema?: Schema,
    tableAlias?: string,
    logger?: QueryLogger,
    queryExecutor?: QueryExecutor
  ) {
    this.pool = pool;
    this.tableName = tableName;
    this.tableAlias = tableAlias;
    this.schema = schema || ({} as Schema);
    this.logger = logger;
    this.queryExecutor = queryExecutor;
    this.joinedTables.add(String(tableName));
  }

  /**
   * Clone the current query with all its state
   */
  clone<NewRow extends Record<string, any> = Row>(): TypedQuery<
    TableName,
    NewRow,
    Schema
  > {
    const newQuery = new TypedQuery<TableName, NewRow, Schema>(
      this.pool,
      this.tableName,
      this.schema,
      this.tableAlias,
      this.logger,
      this.queryExecutor
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
    newQuery.windowFunctions = [...this.windowFunctions];
    newQuery.isDistinct = this.isDistinct;
    newQuery.caseInsensitive = this.caseInsensitive;
    return newQuery;
  }

  /**
   * Enable DISTINCT for this query
   */
  distinct(): this {
    this.isDistinct = true;
    return this;
  }

  /**
   * Select specific columns with optional aliases (type-safe for known schemas)
   *
   * Supports three forms:
   * 1. Column names from schema: .select("id", "name")
   * 2. Aliased columns (typed): .select({ column: "firstName" as keyof Row, as: "name" })
   * 3. String expressions: .select("COUNT(*)", "MAX(age)") - returns any for these fields
   */
  select<S extends ColumnSpec<Row>[]>(
    ...columns: S
  ): TypedQuery<TableName, ResultColumns<Row, S>, Schema>;
  select(
    ...columns: (string | ColumnAlias<Row> | { column: string; as: string })[]
  ): TypedQuery<TableName, Record<string, any>, Schema> {
    const newQuery = this.clone<any>();
    newQuery.selectedColumns = columns.map((col) => {
      if (typeof col === "string") {
        // String columns are treated as simple column names - no "AS" parsing
        // To use aliases, use the object syntax: { column: "name", as: "alias" }

        // Validate that string columns are simple identifiers or qualified names (table.column)
        // SQL expressions with functions, operators, or dangerous patterns must use expr() helper
        if (
          col.includes('(') ||
          col.includes(';') ||
          col.includes('--') ||
          /\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|\bUNION\b/i.test(col)
        ) {
          throw new Error(
            `Invalid column name "${col}". SQL expressions and functions must use expr() helper. ` +
            `Example: select(expr("COUNT(*)", "total"))`
          );
        }

        return this.qualifyColumnName(col);
      } else {
        // Object syntax - validate based on whether it's marked as a safe expression
        const columnStr = String(col.column);

        // If not marked as an expression (via expr() helper), validate like string columns
        // EXCEPT for safe library-generated functions (jsonb_*, JSON operators, etc.)
        if (!(col as any).__isExpression) {
          // Check if this is a safe library-generated expression
          const isSafeLibraryExpression = /^(jsonb_set|jsonb_insert|jsonb_concat|jsonb_build_object|jsonb_build_array|jsonb_delete_path|jsonb_object_keys|jsonb_typeof|jsonb_array_length)\(/.test(columnStr);
          // Check for JSON operators but NOT parentheses (those are function calls)
          const hasJsonOperator = /[@?#\[\]]|->|#-|\s-\s|\|\||::jsonb/.test(columnStr);

          // For non-safe expressions, enforce validation
          if (!isSafeLibraryExpression && !hasJsonOperator) {
            if (
              columnStr.includes('(') ||
              columnStr.includes(';') ||
              columnStr.includes('--') ||
              /\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|\bUNION\b/i.test(columnStr)
            ) {
              throw new Error(
                `Invalid column name "${columnStr}". SQL expressions and functions must use expr() helper. ` +
                `Example: select(expr("COUNT(*)", "total"))`
              );
            }
          }
        }

        // Always sanitize column names, even in object syntax
        // Object syntax goes through qualifyColumnName() which applies sanitizeIdentifier()
        // with appropriate security checks for complex expressions
        const colExpr = this.qualifyColumnName(columnStr);
        return `${colExpr} AS ${this.sanitizeIdentifier(col.as)}`;
      }
    });
    return newQuery;
  }

  /**
   * INNER JOIN with another table
   */
  innerJoin<JoinedTable extends keyof Schema & string>(
    joinedTable: JoinedTable,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, Row & Schema[JoinedTable], Schema>;
  innerJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema>;
  innerJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin(
        "INNER",
        joinedTable,
        [
          {
            leftColumn: leftColumnOrConditions,
            rightColumn: rightColumnOrAlias as string,
          },
        ],
        alias
      );
    } else {
      // Compound key join
      return this.addJoin(
        "INNER",
        joinedTable,
        leftColumnOrConditions,
        rightColumnOrAlias
      );
    }
  }

  /**
   * LEFT JOIN with another table
   */
  leftJoin<JoinedTable extends keyof Schema & string>(
    joinedTable: JoinedTable,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, Row & Partial<Schema[JoinedTable]>, Schema>;
  leftJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema>;
  leftJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin(
        "LEFT",
        joinedTable,
        [
          {
            leftColumn: leftColumnOrConditions,
            rightColumn: rightColumnOrAlias as string,
          },
        ],
        alias
      );
    } else {
      // Compound key join
      return this.addJoin(
        "LEFT",
        joinedTable,
        leftColumnOrConditions,
        rightColumnOrAlias
      );
    }
  }

  /**
   * RIGHT JOIN with another table
   */
  rightJoin<JoinedTable extends keyof Schema & string>(
    joinedTable: JoinedTable,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, Partial<Row> & Schema[JoinedTable], Schema>;
  rightJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema>;
  rightJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin(
        "RIGHT",
        joinedTable,
        [
          {
            leftColumn: leftColumnOrConditions,
            rightColumn: rightColumnOrAlias as string,
          },
        ],
        alias
      );
    } else {
      // Compound key join
      return this.addJoin(
        "RIGHT",
        joinedTable,
        leftColumnOrConditions,
        rightColumnOrAlias
      );
    }
  }

  /**
   * FULL OUTER JOIN with another table
   */
  fullJoin<JoinedTable extends keyof Schema & string>(
    joinedTable: JoinedTable,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, Partial<Row> & Partial<Schema[JoinedTable]>, Schema>;
  fullJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema>;
  fullJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin(
        "FULL",
        joinedTable,
        [
          {
            leftColumn: leftColumnOrConditions,
            rightColumn: rightColumnOrAlias as string,
          },
        ],
        alias
      );
    } else {
      // Compound key join
      return this.addJoin(
        "FULL",
        joinedTable,
        leftColumnOrConditions,
        rightColumnOrAlias
      );
    }
  }

  /**
   * Internal method to add joins
   */
  private addJoin(
    type: "INNER" | "LEFT" | "RIGHT" | "FULL",
    joinedTable: string,
    conditions: JoinCondition[],
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    const newQuery = this.clone<any>();

    newQuery.joins.push({
      type,
      table: joinedTable,
      conditions,
      alias,
    });

    newQuery.joinedTables.add(alias || joinedTable);

    return newQuery;
  }

  /**
   * Validate parameter value for size to prevent DoS attacks
   */
  private validateParameterSize(value: any, context: string = "Parameter"): void {
    if (typeof value === 'string') {
      // 10MB limit for string parameters
      const maxSize = 10 * 1024 * 1024;
      if (value.length > maxSize) {
        throw new DatabaseError(
          `${context} exceeds maximum size of 10 MB`,
          'PARAMETER_TOO_LARGE',
          { query: '', params: [], detail: `length: ${value.length} bytes` }
        );
      }
    } else if (Array.isArray(value)) {
      // Recursively validate array elements
      value.forEach((item, index) => {
        this.validateParameterSize(item, `${context}[${index}]`);
      });
    }
  }

  /**
   * Validate operator to prevent SQL injection at runtime
   */
  private validateOperator(operator: string): void {
    const validOperators = new Set([
      '=', '!=', '<>', '<', '>', '<=', '>=',
      'LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE',
      'IN', 'NOT IN', 'BETWEEN', 'IS NULL', 'IS NOT NULL',
      '~', '~*', '!~', '!~*', // Regex operators
      '->', '->>', '#>', '#>>',  // JSON operators
      '?', '?|', '?&', '@>', '<@', '@@', '@?', '#-', '||'
    ]);

    const normalizedOp = operator.trim().toUpperCase();
    if (!validOperators.has(normalizedOp) && !validOperators.has(operator.trim())) {
      throw new DatabaseError(
        `Invalid operator: ${operator}`,
        'INVALID_OPERATOR',
        { query: '', params: [], detail: `operator: ${operator}` }
      );
    }
  }

  /**
   * Add WHERE clause (supports both typed columns and string-based columns)
   */
  where<K extends ColumnNames<Row>>(
    column: K,
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN"
      | "BETWEEN"
      | "IS NULL"
      | "IS NOT NULL"
      | JSONOperator,
    value: Row[K] | Row[K][] | JSONValue | JSONPath
  ): this;
  where(
    column: string,
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN"
      | "BETWEEN"
      | "IS NULL"
      | "IS NOT NULL"
      | JSONOperator,
    value: any
  ): this;
  where(
    column: string,
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN"
      | "BETWEEN"
      | "IS NULL"
      | "IS NOT NULL"
      | JSONOperator,
    value: unknown
  ): this {
    // Validate operator at runtime (TypeScript types can be bypassed)
    this.validateOperator(operator);

    // Validate parameter size to prevent DoS
    this.validateParameterSize(value, 'WHERE parameter');

    // Check WHERE clause count limit
    const whereCount = this.whereParams.length;
    if (whereCount >= 500) {
      throw new DatabaseError(
        'WHERE clause count exceeds maximum of 500 conditions',
        'TOO_MANY_WHERE_CONDITIONS',
        { query: '', params: [], detail: `current count: ${whereCount}` }
      );
    }

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
    } else if (
      operator === "BETWEEN" &&
      Array.isArray(value) &&
      value.length === 2
    ) {
      this.whereClause += `${qualifiedColumn} BETWEEN $${
        this.paramCounter
      } AND $${this.paramCounter + 1}`;
      this.whereParams.push(value[0], value[1]);
      this.paramCounter += 2;
    } else if (operator === "IS NULL" || operator === "IS NOT NULL") {
      this.whereClause += `${qualifiedColumn} ${operator}`;
    } else if (
      operator === "->" ||
      operator === "->>" ||
      operator === "#>" ||
      operator === "#>>"
    ) {
      // JSON field/path access operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    } else if (operator === "@>" || operator === "<@") {
      // JSON containment operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}::jsonb`;
      this.whereParams.push(JSON.stringify(value));
      this.paramCounter++;
    } else if (operator === "?" || operator === "?|" || operator === "?&") {
      // JSON key existence operators
      if (Array.isArray(value) && (operator === "?|" || operator === "?&")) {
        this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}::text[]`;
        this.whereParams.push(value);
        this.paramCounter++;
      } else {
        this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
        this.whereParams.push(value);
        this.paramCounter++;
      }
    } else if (operator === "@?" || operator === "@@") {
      // JSONPath operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    } else {
      let finalOperator = operator;
      if (
        this.caseInsensitive &&
        (operator === "=" || operator === "!=" || operator === "LIKE")
      ) {
        if (operator === "=") finalOperator = "ILIKE";
        else if (operator === "!=") {
          finalOperator = "NOT ILIKE";
          value = String(value).replace(/^%|%$/g, ""); // Remove wildcards for exact match
        } else if (operator === "LIKE") finalOperator = "ILIKE";
        value = String(value);
      }
      this.whereClause += `${qualifiedColumn} ${finalOperator} $${this.paramCounter}`;
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
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN"
      | "BETWEEN"
      | "IS NULL"
      | "IS NOT NULL"
      | JSONOperator,
    value: Row[K] | Row[K][] | JSONValue | JSONPath
  ): this;
  orWhere(
    column: string,
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN"
      | "BETWEEN"
      | "IS NULL"
      | "IS NOT NULL"
      | JSONOperator,
    value: any
  ): this;
  orWhere(
    column: any,
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN"
      | "BETWEEN"
      | "IS NULL"
      | "IS NOT NULL"
      | JSONOperator,
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
    } else if (
      operator === "BETWEEN" &&
      Array.isArray(value) &&
      value.length === 2
    ) {
      this.whereClause += `${qualifiedColumn} BETWEEN $${
        this.paramCounter
      } AND $${this.paramCounter + 1}`;
      this.whereParams.push(value[0], value[1]);
      this.paramCounter += 2;
    } else if (operator === "IS NULL" || operator === "IS NOT NULL") {
      this.whereClause += `${qualifiedColumn} ${operator}`;
    } else if (
      operator === "->" ||
      operator === "->>" ||
      operator === "#>" ||
      operator === "#>>"
    ) {
      // JSON field/path access operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    } else if (operator === "@>" || operator === "<@") {
      // JSON containment operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}::jsonb`;
      this.whereParams.push(JSON.stringify(value));
      this.paramCounter++;
    } else if (operator === "?" || operator === "?|" || operator === "?&") {
      // JSON key existence operators
      if (Array.isArray(value) && (operator === "?|" || operator === "?&")) {
        this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}::text[]`;
        this.whereParams.push(value);
        this.paramCounter++;
      } else {
        this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
        this.whereParams.push(value);
        this.paramCounter++;
      }
    } else if (operator === "@?" || operator === "@@") {
      // JSONPath operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    } else {
      let finalOperator = operator;
      if (
        this.caseInsensitive &&
        (operator === "=" || operator === "!=" || operator === "LIKE")
      ) {
        if (operator === "=") finalOperator = "ILIKE";
        else if (operator === "!=") {
          finalOperator = "NOT ILIKE";
          value = String(value).replace(/^%|%$/g, ""); // Remove wildcards for exact match
        } else if (operator === "LIKE") finalOperator = "ILIKE";
        value = String(value);
      }
      this.whereClause += `${qualifiedColumn} ${finalOperator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    }
    return this;
  }

  /**
   * Validate ORDER BY column
   */
  private validateOrderByColumn(column: string): void {
    const isAggregateOrWindowFunc = this.isAggregateFunction(column);
    const isQualifiedColumn = column.includes(".");

    if (isAggregateOrWindowFunc) {
      return;
    }

    const tableName = this.tableAlias || String(this.tableName);
    const baseColumn = String(column).split(".").pop() || "";

    // Allow ordering by columns in GROUP BY
    if (this.groupByColumns.includes(this.qualifyColumnName(column))) {
      return;
    }

    // Skip validation for qualified columns from other tables or test mode
    if (isQualifiedColumn || !this.schema[tableName]) {
      return;
    }

    // Check if column exists in schema
    if (!Object.keys(this.schema[tableName]).includes(baseColumn)) {
      throw new Error(`Invalid column name in ORDER BY: ${column}`);
    }
  }

  /**
   * Add ORDER BY clause (supports both typed columns and string-based columns)
   */
  orderBy<K extends ColumnNames<Row>>(
    column: K,
    direction?: "ASC" | "DESC"
  ): this;
  orderBy(column: string, direction?: "ASC" | "DESC"): this;
  orderBy(column: string, direction: "ASC" | "DESC" = "ASC"): this {
    // Validate direction at runtime (TypeScript types can be bypassed)
    const normalizedDir = direction.trim().toUpperCase();
    if (normalizedDir !== 'ASC' && normalizedDir !== 'DESC') {
      throw new DatabaseError(
        `Invalid ORDER BY direction: ${direction}. Must be ASC or DESC`,
        'INVALID_DIRECTION',
        { query: '', params: [], detail: `direction: ${direction}` }
      );
    }

    // Check ORDER BY clause count limit (100 max)
    const orderByCount = (this.orderByClause.match(/,/g) || []).length + (this.orderByClause ? 1 : 0);
    if (orderByCount >= 100) {
      throw new DatabaseError(
        'ORDER BY clause count exceeds maximum of 100',
        'TOO_MANY_ORDER_BY',
        { query: '', params: [], detail: `current count: ${orderByCount}` }
      );
    }

    this.validateOrderByColumn(String(column));
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
   * @param count - Maximum number of rows to return (must be positive integer)
   * @throws {Error} If count is not a valid positive integer
   */
  limit(count: number): this {
    // Validate that count is a finite number
    if (!Number.isFinite(count)) {
      throw new Error(`LIMIT value must be a finite number. Got: ${count}`);
    }

    // Validate that count is an integer
    if (!Number.isInteger(count)) {
      throw new Error(`LIMIT value must be an integer. Got: ${count}`);
    }

    // Validate that count is positive
    if (count <= 0) {
      throw new Error(`LIMIT value must be positive. Got: ${count}`);
    }

    // Validate reasonable upper bound to prevent DoS (10 million rows)
    const MAX_LIMIT = 10000000;
    if (count > MAX_LIMIT) {
      throw new Error(
        `LIMIT value ${count} exceeds maximum allowed value of ${MAX_LIMIT}. ` +
          `Large result sets should be paginated using LIMIT and OFFSET.`
      );
    }

    this.limitClause = ` LIMIT ${count}`;
    return this;
  }

  /**
   * Add OFFSET clause
   * @param count - Number of rows to skip (must be non-negative integer)
   * @throws {Error} If count is not a valid non-negative integer
   */
  offset(count: number): this {
    // Validate that count is a finite number
    if (!Number.isFinite(count)) {
      throw new Error(`OFFSET value must be a finite number. Got: ${count}`);
    }

    // Validate that count is an integer
    if (!Number.isInteger(count)) {
      throw new Error(`OFFSET value must be an integer. Got: ${count}`);
    }

    // Validate that count is non-negative
    if (count < 0) {
      throw new Error(`OFFSET value must be non-negative. Got: ${count}`);
    }

    // Validate reasonable upper bound to prevent DoS (100 million rows)
    const MAX_OFFSET = 100000000;
    if (count > MAX_OFFSET) {
      throw new Error(
        `OFFSET value ${count} exceeds maximum allowed value of ${MAX_OFFSET}. ` +
          `For deep pagination, consider using cursor-based pagination instead.`
      );
    }

    this.offsetClause = ` OFFSET ${count}`;
    return this;
  }

  /**
   * Add GROUP BY clause (supports both typed columns and string-based columns)
   */
  groupBy<K extends ColumnNames<Row>>(...columns: K[]): this;
  groupBy(...columns: string[]): this;
  groupBy(...columns: string[]): this {
    if (columns.length === 0) {
      throw new Error("GROUP BY clause requires at least one column");
    }

    const invalidColumns = columns.filter((col) => {
      const tableName = this.tableAlias || String(this.tableName);
      const isQualified = String(col).includes(".");
      // Skip validation for qualified columns and test mode
      if (isQualified || !this.schema[tableName]) {
        return false;
      }
      return !Object.keys(this.schema[tableName]).includes(
        String(col).split(".").pop() || ""
      );
    });

    if (invalidColumns.length > 0) {
      throw new Error(
        `Invalid column names in GROUP BY: ${invalidColumns.join(", ")}`
      );
    }

    const qualifiedColumns = columns.map((col) =>
      this.qualifyColumnName(String(col))
    );
    this.groupByColumns.push(...qualifiedColumns);
    return this;
  }

  /**
   * Check if expression is an aggregate function
   */
  private isAggregateFunction(expr: string): boolean {
    const aggFuncs = [
      "COUNT",
      "SUM",
      "AVG",
      "MIN",
      "MAX",
      "VARIANCE",
      "FIRST_VALUE",
      "ROW_NUMBER",
      "RANK",
      "DENSE_RANK",
    ];
    const upperExpr = expr.toUpperCase();
    return (
      aggFuncs.some((func) => upperExpr.includes(func + "(")) ||
      upperExpr.includes(" OVER ")
    );
  }

  /**
   * Escape single quotes in SQL strings
   */
  private escapeSingleQuotes(value: string): string {
    return value.replace(/'/g, "''");
  }

  /**
   * Validate JSON field/path component to prevent SQL injection
   * JSON field names should be safe identifiers
   */
  private validateJsonIdentifier(value: string, context: string = "JSON identifier"): string {
    const strValue = String(value).trim();

    if (strValue.length === 0) {
      throw new DatabaseError(
        `Invalid ${context}: cannot be empty`,
        'INVALID_JSON_IDENTIFIER',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    // Check for dangerous characters that could bypass escaping
    if (/[;"`\\]|--|\*\/|\/\*/.test(strValue)) {
      throw new DatabaseError(
        `Invalid ${context}: contains dangerous characters`,
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    // Check for SQL keywords as whole words
    if (/\b(UNION|SELECT|DROP|INSERT|UPDATE|DELETE|TRUNCATE|ALTER|EXEC|EXECUTE)\b/i.test(strValue)) {
      throw new DatabaseError(
        `Invalid ${context}: contains SQL keywords`,
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    // Check for SQL operators in suspicious contexts
    if (/(^|\s)(OR|AND)(\s|$)/i.test(strValue) || /'.*?(OR|AND).*?'/i.test(strValue)) {
      throw new DatabaseError(
        `Invalid ${context}: contains SQL operators in suspicious context`,
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    if (strValue.length > 255) {
      throw new DatabaseError(
        `Invalid ${context}: exceeds maximum length of 255 characters`,
        'INVALID_JSON_IDENTIFIER',
        { query: '', params: [], detail: `value: ${strValue.substring(0, 50)}...` }
      );
    }

    return strValue;
  }

  /**
   * Get JSON object field (type-safe)
   * @template T - The type of the JSON object
   * @param column - The column name containing JSON
   * @param field - The field name (type-checked against T)
   */
  jsonField<K extends ColumnNames<Row>, T = Row[K]>(
    column: K,
    field: T extends Record<string, any> ? keyof T : string
  ): string;
  jsonField<T extends Record<string, any>>(
    column: string,
    field: keyof T
  ): string;
  jsonField(column: string, field: string): string {
    const validatedField = this.validateJsonIdentifier(field, "JSON field name");
    return `${this.qualifyColumnName(column)}->'${this.escapeSingleQuotes(validatedField)}'`;
  }

  /**
   * Get JSON object field as text (type-safe)
   * @template T - The type of the JSON object
   * @param column - The column name containing JSON
   * @param field - The field name (type-checked against T)
   */
  jsonFieldAsText<K extends ColumnNames<Row>, T = Row[K]>(
    column: K,
    field: T extends Record<string, any> ? keyof T : string
  ): string;
  jsonFieldAsText<T extends Record<string, any>>(
    column: string,
    field: keyof T
  ): string;
  jsonFieldAsText(column: string, field: string): string {
    const validatedField = this.validateJsonIdentifier(field, "JSON field name");
    return `${this.qualifyColumnName(column)}->>'${this.escapeSingleQuotes(validatedField)}'`;
  }

  /**
   * Get JSON object at path (type-safe)
   * @template T - The type of the JSON object
   * @template P - The path tuple type
   * @param column - The column name containing JSON
   * @param path - Array of path segments (type-checked for valid paths)
   */
  jsonPath<
    K extends ColumnNames<Row>,
    T = Row[K],
    P extends readonly string[] = readonly string[]
  >(column: K, path: P): string;
  jsonPath(column: string, path: string[]): string;
  jsonPath(column: string, path: string[]): string {
    // Validate path depth to prevent DoS
    if (path.length > 20) {
      throw new DatabaseError(
        'JSON path exceeds maximum depth of 20 levels',
        'PATH_TOO_DEEP',
        { query: '', params: [], detail: `depth: ${path.length}` }
      );
    }

    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const pathStr = validatedPath
      .map((p) => `'${this.escapeSingleQuotes(p)}'`)
      .join(",");
    return `${this.qualifyColumnName(column)}#>ARRAY[${pathStr}]`;
  }

  /**
   * Get JSON object at path as text (type-safe)
   * @template T - The type of the JSON object
   * @template P - The path tuple type
   * @param column - The column name containing JSON
   * @param path - Array of path segments (type-checked for valid paths)
   */
  jsonPathAsText<
    K extends ColumnNames<Row>,
    T = Row[K],
    P extends readonly string[] = readonly string[]
  >(column: K, path: P): string;
  jsonPathAsText(column: string, path: string[]): string;
  jsonPathAsText(column: string, path: string[]): string {
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const pathStr = validatedPath
      .map((p) => `'${this.escapeSingleQuotes(p)}'`)
      .join(",");
    return `${this.qualifyColumnName(column)}#>>ARRAY[${pathStr}]`;
  }

  /**
   * Check if JSON object exists at path
   */
  hasJsonPath(column: string, path: string[]): this {
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const pathStr = validatedPath
      .map((p) => `'${this.escapeSingleQuotes(p)}'`)
      .join(",");
    return this.where(
      `${this.qualifyColumnName(column)}#>ARRAY[${pathStr}]`,
      "IS NOT NULL",
      null
    );
  }

  /**
   * Check if JSON object contains the given JSON value
   */
  containsJson<K extends ColumnNames<Row>>(
    column: K,
    value: Row[K] extends Record<string, any> ? Partial<Row[K]> : never
  ): this {
    return this.where(column, "@>", value);
  }

  /**
   * Check if JSON object is contained within the given JSON value
   */
  containedInJson<K extends ColumnNames<Row>>(
    column: K,
    value: Row[K] extends Record<string, any> ? Record<string, any> : never
  ): this {
    return this.where(column, "<@", value);
  }

  /**
   * Check if JSON object has the given key at top level
   */
  hasJsonKey(column: string, key: string): this {
    return this.where(column, "?", key);
  }

  /**
   * Check if JSON object has any of the given keys at top level
   */
  hasAnyJsonKey(column: string, keys: string[]): this {
    return this.where(column, "?|", keys);
  }

  /**
   * Check if JSON object has all of the given keys at top level
   */
  hasAllJsonKeys(column: string, keys: string[]): this {
    return this.where(column, "?&", keys);
  }

  /**
   * Query JSON object with JSONPath expression
   */
  jsonPathQuery(column: string, path: string): this {
    return this.where(column, "@?", path);
  }

  /**
   * Match JSON object with JSONPath predicate
   */
  jsonPathMatch(column: string, path: string): this {
    return this.where(column, "@@", path);
  }

  /**
   * Create a JSONB set expression for updating a value at a path
   * Usage: .select(jsonbSet("data", ["address", "city"], "New York"))
   * Returns: jsonb_set(data, '{address,city}', '"New York"'::jsonb)
   */
  jsonbSet(
    column: string,
    path: string[],
    value: any,
    createMissing: boolean = true
  ): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
    const pathStr = `{${escapedPath.join(",")}}`;
    const jsonValueStr = this.escapeSingleQuotes(JSON.stringify(value));
    return `jsonb_set(${qualifiedColumn}, '${pathStr}', '${jsonValueStr}'::jsonb, ${createMissing})`;
  }

  /**
   * Create a JSONB insert expression for inserting a value at a path
   * Usage: .select(jsonbInsert("data", ["items", "0"], "new item"))
   * Returns: jsonb_insert(data, '{items,0}', '"new item"'::jsonb)
   */
  jsonbInsert(
    column: string,
    path: string[],
    value: any,
    insertAfter: boolean = false
  ): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
    const pathStr = `{${escapedPath.join(",")}}`;
    const jsonValueStr = this.escapeSingleQuotes(JSON.stringify(value));
    return `jsonb_insert(${qualifiedColumn}, '${pathStr}', '${jsonValueStr}'::jsonb, ${insertAfter})`;
  }

  /**
   * Create a JSONB delete path expression for removing a value at a path
   * Usage: .select(jsonbDeletePath("data", ["address", "city"]))
   * Returns: data #- '{address,city}'
   */
  jsonbDeletePath(column: string, path: string[]): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
    const pathStr = `{${escapedPath.join(",")}}`;
    return `${qualifiedColumn} #- '${pathStr}'`;
  }

  /**
   * Create a JSONB delete key expression for removing a top-level key
   * Usage: .select(jsonbDeleteKey("data", "oldField"))
   * Returns: data - 'oldField'
   */
  jsonbDeleteKey(column: string, key: string): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    const validatedKey = this.validateJsonIdentifier(key, "JSON key");
    return `${qualifiedColumn} - '${this.escapeSingleQuotes(validatedKey)}'`;
  }

  /**
   * Create a JSONB concatenation expression
   * Usage: .select(jsonbConcat("data", { newField: "value" }))
   * Returns: data || '{"newField":"value"}'::jsonb
   */
  jsonbConcat(column: string, value: Record<string, any>): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    return `${qualifiedColumn} || '${this.escapeSingleQuotes(JSON.stringify(value))}'::jsonb`;
  }

  /**
   * Extract JSON object keys as text array
   * Usage: .select(jsonbObjectKeys("data"))
   * Returns: jsonb_object_keys(data)
   */
  jsonbObjectKeys(column: string): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    return `jsonb_object_keys(${qualifiedColumn})`;
  }

  /**
   * Get the type of a JSON value
   * Usage: .select(jsonbTypeof("data", ["field"]))
   * Returns: jsonb_typeof(data#>'{field}')
   */
  jsonbTypeof(column: string, path?: string[]): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    if (path && path.length > 0) {
      const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
      const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
      const pathStr = `{${escapedPath.join(",")}}`;
      return `jsonb_typeof(${qualifiedColumn}#>'${pathStr}')`;
    }
    return `jsonb_typeof(${qualifiedColumn})`;
  }

  /**
   * Get JSONB array length
   * Usage: .select(jsonbArrayLength("tags"))
   * Returns: jsonb_array_length(tags)
   */
  jsonbArrayLength(column: string, path?: string[]): string {
    const qualifiedColumn = this.qualifyColumnName(column);
    if (path && path.length > 0) {
      const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
      const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
      const pathStr = `{${escapedPath.join(",")}}`;
      return `jsonb_array_length(${qualifiedColumn}#>'${pathStr}')`;
    }
    return `jsonb_array_length(${qualifiedColumn})`;
  }

  /**
   * Build a JSONB object from key-value pairs
   * Usage: .select(jsonbBuildObject({ name: "John", age: 30 }))
   * Returns: jsonb_build_object('name', 'John', 'age', 30)
   */
  jsonbBuildObject(obj: Record<string, any>): string {
    const pairs = Object.entries(obj).flatMap(([key, value]) => {
      const validatedKey = this.validateJsonIdentifier(key, "JSON object key");
      let sqlValue: string;
      if (typeof value === "string") {
        sqlValue = `'${this.escapeSingleQuotes(value)}'`;
      } else if (value !== null && typeof value === "object") {
        sqlValue = `'${this.escapeSingleQuotes(JSON.stringify(value))}'::jsonb`;
      } else {
        sqlValue = String(value);
      }
      return [`'${this.escapeSingleQuotes(validatedKey)}'`, sqlValue];
    });
    return `jsonb_build_object(${pairs.join(", ")})`;
  }

  /**
   * Build a JSONB array from values
   * Usage: .select(jsonbBuildArray([1, 2, 3]))
   * Returns: jsonb_build_array(1, 2, 3)
   */
  jsonbBuildArray(arr: any[]): string {
    const values = arr.map((v) => {
      if (typeof v === "string") return `'${this.escapeSingleQuotes(v)}'`;
      if (v !== null && typeof v === "object")
        return `'${this.escapeSingleQuotes(JSON.stringify(v))}'::jsonb`;
      return String(v);
    });
    return `jsonb_build_array(${values.join(", ")})`;
  }

  /**
   * Add HAVING clause for aggregate conditions
   */
  having(
    column: string,
    operator:
      | "="
      | "!="
      | ">"
      | "<"
      | ">="
      | "<="
      | "LIKE"
      | "ILIKE"
      | "NOT ILIKE"
      | "IN",
    value: any
  ): this {
    if (!this.groupByColumns.length) {
      throw new Error("HAVING clause requires GROUP BY");
    }

    if (
      !this.isAggregateFunction(column) &&
      !this.groupByColumns.includes(this.qualifyColumnName(column))
    ) {
      throw new Error(
        `HAVING clause must reference either an aggregate function or a GROUP BY column: ${column}`
      );
    }

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
   * Validate SQL expression to prevent injection attacks
   * Used for aggregate expressions and window functions where complex SQL is expected
   *
   * @throws {DatabaseError} if expression contains dangerous SQL patterns
   */
  private validateExpression(expr: string, context: string): void {
    // Check for statement separators and comments
    if (/;/.test(expr)) {
      throw new DatabaseError(
        `${context} cannot contain semicolons (statement separators)`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }

    if (/--/.test(expr)) {
      throw new DatabaseError(
        `${context} cannot contain SQL comments (--)`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }

    if (/\/\*|\*\//.test(expr)) {
      throw new DatabaseError(
        `${context} cannot contain multi-line SQL comments (/* */)`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }

    // Check for dangerous SQL keywords that suggest DDL or DML operations
    const dangerousKeywords = /\b(DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|DELETE|INSERT|UPDATE)\b/i;
    if (dangerousKeywords.test(expr)) {
      throw new DatabaseError(
        `${context} cannot contain DDL or DML keywords (DROP, CREATE, ALTER, DELETE, INSERT, UPDATE, etc.)`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }

    // Check for UNION which could be used for SQL injection
    if (/\bUNION\b/i.test(expr)) {
      throw new DatabaseError(
        `${context} cannot contain UNION statements. Use the raw() method for complex queries.`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }

    // Check for backslash escapes (except \" which is valid in JSON)
    if (/\\(?!")/.test(expr)) {
      throw new DatabaseError(
        `${context} contains invalid escape sequences`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }

    // Check for suspicious quote patterns that suggest SQL injection attempts
    // e.g., "' OR '1'='1", "x'; DROP", "'' OR", "'1' AND '1'='1'", etc.
    if (/''\s*(?:OR|AND)\s|'\s*(?:OR|AND)\s*'[^']*'=|';|"[^"]*;/i.test(expr)) {
      throw new DatabaseError(
        `${context} contains suspicious quote patterns`,
        'INVALID_EXPRESSION',
        { query: expr, params: [] }
      );
    }
  }

  /**
   * Sanitize SQL identifier to prevent injection
   * Uses the imported sanitizeSqlIdentifier function from utils
   */
  private sanitizeIdentifier(
    identifier: string,
    allowComplexExpressions: boolean = false
  ): string {
    // Allow complex expressions only if they do NOT contain common injection tokens.
    if (allowComplexExpressions && !/^[a-zA-Z_][a-zA-Z0-9_.]*$/.test(identifier)) {
      // Check if this is a JSONB function call (these are safe constructions from our library)
      const isJsonbFunction = /^(jsonb_set|jsonb_insert|jsonb_concat|jsonb_build_object|jsonb_build_array|jsonb_delete_path|jsonb_object_keys|jsonb_typeof|jsonb_array_length)\(/.test(identifier);
      const isConcatOperation = /^\w+\s*\|\|\s*'/.test(identifier); // e.g., column || '...'

      // JSONB functions are safe - they're constructed by our library with proper escaping
      if (isJsonbFunction || isConcatOperation) {
        return identifier;
      }

      // For other complex expressions, apply security checks
      // Block semicolons (statement separator) and comment markers
      // Note: We allow single quotes in complex expressions because they're used legitimately
      // in JSON operators (e.g., data->'field') and SQL constructs (e.g., INTERVAL '7 days')
      // We also allow \" (escaped double quote) for JSON values, but block other backslashes
      if (/;|--|\/\*|\*\//.test(identifier)) {
        throw new Error(`Invalid SQL identifier: ${identifier}`);
      }
      // Block backslashes except when used as \" (JSON escape)
      if (/\\(?!")/.test(identifier)) {
        throw new Error(`Invalid SQL identifier: ${identifier}`);
      }
      // Block quotes that appear in obviously malicious patterns
      // e.g., "' OR '1'='1", "x'; DROP", etc.
      if (/'[^']*(?:OR|AND|DROP|DELETE|INSERT|UPDATE|UNION|SELECT)[^']*'|';|"[^"]*;/i.test(identifier)) {
        throw new Error(`Invalid SQL identifier: ${identifier}`);
      }
      return identifier;
    }

    // Allow subqueries (starting with parenthesis) if they pass basic token checks
    if (identifier.trim().startsWith("(")) {
      // For subqueries, we're more restrictive because they should be carefully constructed
      // Block semicolons, comment markers, and backslashes
      if (/;|--|\/\*|\*\/|\\/.test(identifier)) {
        throw new Error(`Invalid SQL subquery: ${identifier}`);
      }
      return identifier;
    }

    // Delegate to the utility function for standard identifier sanitization
    return sanitizeSqlIdentifier(identifier);
  }

  /**
   * Qualify column name if needed
   */
  private qualifyColumnName(column: string): string {
    // Check if this is a complex expression (like JSON operators, aggregate functions, or JSONB operations)
    // Includes: ->, ->>, #>, #>>, @>, <@, ?, ?|, ?&, @?, @@, #-, - (subtract), || (concat), jsonb_ functions
    const isComplexExpression =
      /[@?#()\[\]>]|->|#-|\s-\s|\|\||jsonb_|::jsonb/.test(column);

    if (isComplexExpression) {
      // For complex expressions, return as-is (already built by helper methods)
      return this.sanitizeIdentifier(column, true);
    }

    // If column contains a dot, check if it looks like a qualified name (table.column)
    // Qualified names have the pattern: valid_identifier.valid_identifier
    if (column.includes(".")) {
      const qualifiedPattern =
        /^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$/;
      if (qualifiedPattern.test(column)) {
        // Looks like table.column - sanitize as qualified name
        return this.sanitizeIdentifier(column);
      } else {
        // Has dot but doesn't look like qualified name
        // Still need to check for SQL injection before quoting
        if (/[;'"\\]|--|\*\/|\/\*/.test(column)) {
          throw new Error(`Invalid SQL identifier: ${column}`);
        }
        // Treat as single identifier that needs quoting
        return `"${column.replace(/"/g, '""')}"`;
      }
    }

    // Only qualify columns when we have JOINs (to avoid ambiguity)
    // For single table queries, use unqualified names for backward compatibility
    if (this.joins.length === 0) {
      return this.sanitizeIdentifier(column);
    }

    // For queries with JOINs, qualify with main table reference to avoid ambiguity
    return `${this.sanitizeIdentifier(
      this.getTableReference()
    )}.${this.sanitizeIdentifier(column)}`;
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
    let fromClause = `FROM ${this.sanitizeIdentifier(String(this.tableName))}`;
    if (this.tableAlias) {
      fromClause += ` AS ${this.sanitizeIdentifier(this.tableAlias)}`;
    }

    for (const join of this.joins) {
      const joinTableRef = join.alias
        ? `${this.sanitizeIdentifier(join.table)} AS ${this.sanitizeIdentifier(
            join.alias
          )}`
        : this.sanitizeIdentifier(join.table);

      const joinConditions = join.conditions
        .map(
          (cond) =>
            `${this.sanitizeIdentifier(
              cond.leftColumn
            )} = ${this.sanitizeIdentifier(cond.rightColumn)}`
        )
        .join(" AND ");

      fromClause += ` ${join.type} JOIN ${joinTableRef} ON ${joinConditions}`;
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

    let columns = this.selectedColumns.length
      ? this.selectedColumns.join(", ")
      : "*";

    if (this.windowFunctions.length > 0) {
      columns += `, ${this.windowFunctions
        .map((w, i) => `${w} as window_${i + 1}`)
        .join(", ")}`;
    }

    const query = `SELECT ${
      this.isDistinct ? "DISTINCT " : ""
    }${columns} ${this.buildFromClause()}${this.whereClause}${
      this.groupByColumns.length
        ? ` GROUP BY ${this.groupByColumns.join(", ")}`
        : ""
    }${this.havingClause}${this.orderByClause}${this.limitClause}${
      this.offsetClause
    }`;

    const params = [...this.whereParams, ...this.havingParams];
    const startTime = Date.now();
    const timestamp = new Date();

    try {
      // Use queryExecutor if available (provides timeout, retry, security validation)
      // Otherwise fall back to direct pool.query
      const result = this.queryExecutor
        ? await this.queryExecutor<Row>(query, params, "select")
        : await this.pool.query<Row>(query, params);

      const duration = Date.now() - startTime;

      // Only log if using direct pool.query (queryExecutor logs internally)
      if (!this.queryExecutor && this.logger) {
        this.logger.log("debug", {
          query,
          params,
          duration,
          timestamp,
        });
      }

      return result.rows;
    } catch (error) {
      const duration = Date.now() - startTime;

      // Only log if using direct pool.query (queryExecutor logs internally)
      if (!this.queryExecutor && this.logger) {
        this.logger.log("error", {
          query,
          params,
          duration,
          timestamp,
          error: error as Error,
        });
      }

      throw error;
    }
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
    const params = this.whereParams;
    const startTime = Date.now();
    const timestamp = new Date();

    try {
      // Use queryExecutor if available (provides timeout, retry, security validation)
      const result = this.queryExecutor
        ? await this.queryExecutor<{ count: string }>(query, params, "count")
        : await this.pool.query<{ count: string }>(query, params);

      const duration = Date.now() - startTime;

      // Only log if using direct pool.query (queryExecutor logs internally)
      if (!this.queryExecutor && this.logger) {
        this.logger.log("debug", {
          query,
          params,
          duration,
          timestamp,
        });
      }

      return parseInt(result.rows[0].count, 10);
    } catch (error) {
      const duration = Date.now() - startTime;

      // Only log if using direct pool.query (queryExecutor logs internally)
      if (!this.queryExecutor && this.logger) {
        this.logger.log("error", {
          query,
          params,
          duration,
          timestamp,
          error: error as Error,
        });
      }

      throw error;
    }
  }

  /**
   * Get minimum value for a column
   */
  min<K extends ColumnNames<Row>>(
    column: K
  ): TypedQuery<TableName, { min: Row[K] }, Schema>;
  min(column: string): TypedQuery<TableName, { min: any }, Schema>;
  min(column: any): TypedQuery<TableName, { min: any }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { min: any }, Schema>).select(
      {
        column: `MIN(${qualifiedColumn})`,
        as: "min",
        __isExpression: true,
      } as const
    );
  }

  /**
   * Get maximum value for a column
   */
  max<K extends ColumnNames<Row>>(
    column: K
  ): TypedQuery<TableName, { max: Row[K] }, Schema>;
  max(column: string): TypedQuery<TableName, { max: any }, Schema>;
  max(column: any): TypedQuery<TableName, { max: any }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { max: any }, Schema>).select(
      {
        column: `MAX(${qualifiedColumn})`,
        as: "max",
        __isExpression: true,
      } as const
    );
  }

  /**
   * Get sum of values for a column
   */
  sum<K extends ColumnNames<Row>>(
    column: K
  ): TypedQuery<TableName, { sum: Row[K] }, Schema>;
  sum(column: string): TypedQuery<TableName, { sum: any }, Schema>;
  sum(column: any): TypedQuery<TableName, { sum: any }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { sum: any }, Schema>).select(
      {
        column: `SUM(${qualifiedColumn})`,
        as: "sum",
        __isExpression: true,
      } as const
    );
  }

  /**
   * Get average value for a column
   */
  avg<K extends ColumnNames<Row>>(
    column: K
  ): TypedQuery<TableName, { avg: number }, Schema>;
  avg(column: string): TypedQuery<TableName, { avg: number }, Schema>;
  avg(column: any): TypedQuery<TableName, { avg: number }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (
      this.clone() as TypedQuery<TableName, { avg: number }, Schema>
    ).select({
      column: `AVG(${qualifiedColumn})`,
      as: "avg",
      __isExpression: true,
    } as const);
  }

  /**
   * Aggregate multiple columns with custom aliases
   *
   * @example
   * .aggregate({
   *   total: "COUNT(*)",
   *   avg_age: "AVG(age)",
   *   max_salary: "MAX(salary)"
   * })
   *
   * Note: Expressions are validated for SQL injection. For complex queries
   * that require raw SQL, use the raw() method instead.
   */
  aggregate<T extends Record<string, any>>(aggregations: {
    [K in keyof T]: string;
  }): TypedQuery<TableName, T, Schema> {
    const newQuery = this.clone<T>();
    newQuery.selectedColumns = Object.entries(aggregations).map(
      ([alias, expr]) => {
        // Validate the expression for SQL injection
        this.validateExpression(String(expr), 'Aggregate expression');
        return `${expr} AS ${this.sanitizeIdentifier(alias)}`;
      }
    );
    return newQuery;
  }

  /**
   * Add ROW_NUMBER() window function
   */
  rowNumber(
    partitionBy?: string[],
    orderBy?: [string, "ASC" | "DESC"][]
  ): this {
    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumnName(c)).join(", ")}`
      : "";
    const order = orderBy?.length
      ? `ORDER BY ${orderBy.map(([col, dir]) => `${this.qualifyColumnName(col)} ${dir}`).join(", ")}`
      : "";
    this.windowFunctions.push(`ROW_NUMBER() OVER (${partition} ${order})`);
    return this;
  }

  /**
   * Add RANK() window function
   */
  rank(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): this {
    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumnName(c)).join(", ")}`
      : "";
    const order = orderBy?.length
      ? `ORDER BY ${orderBy.map(([col, dir]) => `${this.qualifyColumnName(col)} ${dir}`).join(", ")}`
      : "";
    this.windowFunctions.push(`RANK() OVER (${partition} ${order})`);
    return this;
  }

  /**
   * Add DENSE_RANK() window function
   */
  denseRank(
    partitionBy?: string[],
    orderBy?: [string, "ASC" | "DESC"][]
  ): this {
    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumnName(c)).join(", ")}`
      : "";
    const order = orderBy?.length
      ? `ORDER BY ${orderBy.map(([col, dir]) => `${this.qualifyColumnName(col)} ${dir}`).join(", ")}`
      : "";
    this.windowFunctions.push(`DENSE_RANK() OVER (${partition} ${order})`);
    return this;
  }

  /**
   * Add LAG() window function
   */
  lag(
    column: string,
    offset: number = 1,
    defaultValue?: any,
    partitionBy?: string[]
  ): this {
    // Validate defaultValue to prevent SQL injection
    if (defaultValue !== undefined) {
      if (typeof defaultValue === 'string') {
        throw new DatabaseError(
          'LAG defaultValue cannot be a string. Use NULL, numbers, or booleans only.',
          'INVALID_WINDOW_FUNCTION'
        );
      }
      if (typeof defaultValue !== 'number' && typeof defaultValue !== 'boolean' && defaultValue !== null) {
        throw new DatabaseError(
          'LAG defaultValue must be a number, boolean, or null',
          'INVALID_WINDOW_FUNCTION'
        );
      }
      if (typeof defaultValue === 'number' && !Number.isFinite(defaultValue)) {
        throw new DatabaseError('LAG defaultValue must be a finite number', 'INVALID_WINDOW_FUNCTION');
      }
    }

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumnName(c)).join(", ")}`
      : "";
    const def = defaultValue !== undefined ? `, ${defaultValue}` : "";
    this.windowFunctions.push(
      `LAG(${this.qualifyColumnName(column)}, ${offset}${def}) OVER (${partition})`
    );
    return this;
  }

  /**
   * Add LEAD() window function
   */
  lead(
    column: string,
    offset: number = 1,
    defaultValue?: any,
    partitionBy?: string[]
  ): this {
    // Validate defaultValue to prevent SQL injection
    if (defaultValue !== undefined) {
      if (typeof defaultValue === 'string') {
        throw new DatabaseError(
          'LEAD defaultValue cannot be a string. Use NULL, numbers, or booleans only.',
          'INVALID_WINDOW_FUNCTION'
        );
      }
      if (typeof defaultValue !== 'number' && typeof defaultValue !== 'boolean' && defaultValue !== null) {
        throw new DatabaseError(
          'LEAD defaultValue must be a number, boolean, or null',
          'INVALID_WINDOW_FUNCTION'
        );
      }
      if (typeof defaultValue === 'number' && !Number.isFinite(defaultValue)) {
        throw new DatabaseError('LEAD defaultValue must be a finite number', 'INVALID_WINDOW_FUNCTION');
      }
    }

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumnName(c)).join(", ")}`
      : "";
    const def = defaultValue !== undefined ? `, ${defaultValue}` : "";
    this.windowFunctions.push(
      `LEAD(${this.qualifyColumnName(column)}, ${offset}${def}) OVER (${partition})`
    );
    return this;
  }

  /**
   * Add any window function with custom OVER clause
   *
   * @example
   * .window("FIRST_VALUE(salary)", "PARTITION BY department ORDER BY salary DESC")
   * .window("PERCENT_RANK()", "ORDER BY score")
   *
   * Note: Both function and OVER clause are validated for SQL injection.
   * For complex queries that require raw SQL, use the raw() method instead.
   */
  window(function_: string, over: string): this {
    // Validate both the function expression and OVER clause
    this.validateExpression(function_, 'Window function');
    this.validateExpression(over, 'Window OVER clause');
    this.windowFunctions.push(`${function_} OVER (${over})`);
    return this;
  }

  /**
   * Get the SQL query string (for debugging)
   */
  toSQL(): { query: string; params: any[] } {
    const columns = this.selectedColumns.length
      ? this.selectedColumns.join(", ")
      : "*";

    const query = `SELECT ${
      this.isDistinct ? "DISTINCT " : ""
    }${columns} ${this.buildFromClause()}${this.whereClause}${
      this.groupByColumns.length
        ? ` GROUP BY ${this.groupByColumns.join(", ")}`
        : ""
    }${this.havingClause}${this.orderByClause}${this.limitClause}${
      this.offsetClause
    }`;

    return { query, params: [...this.whereParams, ...this.havingParams] };
  }
}
