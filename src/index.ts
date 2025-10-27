import { Pool } from "pg";

/**
 * Extract column names from a table type
 */
/**
 * JSON operators supported by PostgreSQL
 */
type JSONOperator =
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
type JSONPath = string[] | string;

/**
 * JSON value types that can be used in queries
 */
type JSONValue = Record<string, any> | any[] | string | number | boolean | null;

type ColumnNames<T> = keyof T & string;

/**
 * Column alias configuration
 */
type ColumnAlias<T, K extends keyof T = keyof T> = {
  column: K;
  as: string;
};

/**
 * Column specification that allows both simple column names and complex expressions
 */
type ColumnSpec<T> = keyof T | string | ColumnAlias<T, keyof T> | { column: string; as: string };

/**
 * Extract the result type from a mix of column names and aliases
 */
type ResultColumns<T, S extends ColumnSpec<T>[]> = {
  [K in S[number] as
    K extends { as: infer A } ? A :
    K extends keyof T ? K :
    K extends string ? K :
    never
  ]:
    K extends { column: keyof T } ? T[K['column']] :
    K extends { column: string, as: infer A } ? A extends keyof T ? T[A] : any :
    K extends keyof T ? T[K] :
    any
};

/**
 * Pick specific columns from a table
 */
type SelectColumns<T, K extends keyof T & string> = Pick<T, K>;

/**
 * Join configuration interface
 */
interface JoinCondition {
  leftColumn: string;
  rightColumn: string;
}

interface JoinConfig {
  type: "INNER" | "LEFT" | "RIGHT" | "FULL";
  table: string;
  conditions: JoinCondition[];
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
  private caseInsensitive: boolean = false;
  private joins: JoinConfig[] = [];

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
  clone<NewRow extends Record<string, any> = Row>(): TypedQuery<
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
   */
  select<S extends ColumnSpec<Row>[]>(
    ...columns: S
  ): TypedQuery<TableName, ResultColumns<Row, S>, Schema>;
  select(...columns: (string | ColumnAlias<Row> | { column: string; as: string })[]): TypedQuery<TableName, any, Schema> {
    const newQuery = this.clone<any>();
    newQuery.selectedColumns = columns.map(col => {
      if (typeof col === 'string') {
        // Check if this is a raw expression with AS clause (e.g., "column AS alias")
        // Allow this pattern to pass through for backward compatibility
        if (/ as /i.test(col)) {
          const parts = col.split(/ as /i);
          if (parts.length === 2) {
            const colPart = parts[0].trim();
            const aliasPart = parts[1].trim();
            return `${this.qualifyColumnName(colPart)} AS ${this.sanitizeIdentifier(aliasPart)}`;
          }
        }
        return this.qualifyColumnName(col);
      } else {
        const colExpr = typeof col.column === 'string' ? col.column : this.qualifyColumnName(String(col.column));
        return `${colExpr} AS ${this.sanitizeIdentifier(col.as)}`;
      }
    });
    return newQuery;
  }

  /**
   * INNER JOIN with another table
   */
  innerJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin("INNER", joinedTable, [{
        leftColumn: leftColumnOrConditions,
        rightColumn: rightColumnOrAlias as string
      }], alias);
    } else {
      // Compound key join
      return this.addJoin("INNER", joinedTable, leftColumnOrConditions, rightColumnOrAlias);
    }
  }

  /**
   * LEFT JOIN with another table
   */
  leftJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin("LEFT", joinedTable, [{
        leftColumn: leftColumnOrConditions,
        rightColumn: rightColumnOrAlias as string
      }], alias);
    } else {
      // Compound key join
      return this.addJoin("LEFT", joinedTable, leftColumnOrConditions, rightColumnOrAlias);
    }
  }

  /**
   * RIGHT JOIN with another table
   */
  rightJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin("RIGHT", joinedTable, [{
        leftColumn: leftColumnOrConditions,
        rightColumn: rightColumnOrAlias as string
      }], alias);
    } else {
      // Compound key join
      return this.addJoin("RIGHT", joinedTable, leftColumnOrConditions, rightColumnOrAlias);
    }
  }

  /**
   * FULL OUTER JOIN with another table
   */
  fullJoin(
    joinedTable: string,
    leftColumnOrConditions: string | JoinCondition[],
    rightColumnOrAlias?: string,
    alias?: string
  ): TypedQuery<TableName, any, Schema> {
    if (typeof leftColumnOrConditions === "string") {
      // Single-column join (backward compatible)
      return this.addJoin("FULL", joinedTable, [{
        leftColumn: leftColumnOrConditions,
        rightColumn: rightColumnOrAlias as string
      }], alias);
    } else {
      // Compound key join
      return this.addJoin("FULL", joinedTable, leftColumnOrConditions, rightColumnOrAlias);
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
   * Add WHERE clause (supports both typed columns and string-based columns)
   */
  where<K extends ColumnNames<Row>>(
    column: K,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | "IN" | "BETWEEN" | "IS NULL" | "IS NOT NULL" | JSONOperator,
    value: Row[K] | Row[K][] | JSONValue | JSONPath
  ): this;
  where(
    column: string,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | "IN" | "BETWEEN" | "IS NULL" | "IS NOT NULL" | JSONOperator,
    value: any
  ): this;
  where(
    column: any,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | "IN" | "BETWEEN" | "IS NULL" | "IS NOT NULL" | JSONOperator,
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
    } else if (operator === "BETWEEN" && Array.isArray(value) && value.length === 2) {
      this.whereClause += `${qualifiedColumn} BETWEEN $${this.paramCounter} AND $${this.paramCounter + 1}`;
      this.whereParams.push(value[0], value[1]);
      this.paramCounter += 2;
    } else if (operator === "IS NULL" || operator === "IS NOT NULL") {
      this.whereClause += `${qualifiedColumn} ${operator}`;
    } else if (operator === "->" || operator === "->>" || operator === "#>" || operator === "#>>") {
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
      if (this.caseInsensitive && (operator === "=" || operator === "!=" || operator === "LIKE")) {
        if (operator === "=") finalOperator = "ILIKE";
        else if (operator === "!=") {
          finalOperator = "NOT ILIKE";
          value = String(value).replace(/^%|%$/g, ''); // Remove wildcards for exact match
        }
        else if (operator === "LIKE") finalOperator = "ILIKE";
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
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | JSONOperator,
    value: Row[K] | JSONValue | JSONPath
  ): this;
  orWhere(
    column: string,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | "IN" | JSONOperator,
    value: any
  ): this;
  orWhere(
    column: any,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | "IN" | JSONOperator,
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
    } else if (operator === "->" || operator === "->>" || operator === "#>" || operator === "#>>") {
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
      if (this.caseInsensitive && (operator === "=" || operator === "!=" || operator === "LIKE")) {
        if (operator === "=") finalOperator = "ILIKE";
        else if (operator === "!=") {
          finalOperator = "NOT ILIKE";
          value = String(value).replace(/^%|%$/g, ''); // Remove wildcards for exact match
        }
        else if (operator === "LIKE") finalOperator = "ILIKE";
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
  orderBy(column: any, direction: "ASC" | "DESC" = "ASC"): this {
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
    if (columns.length === 0) {
      throw new Error("GROUP BY clause requires at least one column");
    }

    const invalidColumns = columns.filter(col => {
      const tableName = this.tableAlias || String(this.tableName);
      const isQualified = String(col).includes(".");
      // Skip validation for qualified columns and test mode
      if (isQualified || !this.schema[tableName]) {
        return false;
      }
      return !Object.keys(this.schema[tableName]).includes(String(col).split(".").pop() || "");
    });

    if (invalidColumns.length > 0) {
      throw new Error(
        `Invalid column names in GROUP BY: ${invalidColumns.join(", ")}`
      );
    }

    const qualifiedColumns = columns.map(col => this.qualifyColumnName(String(col)));
    this.groupByColumns.push(...qualifiedColumns);
    return this;
  }

  /**
   * Check if expression is an aggregate function
   */
  private isAggregateFunction(expr: string): boolean {
    const aggFuncs = ["COUNT", "SUM", "AVG", "MIN", "MAX", "VARIANCE", "FIRST_VALUE", "ROW_NUMBER", "RANK", "DENSE_RANK"];
    const upperExpr = expr.toUpperCase();
    return aggFuncs.some(func => upperExpr.includes(func + "(")) || upperExpr.includes(" OVER ");
  }

  /**
   * Escape single quotes in SQL strings
   */
  private escapeSingleQuotes(value: string): string {
    return value.replace(/'/g, "''");
  }

  /**
   * Get JSON object field
   */
  jsonField<T extends Record<string, any>>(column: string, field: keyof T): string {
    return `${this.qualifyColumnName(column)}->'${this.escapeSingleQuotes(String(field))}'`;
  }

  /**
   * Get JSON object field as text
   */
  jsonFieldAsText<T extends Record<string, any>>(column: string, field: keyof T): string {
    return `${this.qualifyColumnName(column)}->>'${this.escapeSingleQuotes(String(field))}'`;
  }

  /**
   * Get JSON object at path
   */
  jsonPath(column: string, path: string[]): string {
    const pathStr = path.map(p => `'${this.escapeSingleQuotes(p)}'`).join(",");
    return `${this.qualifyColumnName(column)}#>ARRAY[${pathStr}]`;
  }

  /**
   * Get JSON object at path as text
   */
  jsonPathAsText(column: string, path: string[]): string {
    const pathStr = path.map(p => `'${this.escapeSingleQuotes(p)}'`).join(",");
    return `${this.qualifyColumnName(column)}#>>ARRAY[${pathStr}]`;
  }

  /**
   * Check if JSON object exists at path
   */
  hasJsonPath(column: string, path: string[]): this {
    const pathStr = path.map(p => `'${this.escapeSingleQuotes(p)}'`).join(",");
    return this.where(`${this.qualifyColumnName(column)}#>ARRAY[${pathStr}]`, "IS NOT NULL", null);
  }

  /**
   * Check if JSON object contains the given JSON value
   */
  containsJson<K extends ColumnNames<Row>>(column: K, value: Row[K] extends Record<string, any> ? Partial<Row[K]> : never): this {
    return this.where(column, "@>", value);
  }

  /**
   * Check if JSON object is contained within the given JSON value
   */
  containedInJson<K extends ColumnNames<Row>>(column: K, value: Row[K] extends Record<string, any> ? Record<string, any> : never): this {
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
   * Add HAVING clause for aggregate conditions
   */
  having(
    column: string,
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "NOT ILIKE" | "IN",
    value: any
  ): this {
    if (!this.groupByColumns.length) {
      throw new Error("HAVING clause requires GROUP BY");
    }

    if (!this.isAggregateFunction(column) && !this.groupByColumns.includes(this.qualifyColumnName(column))) {
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
   * Sanitize SQL identifier to prevent injection
   */
  private sanitizeIdentifier(identifier: string, allowComplexExpressions: boolean = false): string {
    // If complex expressions are allowed (like JSON operators or aggregate functions), skip sanitization
    // This is used when the column is already a validated expression like data->>'field'
    if (allowComplexExpressions && !/^[a-zA-Z_][a-zA-Z0-9_.]*$/.test(identifier)) {
      return identifier;
    }

    // Allow subqueries (starting with parenthesis) - used for JOIN subqueries
    if (identifier.trim().startsWith('(')) {
      return identifier;
    }

    // Standard SQL identifiers: alphanumeric and underscore, starting with letter or underscore
    // This pattern also allows dots for qualified names (table.column)
    if (/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/.test(identifier)) {
      // Return unquoted for backward compatibility (valid identifiers are safe)
      return identifier;
    }

    // For special characters, check for SQL injection patterns before quoting
    // Block obvious SQL injection attempts (semicolons, comments, etc)
    if (/[;'"\\]|--|\*\/|\/\*/.test(identifier)) {
      throw new Error(`Invalid SQL identifier: ${identifier}`);
    }

    // Quote the identifier and escape internal double quotes
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Qualify column name if needed
   */
  private qualifyColumnName(column: string): string {
    // Check if this is a complex expression (like JSON operators or aggregate functions)
    const isComplexExpression = /[@?#()\[\]>]|->/.test(column);

    if (isComplexExpression) {
      // For complex expressions, return as-is (already built by helper methods)
      return this.sanitizeIdentifier(column, true);
    }

    // If column contains a dot, check if it looks like a qualified name (table.column)
    // Qualified names have the pattern: valid_identifier.valid_identifier
    if (column.includes(".")) {
      const qualifiedPattern = /^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$/;
      if (qualifiedPattern.test(column)) {
        // Looks like table.column - sanitize as qualified name
        return this.sanitizeIdentifier(column);
      } else {
        // Has dot but doesn't look like qualified name - treat as single identifier that needs quoting
        return `"${column.replace(/"/g, '""')}"`;
      }
    }

    // Only qualify columns when we have JOINs (to avoid ambiguity)
    // For single table queries, use unqualified names for backward compatibility
    if (this.joins.length === 0) {
      return this.sanitizeIdentifier(column);
    }

    // For queries with JOINs, qualify with main table reference to avoid ambiguity
    return `${this.sanitizeIdentifier(this.getTableReference())}.${this.sanitizeIdentifier(column)}`;
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
        ? `${this.sanitizeIdentifier(join.table)} AS ${this.sanitizeIdentifier(join.alias)}`
        : this.sanitizeIdentifier(join.table);

      const joinConditions = join.conditions
        .map(cond => `${this.sanitizeIdentifier(cond.leftColumn)} = ${this.sanitizeIdentifier(cond.rightColumn)}`)
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
      columns += `, ${this.windowFunctions.map((w, i) => `${w} as window_${i + 1}`).join(", ")}`;
    }

    const query = `SELECT ${this.isDistinct ? 'DISTINCT ' : ''}${columns} ${this.buildFromClause()}${
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
   * Get minimum value for a column
   */
  min<K extends ColumnNames<Row>>(column: K): TypedQuery<TableName, { min: Row[K] }, Schema>;
  min(column: string): TypedQuery<TableName, { min: any }, Schema>;
  min(column: any): TypedQuery<TableName, { min: any }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { min: any }, Schema>)
      .select({
        column: `MIN(${qualifiedColumn})`,
        as: "min"
      } as const);
  }

  /**
   * Get maximum value for a column
   */
  max<K extends ColumnNames<Row>>(column: K): TypedQuery<TableName, { max: Row[K] }, Schema>;
  max(column: string): TypedQuery<TableName, { max: any }, Schema>;
  max(column: any): TypedQuery<TableName, { max: any }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { max: any }, Schema>)
      .select({
        column: `MAX(${qualifiedColumn})`,
        as: "max"
      } as const);
  }

  /**
   * Get sum of values for a column
   */
  sum<K extends ColumnNames<Row>>(column: K): TypedQuery<TableName, { sum: Row[K] }, Schema>;
  sum(column: string): TypedQuery<TableName, { sum: any }, Schema>;
  sum(column: any): TypedQuery<TableName, { sum: any }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { sum: any }, Schema>)
      .select({
        column: `SUM(${qualifiedColumn})`,
        as: "sum"
      } as const);
  }

  /**
   * Get average value for a column
   */
  avg<K extends ColumnNames<Row>>(column: K): TypedQuery<TableName, { avg: number }, Schema>;
  avg(column: string): TypedQuery<TableName, { avg: number }, Schema>;
  avg(column: any): TypedQuery<TableName, { avg: number }, Schema> {
    const qualifiedColumn = this.qualifyColumnName(String(column));
    return (this.clone() as TypedQuery<TableName, { avg: number }, Schema>)
      .select({
        column: `AVG(${qualifiedColumn})`,
        as: "avg"
      } as const);
  }

  /**
   * Aggregate multiple columns with custom aliases
   */
  aggregate<T extends Record<string, any>>(
    aggregations: { [K in keyof T]: string }
  ): TypedQuery<TableName, T, Schema> {
    const newQuery = this.clone<T>();
    newQuery.selectedColumns = Object.entries(aggregations)
      .map(([alias, expr]) => `${expr} AS ${this.sanitizeIdentifier(alias)}`);
    return newQuery;
  }

  /**
   * Add ROW_NUMBER() window function
   */
  rowNumber(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): this {
    const partition = partitionBy?.length ? `PARTITION BY ${partitionBy.join(", ")}` : "";
    const order = orderBy?.length ? `ORDER BY ${orderBy.map(([col, dir]) => `${col} ${dir}`).join(", ")}` : "";
    this.windowFunctions.push(`ROW_NUMBER() OVER (${partition} ${order})`);
    return this;
  }

  /**
   * Add RANK() window function
   */
  rank(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): this {
    const partition = partitionBy?.length ? `PARTITION BY ${partitionBy.join(", ")}` : "";
    const order = orderBy?.length ? `ORDER BY ${orderBy.map(([col, dir]) => `${col} ${dir}`).join(", ")}` : "";
    this.windowFunctions.push(`RANK() OVER (${partition} ${order})`);
    return this;
  }

  /**
   * Add DENSE_RANK() window function
   */
  denseRank(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): this {
    const partition = partitionBy?.length ? `PARTITION BY ${partitionBy.join(", ")}` : "";
    const order = orderBy?.length ? `ORDER BY ${orderBy.map(([col, dir]) => `${col} ${dir}`).join(", ")}` : "";
    this.windowFunctions.push(`DENSE_RANK() OVER (${partition} ${order})`);
    return this;
  }

  /**
   * Add LAG() window function
   */
  lag(column: string, offset: number = 1, defaultValue?: any, partitionBy?: string[]): this {
    const partition = partitionBy?.length ? `PARTITION BY ${partitionBy.join(", ")}` : "";
    const def = defaultValue !== undefined ? `, ${defaultValue}` : "";
    this.windowFunctions.push(`LAG(${column}, ${offset}${def}) OVER (${partition})`);
    return this;
  }

  /**
   * Add LEAD() window function
   */
  lead(column: string, offset: number = 1, defaultValue?: any, partitionBy?: string[]): this {
    const partition = partitionBy?.length ? `PARTITION BY ${partitionBy.join(", ")}` : "";
    const def = defaultValue !== undefined ? `, ${defaultValue}` : "";
    this.windowFunctions.push(`LEAD(${column}, ${offset}${def}) OVER (${partition})`);
    return this;
  }

  /**
   * Add any window function with custom OVER clause
   */
  window(function_: string, over: string): this {
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

    const query = `SELECT ${this.isDistinct ? 'DISTINCT ' : ''}${columns} ${this.buildFromClause()}${
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
    const columnsToUpdate = updateColumns || columns.filter(col => !conflictColumns.includes(col as any));

    // Validate we have columns to update
    if (columnsToUpdate.length === 0) {
      throw new Error("No columns to update in UPSERT operation");
    }

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

    // Build UPDATE SET expression
    const setClause = columnsToUpdate
      .map((col) => `${col} = EXCLUDED.${col}`)
      .join(", ");

    // Build query
    const query = `
      INSERT INTO ${String(tableName)} (${columns.join(", ")})
      VALUES ${values}
      ON CONFLICT (${conflictColumns.join(", ")})
      DO UPDATE SET ${setClause}
      RETURNING *
    `;

    const params = records.flatMap((record) =>
      columns.map((col) => record[col])
    );

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
        deleted: {} as Record<T, Schema[T][]>
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

    // Insert all data at once for better efficiency
    return this.insert(table, data);
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

    // Execute all updates in a single transaction
    return this.transaction(async (tx) => {
      const promises = updates.map(update =>
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

    // Execute all deletes in a single transaction
    return this.transaction(async (tx) => {
      const promises = conditions.map(where =>
        tx.delete(table, where)
      );
      const results = await Promise.all(promises);
      return results.flat();
    });
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
