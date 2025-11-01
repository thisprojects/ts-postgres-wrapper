/**
 * Window Function builder for TypedQuery
 * Handles ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, and custom window functions
 */

import { DatabaseError } from "../errors";

export class WindowFunctionBuilder {
  private windowFunctions: string[] = [];

  constructor(
    private qualifyColumn: (column: string) => string,
    private validateExpression: (expr: string, context: string) => void
  ) {}

  /**
   * Validate array lengths for window functions to prevent DoS
   */
  private validateArrayLengths(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): void {
    if (partitionBy && partitionBy.length > 50) {
      throw new DatabaseError(
        'PARTITION BY column count exceeds maximum of 50',
        'TOO_MANY_PARTITION_COLUMNS',
        { query: '', params: [], detail: `count: ${partitionBy.length}` }
      );
    }
    if (orderBy && orderBy.length > 50) {
      throw new DatabaseError(
        'ORDER BY column count in window function exceeds maximum of 50',
        'TOO_MANY_ORDER_COLUMNS',
        { query: '', params: [], detail: `count: ${orderBy.length}` }
      );
    }
  }

  /**
   * Validate offset parameter for LAG/LEAD functions
   */
  private validateOffset(offset: number, functionName: string): void {
    if (!Number.isInteger(offset) || offset < 0) {
      throw new DatabaseError(
        `${functionName} offset must be a non-negative integer`,
        'INVALID_WINDOW_FUNCTION',
        { query: '', params: [], detail: `offset: ${offset}` }
      );
    }
  }

  /**
   * Validate and format defaultValue parameter for LAG/LEAD functions
   * Returns the formatted SQL fragment or empty string
   */
  private validateAndFormatDefaultValue(defaultValue: any, functionName: string): string {
    if (defaultValue === undefined) {
      return "";
    }

    // Only allow safe primitive types
    if (typeof defaultValue === 'number') {
      if (!Number.isFinite(defaultValue)) {
        throw new DatabaseError(
          `${functionName} defaultValue must be a finite number`,
          'INVALID_WINDOW_FUNCTION',
          { query: '', params: [], detail: `defaultValue: ${defaultValue}` }
        );
      }
      return `, ${defaultValue}`;
    }

    if (typeof defaultValue === 'boolean') {
      return `, ${defaultValue}`;
    }

    if (defaultValue === null) {
      return `, NULL`;
    }

    if (typeof defaultValue === 'string') {
      // String values in window functions should use parameterized queries
      // Since we can't use parameters in window function default values,
      // we must reject string values to prevent SQL injection
      throw new DatabaseError(
        `${functionName} defaultValue cannot be a string. Use NULL, numbers, or booleans only.`,
        'INVALID_WINDOW_FUNCTION',
        { query: '', params: [], detail: 'String default values are not supported for security reasons' }
      );
    }

    throw new DatabaseError(
      `${functionName} defaultValue must be a number, boolean, or null`,
      'INVALID_WINDOW_FUNCTION',
      { query: '', params: [], detail: `defaultValue type: ${typeof defaultValue}` }
    );
  }

  /**
   * Get all window function expressions
   */
  getFunctions(): string[] {
    return this.windowFunctions;
  }

  /**
   * Check if any window functions have been added
   */
  hasFunctions(): boolean {
    return this.windowFunctions.length > 0;
  }

  /**
   * Build window function column aliases
   */
  buildWindowColumns(): string {
    if (this.windowFunctions.length === 0) {
      return "";
    }

    return this.windowFunctions
      .map((w, i) => `${w} as window_${i + 1}`)
      .join(", ");
  }

  /**
   * Add ROW_NUMBER() window function
   */
  addRowNumber(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): void {
    this.validateArrayLengths(partitionBy, orderBy);

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";
    const order = orderBy?.length
      ? `ORDER BY ${orderBy.map(([col, dir]) => `${this.qualifyColumn(col)} ${dir}`).join(", ")}`
      : "";
    this.windowFunctions.push(`ROW_NUMBER() OVER (${partition} ${order})`.trim());
  }

  /**
   * Add RANK() window function
   */
  addRank(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): void {
    this.validateArrayLengths(partitionBy, orderBy);

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";
    const order = orderBy?.length
      ? `ORDER BY ${orderBy.map(([col, dir]) => `${this.qualifyColumn(col)} ${dir}`).join(", ")}`
      : "";
    this.windowFunctions.push(`RANK() OVER (${partition} ${order})`.trim());
  }

  /**
   * Add DENSE_RANK() window function
   */
  addDenseRank(partitionBy?: string[], orderBy?: [string, "ASC" | "DESC"][]): void {
    this.validateArrayLengths(partitionBy, orderBy);

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";
    const order = orderBy?.length
      ? `ORDER BY ${orderBy.map(([col, dir]) => `${this.qualifyColumn(col)} ${dir}`).join(", ")}`
      : "";
    this.windowFunctions.push(`DENSE_RANK() OVER (${partition} ${order})`.trim());
  }

  /**
   * Add LAG() window function
   */
  addLag(
    column: string,
    offset: number = 1,
    defaultValue?: any,
    partitionBy?: string[]
  ): void {
    this.validateOffset(offset, 'LAG');

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";

    const def = this.validateAndFormatDefaultValue(defaultValue, 'LAG');

    this.windowFunctions.push(
      `LAG(${this.qualifyColumn(column)}, ${offset}${def}) OVER (${partition})`.trim()
    );
  }

  /**
   * Add LEAD() window function
   */
  addLead(
    column: string,
    offset: number = 1,
    defaultValue?: any,
    partitionBy?: string[]
  ): void {
    this.validateOffset(offset, 'LEAD');

    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";

    const def = this.validateAndFormatDefaultValue(defaultValue, 'LEAD');

    this.windowFunctions.push(
      `LEAD(${this.qualifyColumn(column)}, ${offset}${def}) OVER (${partition})`.trim()
    );
  }

  /**
   * Add custom window function with validation
   */
  addCustomWindow(functionExpr: string, overClause: string): void {
    // Validate both the function expression and OVER clause for SQL injection
    this.validateExpression(functionExpr, 'Window function');
    this.validateExpression(overClause, 'Window OVER clause');
    this.windowFunctions.push(`${functionExpr} OVER (${overClause})`);
  }

  /**
   * Clone the window function builder state
   */
  clone(): WindowFunctionBuilder {
    const cloned = new WindowFunctionBuilder(this.qualifyColumn, this.validateExpression);
    cloned.windowFunctions = [...this.windowFunctions];
    return cloned;
  }
}
