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
    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";
    const def = defaultValue !== undefined ? `, ${defaultValue}` : "";
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
    const partition = partitionBy?.length
      ? `PARTITION BY ${partitionBy.map(c => this.qualifyColumn(c)).join(", ")}`
      : "";
    const def = defaultValue !== undefined ? `, ${defaultValue}` : "";
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
