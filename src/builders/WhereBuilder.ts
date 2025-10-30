/**
 * WHERE clause builder for TypedQuery
 * Handles WHERE and OR WHERE conditions with type safety
 */

import type { JSONOperator, JSONPath, JSONValue, ColumnNames } from "../types";

export interface WhereCondition {
  clause: string;
  params: any[];
}

export class WhereBuilder<Row extends Record<string, any>> {
  private whereClause: string = "";
  private whereParams: any[] = [];
  private paramCounter: number = 1;
  private caseInsensitive: boolean = false;

  constructor(
    private qualifyColumn: (column: string) => string,
    initialParamCounter: number = 1
  ) {
    this.paramCounter = initialParamCounter;
  }

  /**
   * Get current parameter counter
   */
  getParamCounter(): number {
    return this.paramCounter;
  }

  /**
   * Set parameter counter
   */
  setParamCounter(counter: number): void {
    this.paramCounter = counter;
  }

  /**
   * Enable or disable case-insensitive comparisons
   */
  setCaseInsensitive(value: boolean): void {
    this.caseInsensitive = value;
  }

  /**
   * Get the WHERE clause string
   */
  getClause(): string {
    return this.whereClause;
  }

  /**
   * Get the WHERE parameters
   */
  getParams(): any[] {
    return this.whereParams;
  }

  /**
   * Check if WHERE clause is empty
   */
  isEmpty(): boolean {
    return this.whereClause === "";
  }

  /**
   * Add a WHERE condition
   */
  addWhere<K extends ColumnNames<Row>>(
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
    value?: Row[K] | Row[K][] | JSONValue | JSONPath
  ): void;
  addWhere(
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
    value?: any
  ): void;
  addWhere(column: any, operator: any, value?: any): void {
    const qualifiedColumn = this.qualifyColumn(String(column));

    if (this.whereClause) {
      this.whereClause += " AND ";
    } else {
      this.whereClause = " WHERE ";
    }

    if (operator === "IS NULL" || operator === "IS NOT NULL") {
      this.whereClause += `${qualifiedColumn} ${operator}`;
    } else if (operator === "IN") {
      if (!Array.isArray(value) || value.length === 0) {
        throw new Error("IN operator requires a non-empty array");
      }
      const placeholders = value.map(() => `$${this.paramCounter++}`).join(", ");
      this.whereClause += `${qualifiedColumn} IN (${placeholders})`;
      this.whereParams.push(...value);
    } else if (operator === "BETWEEN") {
      if (!Array.isArray(value) || value.length !== 2) {
        throw new Error("BETWEEN operator requires an array with exactly 2 elements");
      }
      this.whereClause += `${qualifiedColumn} BETWEEN $${this.paramCounter} AND $${this.paramCounter + 1}`;
      this.whereParams.push(value[0], value[1]);
      this.paramCounter += 2;
    } else if (operator === "@>" || operator === "<@") {
      // JSON containment operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}::jsonb`;
      this.whereParams.push(JSON.stringify(value));
      this.paramCounter++;
    } else if (operator === "?" || operator === "?|" || operator === "?&") {
      // JSON key existence operators
      if (operator === "?") {
        this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
        this.whereParams.push(value);
        this.paramCounter++;
      } else {
        // ?| and ?& require array parameter
        if (!Array.isArray(value)) {
          throw new Error(`${operator} operator requires an array of keys`);
        }
        this.whereClause += `${qualifiedColumn} ${operator} ARRAY[${value.map(() => `$${this.paramCounter++}`).join(",")}]`;
        this.whereParams.push(...value);
      }
    } else if (operator === "@?" || operator === "@@") {
      // JSONPath operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    } else {
      let finalOperator = operator;

      // Handle case-insensitive comparisons
      if (this.caseInsensitive && (operator === "=" || operator === "!=")) {
        if (operator === "=") {
          finalOperator = "ILIKE";
        } else {
          finalOperator = "NOT ILIKE";
        }
      }

      this.whereClause += `${qualifiedColumn} ${finalOperator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    }
  }

  /**
   * Add an OR WHERE condition
   */
  addOrWhere<K extends ColumnNames<Row>>(
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
    value?: Row[K] | Row[K][] | JSONValue | JSONPath
  ): void;
  addOrWhere(
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
    value?: any
  ): void;
  addOrWhere(column: any, operator: any, value?: any): void {
    const qualifiedColumn = this.qualifyColumn(String(column));

    if (this.whereClause) {
      this.whereClause += " OR ";
    } else {
      this.whereClause = " WHERE ";
    }

    if (operator === "IS NULL" || operator === "IS NOT NULL") {
      this.whereClause += `${qualifiedColumn} ${operator}`;
    } else if (operator === "IN") {
      if (!Array.isArray(value) || value.length === 0) {
        throw new Error("IN operator requires a non-empty array");
      }
      const placeholders = value.map(() => `$${this.paramCounter++}`).join(", ");
      this.whereClause += `${qualifiedColumn} IN (${placeholders})`;
      this.whereParams.push(...value);
    } else if (operator === "BETWEEN") {
      if (!Array.isArray(value) || value.length !== 2) {
        throw new Error("BETWEEN operator requires an array with exactly 2 elements");
      }
      this.whereClause += `${qualifiedColumn} BETWEEN $${this.paramCounter} AND $${this.paramCounter + 1}`;
      this.whereParams.push(value[0], value[1]);
      this.paramCounter += 2;
    } else if (operator === "@>" || operator === "<@") {
      // JSON containment operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}::jsonb`;
      this.whereParams.push(JSON.stringify(value));
      this.paramCounter++;
    } else if (operator === "?" || operator === "?|" || operator === "?&") {
      // JSON key existence operators
      if (operator === "?") {
        this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
        this.whereParams.push(value);
        this.paramCounter++;
      } else {
        if (!Array.isArray(value)) {
          throw new Error(`${operator} operator requires an array of keys`);
        }
        this.whereClause += `${qualifiedColumn} ${operator} ARRAY[${value.map(() => `$${this.paramCounter++}`).join(",")}]`;
        this.whereParams.push(...value);
      }
    } else if (operator === "@?" || operator === "@@") {
      // JSONPath operators
      this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    } else {
      let finalOperator = operator;

      if (this.caseInsensitive && (operator === "=" || operator === "!=")) {
        if (operator === "=") {
          finalOperator = "ILIKE";
        } else {
          finalOperator = "NOT ILIKE";
        }
      }

      this.whereClause += `${qualifiedColumn} ${finalOperator} $${this.paramCounter}`;
      this.whereParams.push(value);
      this.paramCounter++;
    }
  }

  /**
   * Clone the WHERE builder state
   */
  clone(): WhereBuilder<Row> {
    const cloned = new WhereBuilder<Row>(this.qualifyColumn, this.paramCounter);
    cloned.whereClause = this.whereClause;
    cloned.whereParams = [...this.whereParams];
    cloned.caseInsensitive = this.caseInsensitive;
    return cloned;
  }
}
