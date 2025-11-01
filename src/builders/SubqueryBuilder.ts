/**
 * Subquery builder for TypedQuery
 * Handles subqueries in WHERE clauses (IN, EXISTS, comparisons)
 */

import { sanitizeSqlIdentifier } from "../utils";
import { DatabaseError } from "../errors";

export class SubqueryBuilder {
  /**
   * Validate SQL operator to prevent injection
   */
  private static validateOperator(operator: string): string {
    const validOperators = new Set([
      '=', '!=', '<>', '<', '>', '<=', '>=',
      'LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE',
      '~', '~*', '!~', '!~*', // Regex operators
      '@@', '@>', '<@', // JSONB operators
      'IS', 'IS NOT', 'IS DISTINCT FROM', 'IS NOT DISTINCT FROM'
    ]);

    const normalizedOp = operator.trim().toUpperCase();

    if (!validOperators.has(normalizedOp) && !validOperators.has(operator.trim())) {
      throw new DatabaseError(
        `Invalid operator: ${operator}. Must be one of: ${Array.from(validOperators).join(', ')}`,
        'INVALID_OPERATOR',
        { query: '', params: [], detail: `operator: ${operator}` }
      );
    }

    return operator.trim();
  }

  /**
   * Validate subquery SQL to prevent obvious injection attempts
   * Note: This is basic validation - the subquery should ideally use parameterized queries
   */
  private static validateSubquery(subquery: string): void {
    const trimmed = subquery.trim();

    if (trimmed.length === 0) {
      throw new DatabaseError(
        'Subquery cannot be empty',
        'INVALID_SUBQUERY',
        { query: '', params: [], detail: 'subquery is empty' }
      );
    }

    // Check for obvious SQL injection patterns (stacked queries)
    if (/;\s*(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|CREATE|EXEC|EXECUTE)\b/i.test(trimmed)) {
      throw new DatabaseError(
        'Subquery contains dangerous SQL statements',
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: 'subquery contains stacked queries' }
      );
    }

    // Subquery should start with SELECT (or WITH for CTEs)
    if (!/^\s*(SELECT|WITH)\b/i.test(trimmed)) {
      throw new DatabaseError(
        'Subquery must start with SELECT or WITH',
        'INVALID_SUBQUERY',
        { query: '', params: [], detail: `subquery: ${trimmed.substring(0, 50)}...` }
      );
    }

    // Check for length to prevent DoS
    if (trimmed.length > 10000) {
      throw new DatabaseError(
        'Subquery exceeds maximum length of 10000 characters',
        'INVALID_SUBQUERY',
        { query: '', params: [], detail: `length: ${trimmed.length}` }
      );
    }
  }
  /**
   * Create an IN subquery
   * Usage: whereIn("user_id", subquery("SELECT id FROM users WHERE active = true"))
   */
  static createInSubquery(column: string, subquery: string, params: any[]): {
    clause: string;
    params: any[];
  } {
    const sanitizedColumn = sanitizeSqlIdentifier(column);
    this.validateSubquery(subquery);

    return {
      clause: `${sanitizedColumn} IN (${subquery})`,
      params,
    };
  }

  /**
   * Create a NOT IN subquery
   */
  static createNotInSubquery(column: string, subquery: string, params: any[]): {
    clause: string;
    params: any[];
  } {
    const sanitizedColumn = sanitizeSqlIdentifier(column);
    this.validateSubquery(subquery);

    return {
      clause: `${sanitizedColumn} NOT IN (${subquery})`,
      params,
    };
  }

  /**
   * Create an EXISTS subquery
   */
  static createExistsSubquery(subquery: string, params: any[]): {
    clause: string;
    params: any[];
  } {
    this.validateSubquery(subquery);

    return {
      clause: `EXISTS (${subquery})`,
      params,
    };
  }

  /**
   * Create a NOT EXISTS subquery
   */
  static createNotExistsSubquery(subquery: string, params: any[]): {
    clause: string;
    params: any[];
  } {
    this.validateSubquery(subquery);

    return {
      clause: `NOT EXISTS (${subquery})`,
      params,
    };
  }

  /**
   * Create a comparison subquery (=, >, <, etc.)
   * Usage: where("salary", ">", subquery("SELECT AVG(salary) FROM employees"))
   */
  static createComparisonSubquery(
    column: string,
    operator: string,
    subquery: string,
    params: any[]
  ): {
    clause: string;
    params: any[];
  } {
    const sanitizedColumn = sanitizeSqlIdentifier(column);
    const validatedOperator = this.validateOperator(operator);
    this.validateSubquery(subquery);

    return {
      clause: `${sanitizedColumn} ${validatedOperator} (${subquery})`,
      params,
    };
  }

  /**
   * Create an ANY subquery
   * Usage: where("salary", ">", ANY(subquery("SELECT salary FROM managers")))
   */
  static createAnySubquery(
    column: string,
    operator: string,
    subquery: string,
    params: any[]
  ): {
    clause: string;
    params: any[];
  } {
    const sanitizedColumn = sanitizeSqlIdentifier(column);
    const validatedOperator = this.validateOperator(operator);
    this.validateSubquery(subquery);

    return {
      clause: `${sanitizedColumn} ${validatedOperator} ANY (${subquery})`,
      params,
    };
  }

  /**
   * Create an ALL subquery
   * Usage: where("salary", ">", ALL(subquery("SELECT salary FROM employees")))
   */
  static createAllSubquery(
    column: string,
    operator: string,
    subquery: string,
    params: any[]
  ): {
    clause: string;
    params: any[];
  } {
    const sanitizedColumn = sanitizeSqlIdentifier(column);
    const validatedOperator = this.validateOperator(operator);
    this.validateSubquery(subquery);

    return {
      clause: `${sanitizedColumn} ${validatedOperator} ALL (${subquery})`,
      params,
    };
  }
}
