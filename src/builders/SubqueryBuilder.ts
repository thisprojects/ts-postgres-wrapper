/**
 * Subquery builder for TypedQuery
 * Handles subqueries in WHERE clauses (IN, EXISTS, comparisons)
 */

export class SubqueryBuilder {
  /**
   * Create an IN subquery
   * Usage: whereIn("user_id", subquery("SELECT id FROM users WHERE active = true"))
   */
  static createInSubquery(column: string, subquery: string, params: any[]): {
    clause: string;
    params: any[];
  } {
    return {
      clause: `${column} IN (${subquery})`,
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
    return {
      clause: `${column} NOT IN (${subquery})`,
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
    return {
      clause: `${column} ${operator} (${subquery})`,
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
    return {
      clause: `${column} ${operator} ANY (${subquery})`,
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
    return {
      clause: `${column} ${operator} ALL (${subquery})`,
      params,
    };
  }
}
