/**
 * Set Operations builder for TypedQuery
 * Handles UNION, UNION ALL, INTERSECT, EXCEPT operations
 */

import { DatabaseError } from "../errors";

export type SetOperationType = "UNION" | "UNION ALL" | "INTERSECT" | "EXCEPT";

export interface SetOperation {
  type: SetOperationType;
  query: string;
  params: any[];
}

export class SetOperationsBuilder {
  private operations: SetOperation[] = [];

  /**
   * Add a set operation
   */
  addOperation(type: SetOperationType, query: string, params: any[]): void {
    this.operations.push({ type, query, params });
  }

  /**
   * Check if any set operations have been added
   */
  hasOperations(): boolean {
    return this.operations.length > 0;
  }

  /**
   * Get all set operations
   */
  getOperations(): SetOperation[] {
    return this.operations;
  }

  /**
   * Build the complete query with set operations
   */
  buildQuery(baseQuery: string, baseParams: any[]): { query: string; params: any[] } {
    // Validate operation count to prevent DoS
    if (this.operations.length > 100) {
      throw new DatabaseError(
        'Set operations count exceeds maximum of 100',
        'TOO_MANY_SET_OPERATIONS',
        { query: '', params: [], detail: `count: ${this.operations.length}` }
      );
    }

    if (this.operations.length === 0) {
      return { query: baseQuery, params: baseParams };
    }

    let fullQuery = baseQuery;
    let allParams = [...baseParams];
    let paramCounter = baseParams.length + 1;

    for (const op of this.operations) {
      // Renumber parameters in the operation query
      let renumberedQuery = op.query;
      if (op.params.length > 0) {
        // Find all parameter placeholders in the query
        const paramMatches = renumberedQuery.match(/\$(\d+)/g) || [];
        const paramNumbers = new Set(
          paramMatches.map(match => parseInt(match.substring(1)))
        );

        // Sort in descending order to avoid double-replacement issues
        const sortedParams = Array.from(paramNumbers).sort((a, b) => b - a);

        // Create mapping of old param number to new param number
        const paramMapping = new Map<number, number>();
        for (let i = 1; i <= op.params.length; i++) {
          paramMapping.set(i, paramCounter + i - 1);
        }

        // Replace parameters using the mapping (process largest numbers first)
        for (const oldNum of sortedParams) {
          const newNum = paramMapping.get(oldNum);
          if (newNum !== undefined) {
            renumberedQuery = renumberedQuery.replace(
              new RegExp(`\\$${oldNum}\\b`, 'g'),
              `$${newNum}`
            );
          }
        }

        paramCounter += op.params.length;
      }

      fullQuery += ` ${op.type} ${renumberedQuery}`;
      allParams.push(...op.params);
    }

    return { query: fullQuery, params: allParams };
  }

  /**
   * Clone the set operations builder state
   */
  clone(): SetOperationsBuilder {
    const cloned = new SetOperationsBuilder();
    cloned.operations = [...this.operations];
    return cloned;
  }
}
