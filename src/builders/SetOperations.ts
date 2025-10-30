/**
 * Set Operations builder for TypedQuery
 * Handles UNION, UNION ALL, INTERSECT, EXCEPT operations
 */

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
        // Replace $1, $2, etc. with the correct parameter numbers
        for (let i = op.params.length; i >= 1; i--) {
          const oldParam = `$${i}`;
          const newParam = `$${paramCounter + i - 1}`;
          renumberedQuery = renumberedQuery.replace(new RegExp(`\\${oldParam}\\b`, 'g'), newParam);
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
