/**
 * CTE (Common Table Expression) builder for TypedQuery
 * Handles WITH clauses for complex queries
 */

export interface CteDefinition {
  name: string;
  query: string;
  params: any[];
  columnNames?: string[];
}

export class CteBuilder {
  private ctes: CteDefinition[] = [];

  constructor(
    private sanitizeIdentifier: (identifier: string, allowComplex?: boolean) => string
  ) {}

  /**
   * Add a CTE (Common Table Expression)
   */
  addCte(name: string, query: string, params: any[], columnNames?: string[]): void {
    this.ctes.push({
      name: this.sanitizeIdentifier(name),
      query,
      params,
      columnNames: columnNames?.map(col => this.sanitizeIdentifier(col)),
    });
  }

  /**
   * Check if any CTEs have been added
   */
  hasCtes(): boolean {
    return this.ctes.length > 0;
  }

  /**
   * Get all CTE definitions
   */
  getCtes(): CteDefinition[] {
    return this.ctes;
  }

  /**
   * Build the WITH clause
   */
  buildWithClause(): string {
    if (this.ctes.length === 0) {
      return "";
    }

    const cteStrings = this.ctes.map((cte) => {
      const columns = cte.columnNames
        ? `(${cte.columnNames.join(", ")})`
        : "";
      return `${cte.name}${columns} AS (${cte.query})`;
    });

    return `WITH ${cteStrings.join(", ")} `;
  }

  /**
   * Get all parameters from all CTEs
   */
  getAllParams(): any[] {
    return this.ctes.flatMap(cte => cte.params);
  }

  /**
   * Clone the CTE builder state
   */
  clone(): CteBuilder {
    const cloned = new CteBuilder(this.sanitizeIdentifier);
    cloned.ctes = [...this.ctes];
    return cloned;
  }
}
