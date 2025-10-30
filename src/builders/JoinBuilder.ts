/**
 * JOIN builder for TypedQuery
 * Handles INNER, LEFT, RIGHT, and FULL OUTER joins
 */

import type { JoinCondition } from "../types";

export interface JoinSpec {
  type: "INNER" | "LEFT" | "RIGHT" | "FULL";
  table: string;
  conditions: JoinCondition[];
  alias?: string;
}

export class JoinBuilder {
  private joins: JoinSpec[] = [];
  private joinedTables: Set<string> = new Set();

  constructor(
    private qualifyColumn: (column: string) => string,
    private sanitizeIdentifier: (identifier: string, allowComplex?: boolean) => string
  ) {}

  /**
   * Get all join specifications
   */
  getJoins(): JoinSpec[] {
    return this.joins;
  }

  /**
   * Get set of joined table names
   */
  getJoinedTables(): Set<string> {
    return this.joinedTables;
  }

  /**
   * Check if any joins have been added
   */
  hasJoins(): boolean {
    return this.joins.length > 0;
  }

  /**
   * Add a join with compound key support
   */
  addJoin(
    type: "INNER" | "LEFT" | "RIGHT" | "FULL",
    table: string,
    conditions: JoinCondition[],
    alias?: string
  ): void {
    const tableIdentifier = alias || table;
    this.joinedTables.add(tableIdentifier);

    this.joins.push({
      type,
      table,
      conditions,
      alias,
    });
  }

  /**
   * Build the FROM clause with all joins
   */
  buildFromClause(baseTable: string, baseAlias?: string): string {
    let from = baseAlias
      ? `FROM ${this.sanitizeIdentifier(baseTable)} AS ${this.sanitizeIdentifier(baseAlias)}`
      : `FROM ${this.sanitizeIdentifier(baseTable)}`;

    for (const join of this.joins) {
      const joinTable = join.alias
        ? `${this.sanitizeIdentifier(join.table)} AS ${this.sanitizeIdentifier(join.alias)}`
        : this.sanitizeIdentifier(join.table);

      // Build ON conditions (supporting compound keys)
      const onConditions = join.conditions
        .map((condition) => {
          const left = this.qualifyColumn(condition.leftColumn);
          const right = this.qualifyColumn(condition.rightColumn);
          return `${left} = ${right}`;
        })
        .join(" AND ");

      from += ` ${join.type} JOIN ${joinTable} ON ${onConditions}`;
    }

    return from;
  }

  /**
   * Clone the JOIN builder state
   */
  clone(): JoinBuilder {
    const cloned = new JoinBuilder(this.qualifyColumn, this.sanitizeIdentifier);
    cloned.joins = [...this.joins];
    cloned.joinedTables = new Set(this.joinedTables);
    return cloned;
  }
}
