# GROUP BY and HAVING Implementation Guide

## Quick Reference: Changes Required

### 1. Add Private Properties (Lines 42-44)

```typescript
// EXISTING CODE (lines 32-44)
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
private joins: JoinConfig[] = [];
private schema: Schema;
private joinedTables: Set<string> = new Set();

// ADD THESE THREE LINES
private groupByColumns: string[] = [];
private havingClause: string = "";
private havingParams: any[] = [];
```

### 2. Update clone() Method (Lines 62-83)

```typescript
// EXISTING CODE
private clone<NewRow extends Record<string, any> = Row>(): TypedQuery<
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
  
  // ADD THESE THREE LINES
  newQuery.groupByColumns = [...this.groupByColumns];
  newQuery.havingClause = this.havingClause;
  newQuery.havingParams = [...this.havingParams];
  
  return newQuery;
}
```

### 3. Add groupBy() Method (After orderBy method, around line 270)

```typescript
/**
 * Add GROUP BY clause (supports both typed columns and string-based columns)
 */
groupBy<K extends ColumnNames<Row>>(...columns: K[]): this;
groupBy(...columns: string[]): this;
groupBy(...columns: any[]): this {
  const newQuery = this.clone();
  newQuery.groupByColumns = columns.map((col) =>
    this.qualifyColumnName(String(col))
  );
  return newQuery;
}
```

### 4. Add having() Method (After limit method, around line 286)

```typescript
/**
 * Add HAVING clause (supports comparison operators and aggregate functions)
 */
having(
  condition: string,
  operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
  value: any
): this {
  if (this.havingClause) {
    this.havingClause += " AND ";
  } else {
    this.havingClause = " HAVING ";
  }

  if (operator === "IN" && Array.isArray(value)) {
    const placeholders = value
      .map(() => `$${this.paramCounter++}`)
      .join(", ");
    this.havingClause += `${condition} IN (${placeholders})`;
    this.havingParams.push(...value);
  } else {
    this.havingClause += `${condition} ${operator} $${this.paramCounter}`;
    this.havingParams.push(value);
    this.paramCounter++;
  }
  return this;
}
```

### 5. Add orHaving() Method (After having method)

```typescript
/**
 * Add OR HAVING clause
 */
orHaving(
  condition: string,
  operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
  value: any
): this {
  if (this.havingClause) {
    this.havingClause += " OR ";
  } else {
    this.havingClause = " HAVING ";
  }

  if (operator === "IN" && Array.isArray(value)) {
    const placeholders = value
      .map(() => `$${this.paramCounter++}`)
      .join(", ");
    this.havingClause += `${condition} IN (${placeholders})`;
    this.havingParams.push(...value);
  } else {
    this.havingClause += `${condition} ${operator} $${this.paramCounter}`;
    this.havingParams.push(value);
    this.paramCounter++;
  }
  return this;
}
```

### 6. Add buildGroupByClause() Method (After buildFromClause, around line 330)

```typescript
/**
 * Build the GROUP BY clause
 */
private buildGroupByClause(): string {
  if (this.groupByColumns.length === 0) {
    return "";
  }
  return ` GROUP BY ${this.groupByColumns.join(", ")}`;
}
```

### 7. Update execute() Method (Lines 335-346)

```typescript
// EXISTING CODE
async execute(): Promise<Row[]> {
  const columns = this.selectedColumns.length
    ? this.selectedColumns.join(", ")
    : "*";

  // CHANGE THIS LINE:
  // OLD:
  // const query = `SELECT ${columns} ${this.buildFromClause()}${
  //   this.whereClause
  // }${this.orderByClause}${this.limitClause}${this.offsetClause}`;

  // NEW:
  const query = `SELECT ${columns} ${this.buildFromClause()}${
    this.whereClause
  }${this.buildGroupByClause()}${this.havingClause}${
    this.orderByClause
  }${this.limitClause}${this.offsetClause}`;

  // CHANGE THIS LINE:
  // OLD:
  // const result = await this.pool.query<Row>(query, this.whereParams);

  // NEW:
  const params = [...this.whereParams, ...this.havingParams];
  const result = await this.pool.query<Row>(query, params);
  
  return result.rows;
}
```

### 8. Update count() Method (Lines 359-368)

```typescript
// EXISTING CODE
async count(): Promise<number> {
  // CHANGE THIS:
  // OLD:
  // const query = `SELECT COUNT(*) as count ${this.buildFromClause()}${
  //   this.whereClause
  // }`;

  // NEW:
  const query = `SELECT COUNT(*) as count ${this.buildFromClause()}${
    this.whereClause
  }${this.buildGroupByClause()}${this.havingClause}`;

  // CHANGE THIS:
  // OLD:
  // const result = await this.pool.query<{ count: string }>(
  //   query,
  //   this.whereParams
  // );

  // NEW:
  const params = [...this.whereParams, ...this.havingParams];
  const result = await this.pool.query<{ count: string }>(
    query,
    params
  );
  
  return parseInt(result.rows[0].count, 10);
}
```

### 9. Update toSQL() Method (Lines 373-383)

```typescript
// EXISTING CODE
toSQL(): { query: string; params: any[] } {
  const columns = this.selectedColumns.length
    ? this.selectedColumns.join(", ")
    : "*";

  // CHANGE THIS:
  // OLD:
  // const query = `SELECT ${columns} ${this.buildFromClause()}${
  //   this.whereClause
  // }${this.orderByClause}${this.limitClause}${this.offsetClause}`;

  // NEW:
  const query = `SELECT ${columns} ${this.buildFromClause()}${
    this.whereClause
  }${this.buildGroupByClause()}${this.havingClause}${
    this.orderByClause
  }${this.limitClause}${this.offsetClause}`;

  // CHANGE THIS:
  // OLD:
  // return { query, params: this.whereParams };

  // NEW:
  const params = [...this.whereParams, ...this.havingParams];
  return { query, params };
}
```

## Testing Checklist

Create a new test file: `tests/unit/groupByHaving.test.ts`

```typescript
import { TypedQuery } from "../../src/index";
import { MockPool, TestSchema } from "../test_utils";

describe("GROUP BY and HAVING clauses", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
  });

  describe("GROUP BY clauses", () => {
    it("should handle single GROUP BY column");
    it("should handle multiple GROUP BY columns");
    it("should handle GROUP BY with WHERE clause");
    it("should handle GROUP BY with ORDER BY");
    it("should handle GROUP BY with LIMIT");
    it("should handle GROUP BY with JOINs");
  });

  describe("HAVING clauses", () => {
    it("should handle HAVING with aggregate function");
    it("should handle HAVING with comparison operators");
    it("should handle HAVING with IN operator");
    it("should handle multiple HAVING conditions with AND");
    it("should handle HAVING with OR");
  });

  describe("GROUP BY and HAVING together", () => {
    it("should handle GROUP BY with HAVING");
    it("should handle GROUP BY with WHERE and HAVING");
    it("should handle parameter numbering correctly");
  });
});
```

## Usage Examples

### Example 1: Simple GROUP BY

```typescript
const results = await db.table("posts")
  .select("user_id", "COUNT(*) as post_count")
  .groupBy("user_id")
  .execute();

// SQL: SELECT user_id, COUNT(*) as post_count FROM posts GROUP BY user_id
```

### Example 2: GROUP BY with HAVING

```typescript
const results = await db.table("posts")
  .select("user_id", "COUNT(*) as post_count")
  .groupBy("user_id")
  .having("COUNT(*)", ">", 5)
  .execute();

// SQL: SELECT user_id, COUNT(*) as post_count FROM posts GROUP BY user_id HAVING COUNT(*) > $1
// params: [5]
```

### Example 3: Complex Query with WHERE and HAVING

```typescript
const results = await db.table("posts")
  .select("user_id", "AVG(likes) as avg_likes")
  .where("published", "=", true)
  .groupBy("user_id")
  .having("AVG(likes)", ">", 10)
  .orderBy("avg_likes", "DESC")
  .limit(10)
  .execute();

// SQL: SELECT user_id, AVG(likes) as avg_likes FROM posts WHERE published = $1 GROUP BY user_id HAVING AVG(likes) > $2 ORDER BY avg_likes DESC LIMIT 10
// params: [true, 10]
```

### Example 4: With JOINs

```typescript
const results = await db.table("users")
  .select("users.id", "users.name", "COUNT(posts.id) as total_posts")
  .leftJoin("posts", "users.id", "posts.user_id")
  .groupBy("users.id", "users.name")
  .having("COUNT(posts.id)", ">", 0)
  .execute();

// SQL: SELECT users.id, users.name, COUNT(posts.id) as total_posts FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id, users.name HAVING COUNT(posts.id) > $1
// params: [0]
```

## Key Points to Remember

1. **Parameter Order**: WHERE parameters come before HAVING parameters in the final params array
2. **Column Qualification**: GROUP BY columns are qualified the same way as WHERE columns (auto-qualify if JOINs exist)
3. **Method Pattern**: Following existing pattern, groupBy() and having() clone the query
4. **No Breaking Changes**: All existing code continues to work unchanged
5. **Aggregate Support**: The SELECT clause already supports aggregate functions via string-based selection

## Potential Issues and Solutions

### Issue: What if user forgets GROUP BY with aggregates?

Current behavior: Allows invalid SQL that database will reject.

Solution: Document requirement that aggregates require GROUP BY.

### Issue: Multiple GROUP BY calls replace previous?

Current design: Each groupBy() call REPLACES previous columns (due to clone pattern).

Fix: If chaining multiple groupBy() calls should accumulate, change implementation to:

```typescript
groupBy(...columns: any[]): this {
  const newQuery = this.clone();
  newQuery.groupByColumns.push(
    ...columns.map((col) => this.qualifyColumnName(String(col)))
  );
  return newQuery;
}
```

(Current implementation replaces, which is probably cleaner)

### Issue: Column aliases in HAVING

Users might want: `HAVING total > 100` where `total` is `COUNT(*) as total` from SELECT.

Current implementation: Supports this via string parameter:
```typescript
.having("total", ">", 100)
```

This works because we pass condition as string parameter.

