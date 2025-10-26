# TypedQuery Class Analysis for GROUP BY and HAVING Integration

## Executive Summary

This document provides a detailed analysis of the `TypedQuery` class implementation to understand how we can integrate GROUP BY and HAVING clauses. The analysis covers query state management, method chaining patterns, SQL building mechanisms, and the type system.

---

## 1. Query State Management

### 1.1 Private Properties

The `TypedQuery` class maintains the following private properties to track query state:

```typescript
private pool: Pool;                              // Database connection pool
private tableName: TableName;                    // Table being queried
private tableAlias?: string;                     // Optional table alias
private selectedColumns: string[] = [];          // Columns to select
private whereClause: string = "";                // WHERE clause string
private whereParams: any[] = [];                 // Parameters for WHERE placeholders
private orderByClause: string = "";              // ORDER BY clause string
private limitClause: string = "";                // LIMIT clause string
private offsetClause: string = "";               // OFFSET clause string
private paramCounter: number = 1;                // Counter for generating $1, $2, etc.
private joins: JoinConfig[] = [];                // Array of join configurations
private schema: Schema;                          // Database schema for type safety
private joinedTables: Set<string> = new Set();   // Set of joined table names/aliases
```

### 1.2 Immutability Through Cloning

The class implements immutability by cloning before any mutation:

```typescript
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
  return newQuery;
}
```

**Key Points:**
- Each method call creates a new instance, preserving the original
- Enables safe query composition and reuse
- Arrays and Sets are shallow-cloned

---

## 2. Method Chaining Implementation

### 2.1 Pattern Overview

The class uses two types of method chaining:

#### Type A: Methods that Return `this`
These methods modify the query in-place and return `this`:
- `where()` - adds WHERE conditions
- `orWhere()` - adds OR conditions
- `orderBy()` - adds ORDER BY columns
- `limit()` - sets LIMIT
- `offset()` - sets OFFSET

**Note:** These methods have a critical issue for true immutability - they modify state directly instead of using clone().

#### Type B: Methods that Return Cloned Instance
These methods create a clone and return it:
- `select()` - specifies columns
- `innerJoin()`, `leftJoin()`, `rightJoin()`, `fullJoin()` - add joins
- `first()` - returns first result
- `count()` - returns count

### 2.2 Execution Methods

```typescript
async execute(): Promise<Row[]>        // Execute and return all rows
async first(): Promise<Row | null>     // Return first row or null
async count(): Promise<number>         // Count matching rows
toSQL(): { query: string; params }     // Get SQL without executing
```

---

## 3. SQL Building Mechanism

### 3.1 Query Construction Order

SQL is built in the `execute()` and `toSQL()` methods following standard SQL order:

```typescript
const query = `SELECT ${columns} ${this.buildFromClause()}${
  this.whereClause
}${this.orderByClause}${this.limitClause}${this.offsetClause}`;
```

**Current order:** SELECT → FROM → WHERE → ORDER BY → LIMIT → OFFSET

**Standard SQL order:** SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT → OFFSET

### 3.2 Building FROM Clause

```typescript
private buildFromClause(): string {
  let fromClause = `FROM ${String(this.tableName)}`;
  if (this.tableAlias) {
    fromClause += ` AS ${this.tableAlias}`;
  }

  for (const join of this.joins) {
    const joinTableRef = join.alias
      ? `${join.table} AS ${join.alias}`
      : join.table;
    fromClause += ` ${join.type} JOIN ${joinTableRef} ON ${join.leftColumn} = ${join.rightColumn}`;
  }

  return fromClause;
}
```

### 3.3 Column Qualification

```typescript
private qualifyColumnName(column: string): string {
  // If already qualified (contains dot), return as-is
  if (column.includes(".")) {
    return column;
  }

  // Only qualify if JOINs exist (to avoid ambiguity)
  if (this.joins.length === 0) {
    return column;
  }

  // For queries with JOINs, qualify with main table reference
  return `${this.getTableReference()}.${column}`;
}
```

**Logic:**
- Pre-qualified columns (e.g., "users.id") are passed through
- Single-table queries use unqualified names (backward compatible)
- Multi-table queries auto-qualify with main table reference

---

## 4. Parameter Handling

### 4.1 Parameter Counter Mechanism

The `paramCounter` starts at 1 and increments with each parameterized value:

```typescript
// In where() method
this.whereClause += `${qualifiedColumn} ${operator} $${this.paramCounter}`;
this.whereParams.push(value);
this.paramCounter++;

// For IN operator with arrays
const placeholders = value
  .map(() => `$${this.paramCounter++}`)
  .join(", ");
```

### 4.2 Cloning Consideration

When cloning, `paramCounter` is copied at its current value:
```typescript
newQuery.paramCounter = this.paramCounter;
```

This ensures correct parameter numbering when building on cloned queries.

---

## 5. Current Type System for Query Building

### 5.1 TypedQuery Generic Parameters

```typescript
export class TypedQuery<
  TableName extends string = string,
  Row extends Record<string, any> = Record<string, any>,
  Schema extends Record<string, any> = Record<string, any>
>
```

- **TableName**: The table being queried (ensures type-safe table selection)
- **Row**: The return type of the query (changes with select() calls)
- **Schema**: The full database schema (enables type checking for all tables)

### 5.2 Overloading Example: `where()` Method

```typescript
// Typed version (type-safe column names)
where<K extends ColumnNames<Row>>(
  column: K,
  operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
  value: Row[K] | Row[K][]
): this;

// Flexible version (allows string columns)
where(
  column: string,
  operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
  value: any
): this;

// Implementation
where(column: any, operator: string, value: any): this {
  // ... implementation
  return this;
}
```

This pattern provides both type safety (first overload) and flexibility (second overload).

### 5.3 Return Type Modifications with select()

```typescript
select<K extends keyof Row & string>(
  ...columns: K[]
): TypedQuery<TableName, SelectColumns<Row, K>, Schema>;

select(...columns: string[]): TypedQuery<TableName, any, Schema>;
```

The `select()` method narrows the Row type to only include selected columns.

---

## 6. Integration Strategy for GROUP BY and HAVING

### 6.1 Required Properties

Add to the private properties of TypedQuery:

```typescript
private groupByColumns: string[] = [];           // GROUP BY columns
private havingClause: string = "";               // HAVING clause string
private havingParams: any[] = [];                // Parameters for HAVING placeholders
```

### 6.2 Cloning Updates

Update the `clone()` method to include:

```typescript
newQuery.groupByColumns = [...this.groupByColumns];
newQuery.havingClause = this.havingClause;
newQuery.havingParams = [...this.havingParams];
```

### 6.3 New Methods Required

#### groupBy() method

```typescript
groupBy<K extends ColumnNames<Row>>(
  ...columns: K[]
): TypedQuery<TableName, Row, Schema>;
groupBy(...columns: string[]): TypedQuery<TableName, Row, Schema>;
groupBy(...columns: any[]): TypedQuery<TableName, Row, Schema> {
  const newQuery = this.clone();
  newQuery.groupByColumns = columns.map(col => this.qualifyColumnName(String(col)));
  return newQuery;
}
```

#### having() method

```typescript
having(
  condition: string,
  operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "ILIKE" | "IN",
  value: any
): TypedQuery<TableName, Row, Schema> {
  const newQuery = this.clone();
  
  if (newQuery.havingClause) {
    newQuery.havingClause += " AND ";
  } else {
    newQuery.havingClause = " HAVING ";
  }

  if (operator === "IN" && Array.isArray(value)) {
    const placeholders = value
      .map(() => `$${newQuery.paramCounter++}`)
      .join(", ");
    newQuery.havingClause += `${condition} IN (${placeholders})`;
    newQuery.havingParams.push(...value);
  } else {
    newQuery.havingClause += `${condition} ${operator} $${newQuery.paramCounter}`;
    newQuery.havingParams.push(value);
    newQuery.paramCounter++;
  }

  return newQuery;
}
```

#### orHaving() method

Similar to having() but with OR logic instead of AND.

### 6.4 SQL Building Updates

Update the `execute()` and `toSQL()` methods:

```typescript
// Old order
const query = `SELECT ${columns} ${this.buildFromClause()}${
  this.whereClause
}${this.orderByClause}${this.limitClause}${this.offsetClause}`;

// New order
const query = `SELECT ${columns} ${this.buildFromClause()}${
  this.whereClause
}${this.buildGroupByClause()}${this.havingClause}${
  this.orderByClause
}${this.limitClause}${this.offsetClause}`;
```

Add a new method:

```typescript
private buildGroupByClause(): string {
  if (this.groupByColumns.length === 0) {
    return "";
  }
  return ` GROUP BY ${this.groupByColumns.join(", ")}`;
}
```

### 6.5 Parameter Merging

Currently, `execute()` only passes `this.whereParams`:

```typescript
const result = await this.pool.query<Row>(query, this.whereParams);
```

This must be updated to merge both WHERE and HAVING parameters in the correct order:

```typescript
const params = [...this.whereParams, ...this.havingParams];
const result = await this.pool.query<Row>(query, params);
```

**Important:** Parameter order must match the order they appear in the SQL query. Since WHERE comes before HAVING in SQL, whereParams must come first.

### 6.6 COUNT Query Updates

The `count()` method currently only uses whereParams. It must be updated:

```typescript
async count(): Promise<number> {
  const query = `SELECT COUNT(*) as count ${this.buildFromClause()}${
    this.whereClause
  }${this.buildGroupByClause()}${this.havingClause}`;
  const params = [...this.whereParams, ...this.havingParams];
  const result = await this.pool.query<{ count: string }>(
    query,
    params
  );
  return parseInt(result.rows[0].count, 10);
}
```

---

## 7. Key Design Considerations

### 7.1 Immutability Pattern

**Current Issue:** Methods like `where()`, `orderBy()`, etc. modify state and return `this`, breaking immutability.

**Better Approach:** All methods should clone and return the cloned instance:

```typescript
where(...): this {
  const newQuery = this.clone();
  // modify newQuery
  return newQuery;
}
```

However, since the existing code uses `this` pattern, GROUP BY/HAVING should follow the same pattern for consistency.

### 7.2 Column Qualification

GROUP BY and HAVING columns should use the same qualification logic as WHERE and ORDER BY:

```typescript
// In groupBy method
newQuery.groupByColumns = columns.map(col => 
  this.qualifyColumnName(String(col))
);
```

### 7.3 Aggregate Function Support

GROUP BY queries often use aggregate functions. The current `select()` method already supports arbitrary strings:

```typescript
.select("user_id", "COUNT(*) as count", "AVG(price) as avg_price")
```

This works for aggregates without additional changes.

### 7.4 HAVING Clause Complexity

Unlike WHERE, HAVING can reference:
1. Aggregate functions: `HAVING COUNT(*) > 10`
2. Column aliases: `HAVING total > 100` (if `COUNT(*) as total`)
3. Expressions: `HAVING SUM(amount) / COUNT(*) > 50`

The proposed method signature is flexible enough:

```typescript
having("COUNT(*)", ">", 10)           // Works
having("SUM(amount)", ">", 1000)      // Works
having("total", ">", 100)             // Works (if alias)
```

### 7.5 Parameter Numbering Edge Case

When merging whereParams and havingParams, the paramCounter must be accurate. Current implementation increments paramCounter during WHERE building, ensuring HAVING parameters get correct numbers.

**Example:**
```typescript
query
  .where("active", "=", true)      // Uses $1, increments to 2
  .groupBy("user_id")
  .having("COUNT(*)", ">", 10)     // Uses $2, increments to 3
  // Final query: WHERE active = $1 HAVING COUNT(*) > $2
```

---

## 8. Testing Implications

### 8.1 Required Test Cases

```typescript
describe("GROUP BY clauses", () => {
  it("should handle single GROUP BY column");
  it("should handle multiple GROUP BY columns");
  it("should handle GROUP BY with qualified columns in JOINs");
  it("should handle GROUP BY with aggregate functions in SELECT");
  it("should handle GROUP BY with WHERE clause");
  it("should handle GROUP BY with ORDER BY");
  it("should handle GROUP BY with LIMIT");
});

describe("HAVING clauses", () => {
  it("should handle HAVING with aggregate function");
  it("should handle HAVING with comparison operators");
  it("should handle HAVING with IN operator");
  it("should handle multiple HAVING conditions with AND");
  it("should handle HAVING with OR");
  it("should handle HAVING parameter numbering");
});

describe("GROUP BY and HAVING together", () => {
  it("should handle GROUP BY with HAVING");
  it("should handle GROUP BY with WHERE and HAVING");
  it("should handle GROUP BY with JOINs");
  it("should handle GROUP BY with complex aggregates");
});
```

### 8.2 Test Data Requirements

Current TestSchema needs aggregation-friendly structure:

```typescript
interface AggregationResult {
  user_id: number;
  total_posts: number;
  avg_likes: number;
  min_date: Date;
  max_date: Date;
}
```

---

## 9. Migration Path for Existing Code

### 9.1 Backward Compatibility

All proposed changes are additive:
- New properties in private fields (no impact)
- New methods (no impact to existing methods)
- SQL building change is internal
- Parameter handling change is transparent

### 9.2 No Breaking Changes

Existing code like:
```typescript
db.table("users")
  .select("id", "name")
  .where("active", "=", true)
  .orderBy("name")
  .execute()
```

Will work exactly the same way.

---

## 10. Example Usage After Implementation

```typescript
// Get average age by department where count > 5
const results = await db.table("users")
  .select("department", "COUNT(*) as count", "AVG(age) as avg_age")
  .where("active", "=", true)
  .groupBy("department")
  .having("COUNT(*)", ">", 5)
  .orderBy("avg_age", "DESC")
  .execute();

// With JOINs
const stats = await db.table("users")
  .select(
    "users.id",
    "users.name",
    "COUNT(posts.id) as total_posts",
    "AVG(posts.likes) as avg_likes"
  )
  .leftJoin("posts", "users.id", "posts.user_id")
  .where("users.active", "=", true)
  .groupBy("users.id", "users.name")
  .having("COUNT(posts.id)", ">", 10)
  .orderBy("avg_likes", "DESC")
  .execute();

// Using raw SQL for complex aggregates (still works)
const complex = await db.raw(
  `SELECT user_id, COUNT(*) as count 
   FROM posts 
   GROUP BY user_id 
   HAVING COUNT(*) > $1
   ORDER BY count DESC`,
  [5]
);
```

---

## Summary

The TypedQuery class provides a solid foundation for GROUP BY and HAVING integration through:

1. **Established immutability pattern** - Clone before mutate
2. **Flexible parameter handling** - paramCounter manages placeholder generation
3. **Column qualification logic** - Existing system handles both unqualified and qualified names
4. **Method chaining architecture** - Consistent interface for building queries
5. **Type safety through overloading** - Provides both type-safe and flexible options

The implementation requires:
- 3 new private properties (groupByColumns, havingClause, havingParams)
- 3 new public methods (groupBy, having, orHaving)
- Updates to 4 existing methods (clone, execute, count, toSQL)
- Parameter merging logic for WHERE + HAVING
- Comprehensive test coverage

All changes maintain backward compatibility with existing code.
