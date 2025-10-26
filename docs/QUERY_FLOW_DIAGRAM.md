# TypedQuery State Flow and Method Chaining Visualization

## 1. Query Building Flow

```
TypedQuery Creation
        |
        v
[tableName: "users"]
[selectedColumns: []]
[whereClause: ""]
[whereParams: []]
[groupByColumns: []]
[havingClause: ""]
[havingParams: []]
[orderByClause: ""]
[limitClause: ""]
[offsetClause: ""]
[paramCounter: 1]
[joins: []]

        |
        |---- .select("id", "name") -----> Clone + Update selectedColumns
        |
        |---- .where("active", "=", true) -----> Modify + Return this
        |
        |---- .orWhere(...) -----> Modify + Return this
        |
        |---- .groupBy("dept") -----> Clone + Update groupByColumns
        |
        |---- .having("COUNT(*)", ">", 5) -----> Clone + Update havingClause
        |
        |---- .orderBy("name", "ASC") -----> Modify + Return this
        |
        |---- .limit(10) -----> Modify + Return this
        |
        |---- .offset(5) -----> Modify + Return this
        |
        v
     .execute()
```

## 2. SQL Generation Order

```
                 SELECT columns
                        |
                        v
                   FROM table
                 [JOIN ... ON ...]
                        |
                        v
                   WHERE conditions
                        |
                        v
                  GROUP BY columns  <-- NEW
                        |
                        v
                   HAVING conditions <-- NEW
                        |
                        v
                   ORDER BY columns
                        |
                        v
                     LIMIT n
                        |
                        v
                    OFFSET m
```

## 3. Parameter Array Building Example

```
Query:
  .where("active", "=", true)           // paramCounter: 1, then 2
  .where("age", ">", 18)                // paramCounter: 2, then 3
  .groupBy("dept")
  .having("COUNT(*)", ">", 5)           // paramCounter: 3, then 4
  .having("SUM(salary)", "<", 100000)   // paramCounter: 4, then 5

SQL: SELECT ... WHERE active = $1 AND age > $2 GROUP BY dept 
     HAVING COUNT(*) > $3 AND SUM(salary) < $4

whereParams:  [true, 18]
havingParams: [5, 100000]
params (final): [true, 18, 5, 100000]
                  ^^^ WHERE  ^^^ HAVING
```

## 4. Immutability Pattern (Clone vs Modify)

```
Type A: Methods returning this (modify in-place)
┌──────────────────────────────────────────┐
│ where() / orWhere() / orderBy()         │
│ limit() / offset()                      │
│                                         │
│ where(col, op, val): this {            │
│   this.whereClause += ...              │
│   this.whereParams.push(...)           │
│   this.paramCounter++                  │
│   return this  <-- SAME INSTANCE       │
│ }                                       │
└──────────────────────────────────────────┘

Type B: Methods returning clone
┌──────────────────────────────────────────┐
│ select() / JOIN methods / groupBy()      │
│                                         │
│ select(...cols): this {                │
│   const newQuery = this.clone()        │
│   newQuery.selectedColumns = cols      │
│   return newQuery  <-- NEW INSTANCE    │
│ }                                       │
└──────────────────────────────────────────┘

Issue: Mixing patterns can lead to unexpected behavior!
Solution: Follow existing pattern for consistency
```

## 5. Column Qualification Logic

```
Input column name (e.g., "id")
        |
        v
    Contains "."? ────── YES ──> Return as-is ("users.id")
        |
        NO
        |
        v
    Are there JOINs?
        |
        +---- YES -----> Qualify with table ref: "users.id"
        |
        +---- NO ------> Return unqualified: "id"

Applied to:
- WHERE conditions
- ORDER BY columns
- GROUP BY columns (NEW)
- But NOT ORDER BY clause strings (for consistency)
```

## 6. Clone Method Deep Dive

```
clone<NewRow>(): TypedQuery<TableName, NewRow, Schema> {
  const newQuery = new TypedQuery(pool, tableName, schema, tableAlias)
  
  // Copy all state
  newQuery.selectedColumns = [...this.selectedColumns]          // Shallow clone
  newQuery.whereClause = this.whereClause                       // String copy
  newQuery.whereParams = [...this.whereParams]                  // Shallow clone
  newQuery.orderByClause = this.orderByClause                   // String copy
  newQuery.limitClause = this.limitClause                       // String copy
  newQuery.offsetClause = this.offsetClause                     // String copy
  newQuery.paramCounter = this.paramCounter                     // Value copy
  newQuery.joins = [...this.joins]                              // Shallow clone
  newQuery.joinedTables = new Set(this.joinedTables)           // New Set copy
  
  // NEW: Copy GROUP BY and HAVING state
  newQuery.groupByColumns = [...this.groupByColumns]           // Shallow clone
  newQuery.havingClause = this.havingClause                    // String copy
  newQuery.havingParams = [...this.havingParams]               // Shallow clone
  
  return newQuery
}

Important: Objects in arrays are NOT deep-cloned!
If joins or groupByColumns contain objects, mutations affect originals.
```

## 7. Type System: Method Overloading

```
Example: where() method

// Overload 1: Type-safe (T is from Row)
where<K extends ColumnNames<Row>>(
  column: K,
  operator: Operator,
  value: Row[K] | Row[K][]
): this

// Overload 2: Flexible (string-based)
where(
  column: string,
  operator: Operator,
  value: any
): this

// Implementation (matches both overloads)
where(column: any, operator: any, value: any): this {
  // Actual implementation
}

Usage:
query.where("active", "=", true)  // ✓ Both overloads match
                                   // TypeScript picks most specific (Overload 1)
                                   // Validates column names from Row type

query.where("custom_field", "=", "value")  // ✓ String-based (Overload 2)
```

## 8. Complete Example Flow

```
db.table("users")
  .select("dept", "COUNT(*) as count", "AVG(salary) as avg_sal")
  .where("active", "=", true)
  .where("hired_date", ">", "2020-01-01")
  .groupBy("dept")
  .having("COUNT(*)", ">", 10)
  .having("AVG(salary)", ">", 50000)
  .orderBy("avg_sal", "DESC")
  .limit(5)
  .execute()

State progression:
1. new TypedQuery("users")
2. Clone -> select() -> selectedColumns = ["dept", "COUNT(*) as count", ...]
3. Modify -> where("active", "=", true) 
             whereClause = " WHERE active = $1"
             whereParams = [true]
             paramCounter = 2
4. Modify -> where("hired_date", ">", "2020-01-01")
             whereClause = " WHERE active = $1 AND hired_date > $2"
             whereParams = [true, "2020-01-01"]
             paramCounter = 3
5. Clone -> groupBy("dept")
             groupByColumns = ["dept"]
6. Clone -> having("COUNT(*)", ">", 10)
             havingClause = " HAVING COUNT(*) > $3"
             havingParams = [10]
             paramCounter = 4
7. Clone -> having("AVG(salary)", ">", 50000)
             havingClause = " HAVING COUNT(*) > $3 AND AVG(salary) > $4"
             havingParams = [10, 50000]
             paramCounter = 5
8. Modify -> orderBy("avg_sal", "DESC")
             orderByClause = " ORDER BY avg_sal DESC"
9. Modify -> limit(5)
             limitClause = " LIMIT 5"
10. execute()
    SQL = "SELECT dept, COUNT(*) as count, AVG(salary) as avg_sal 
           FROM users 
           WHERE active = $1 AND hired_date > $2 
           GROUP BY dept 
           HAVING COUNT(*) > $3 AND AVG(salary) > $4 
           ORDER BY avg_sal DESC 
           LIMIT 5"
    params = [true, "2020-01-01", 10, 50000]
    Final query log entry: {
      text: <SQL above>,
      values: [true, "2020-01-01", 10, 50000]
    }
```

## 9. Critical Issues Identified

```
Issue 1: Inconsistent Immutability
┌─────────────────────────────────────────┐
│ where(), orderBy() return 'this'        │
│ select(), JOIN methods return clone     │
│                                         │
│ Problem: Mixing patterns causes issues  │
│ const q1 = new TypedQuery().where(...) │
│ const q2 = q1.select(...).where(...)   │
│ // q2 modifications affect q1!          │
└─────────────────────────────────────────┘

Issue 2: paramCounter Shared Across Clones
┌─────────────────────────────────────────┐
│ paramCounter is value-copied             │
│ BUT modifications affect clones created │
│ from same base                          │
│                                         │
│ q1.where("a", "=", 1)  // paramCounter: 2
│ q2 = q1.select()       // gets copy of 2
│ q2.where("b", "=", 2)  // paramCounter: 3
│ // q1 and q2 both think counter is 3!   │
└─────────────────────────────────────────┘

Issue 3: Shallow Clone of Objects in Arrays
┌─────────────────────────────────────────┐
│ joins = [...this.joins]  // Shallow!    │
│ If JoinConfig objects are mutated,      │
│ mutation affects original               │
│                                         │
│ groupByColumns = [...this.groupByColumns]
│ Same issue with string mutations        │
│ (though strings are immutable in JS)    │
└─────────────────────────────────────────┘
```

## 10. Integration Points for GROUP BY/HAVING

```
buildGroupByClause(): string {
  if (this.groupByColumns.length === 0) return ""
  return ` GROUP BY ${this.groupByColumns.join(", ")}`
}

Updated execute():
  const params = [...this.whereParams, ...this.havingParams]
                 ^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^
                 WHERE params          HAVING params
                 (before GROUP BY)     (after GROUP BY)

Updated toSQL():
  return {
    query: `SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ...`
    params: [...this.whereParams, ...this.havingParams]
  }

Updated count():
  // Include GROUP BY/HAVING in count query
  const query = `SELECT COUNT(*) as count FROM ... WHERE ... GROUP BY ... HAVING ...`
  const params = [...this.whereParams, ...this.havingParams]
```

