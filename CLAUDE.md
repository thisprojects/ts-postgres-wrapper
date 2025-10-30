# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`postgres-query-builder-ts` is a TypeScript wrapper for node-postgres (pg) that provides type-safe queries with autocomplete and compile-time type checking. It's a thin query builder that eliminates the common problem of losing type safety when using `pg` directly.

## Development Commands

### Testing
```bash
npm test                 # Run all tests with Jest
npm run test:watch       # Run tests in watch mode
npm run test:coverage    # Run tests with coverage report
```

### Building
```bash
npm run build           # Compile TypeScript to dist/
npm run clean           # Remove dist/ directory
```

### Running Tests
- All tests are in the `tests/` directory
- Unit tests are in `tests/unit/`
- Test utilities and mocks are in `tests/test_utils/`
- Old integration tests are preserved in `tests/old_tests/` but not actively used
- Tests use Jest with ts-jest preset
- Custom Jest matchers are defined in `jest.setup.ts` for database query assertions

## Architecture

### Core Components

**`TypedQuery<TableName, Row, Schema>` (src/index.ts:27-384)**
- Immutable query builder using method chaining
- Each method returns a cloned instance with updated state
- Handles SELECT, WHERE, ORDER BY, LIMIT, OFFSET, JOIN operations
- Maintains query state: selected columns, where clauses, parameters, joins, ordering, pagination
- Uses parameterized queries ($1, $2, etc.) for SQL injection protection
- Supports both typed column names (from schema) and string-based columns for flexibility

**`TypedPg<Schema>` (src/index.ts:389-594)**
- Main entry point for database operations
- Wraps a pg Pool instance
- Provides CRUD methods: `insert()`, `update()`, `delete()` with schema validation
- Offers `raw()` for complex SQL with manual type annotation
- Implements `transaction()` with automatic commit/rollback
- All mutating operations (UPDATE/DELETE) require explicit WHERE conditions to prevent accidental bulk operations

**`createTypedPg()` (src/index.ts:600-613)**
- Factory function that accepts Pool instance, connection string, or config object
- Returns a TypedPg instance bound to the provided schema type

### Key Design Patterns

1. **Immutable Query Building**: All query builder methods clone the query state before modifying, ensuring queries can be safely reused and composed.

2. **Type Safety Through Generics**: The library uses TypeScript generics extensively to ensure compile-time type checking:
   - `Schema` defines all tables and their columns
   - `TableName` ensures only valid table names can be used
   - `Row` tracks the current query's return type, changing when columns are selected

3. **Column Qualification**: Columns are automatically qualified with table names/aliases when JOINs are present to avoid ambiguity. For single-table queries, unqualified names are used for backward compatibility.

4. **Parameter Counting**: WHERE clauses use a `paramCounter` to generate unique parameter placeholders ($1, $2, etc.) across all conditions.

5. **Validation in Mutations**: UPDATE and DELETE operations validate that:
   - WHERE clause is not empty
   - WHERE values are not null/undefined
   - SET clause has at least one column (for UPDATE)

6. **Pagination Validation**: LIMIT and OFFSET operations validate that (src/index.ts:816-889):
   - LIMIT must be a positive integer (>= 1), max 10,000,000
   - OFFSET must be a non-negative integer (>= 0), max 100,000,000
   - Both reject NaN, Infinity, non-integers, and values exceeding limits
   - Prevents SQL errors and DoS attacks from excessive result sets

### Test Infrastructure

**MockPool (tests/test_utils/MockPool.ts)**
- Simulates pg Pool behavior for unit testing
- Records all query executions with parameters
- Allows setting mock results to be returned
- Custom Jest matchers extend expect() for query assertions

**Custom Jest Matchers (jest.setup.ts)**
- `toHaveExecutedQuery(sql)` - Verify a query was executed
- `toHaveExecutedQueryWithParams(sql, params)` - Verify query with exact parameters
- `toHaveExecutedQueries(count)` - Verify number of queries executed

## Schema Definition Pattern

Users define a `DatabaseSchema` interface mapping table names to row types:

```typescript
interface DatabaseSchema {
  users: {
    id: number;
    name: string;
    email: string;
    // ... other columns
  };
  posts: {
    id: number;
    user_id: number;
    // ... other columns
  };
}
```

This schema is then used to create a typed database connection:

```typescript
const db = createTypedPg<DatabaseSchema>(pool);
```

## JOIN Support

The library supports INNER, LEFT, RIGHT, and FULL OUTER joins via dedicated methods:
- `innerJoin(table, leftCol, rightCol, alias?)`
- `leftJoin(table, leftCol, rightCol, alias?)`
- `rightJoin(table, leftCol, rightCol, alias?)`
- `fullJoin(table, leftCol, rightCol, alias?)`

When JOINs are present, columns are automatically qualified to avoid ambiguity. Users can also manually qualify columns using dot notation (e.g., "users.id").

### Type-Safe JOIN Return Types

JOIN methods use TypeScript generics to provide type-safe merged result types:

**INNER JOIN** - Merges both table types (all columns required):
```typescript
const query = db.from<"users">("users")
  .innerJoin("posts", "users.id", "posts.user_id");
// Result type: (users columns) & (posts columns)
// Both tables' columns are required in the result
```

**LEFT JOIN** - Makes joined table columns optional:
```typescript
const query = db.from<"users">("users")
  .leftJoin("posts", "users.id", "posts.user_id");
// Result type: (users columns) & Partial<(posts columns)>
// Users columns required, posts columns optional (may be null)
```

**RIGHT JOIN** - Makes original table columns optional:
```typescript
const query = db.from<"users">("users")
  .rightJoin("posts", "users.id", "posts.user_id");
// Result type: Partial<(users columns)> & (posts columns)
// Posts columns required, users columns optional (may be null)
```

**FULL JOIN** - Makes all columns optional:
```typescript
const query = db.from<"users">("users")
  .fullJoin("posts", "users.id", "posts.user_id");
// Result type: Partial<(users columns)> & Partial<(posts columns)>
// All columns optional (both tables may have null rows)
```

**Multiple JOINs** - Types accumulate across chained joins:
```typescript
const query = db.from<"users">("users")
  .innerJoin("posts", "users.id", "posts.user_id")
  .leftJoin("comments", "posts.id", "comments.post_id");
// Result type: (users) & (posts) & Partial<(comments)>
```

The type system correctly models SQL join semantics, ensuring TypeScript catches potential null reference errors at compile time.

## Advanced SQL Features

The library provides builder modules for advanced PostgreSQL features in `src/builders/`:

### Set Operations (UNION/INTERSECT/EXCEPT)

**`SetOperationsBuilder` (src/builders/SetOperations.ts)**
- Combines multiple SELECT queries using UNION, UNION ALL, INTERSECT, or EXCEPT
- Automatically renumbers parameters across queries ($1, $2, $3, etc.)
- Supports chaining multiple set operations

Example usage:
```typescript
import { SetOperationsBuilder } from './builders';

const builder = new SetOperationsBuilder();
builder.addOperation("UNION", "SELECT id FROM admins WHERE role = $1", ["admin"]);
builder.addOperation("UNION", "SELECT id FROM moderators WHERE role = $1", ["mod"]);

const result = builder.buildQuery(
  "SELECT id FROM users WHERE status = $1",
  ["active"]
);
// Query: "SELECT id FROM users WHERE status = $1 UNION SELECT id FROM admins WHERE role = $2 UNION SELECT id FROM moderators WHERE role = $3"
// Params: ["active", "admin", "mod"]
```

### Common Table Expressions (CTEs)

**`CteBuilder` (src/builders/CteBuilder.ts)**
- Creates WITH clauses for complex queries
- Supports multiple CTEs in a single query
- Allows specifying column names for CTEs
- Tracks parameters across all CTEs

Example usage:
```typescript
import { CteBuilder } from './builders';
import { sanitizeSqlIdentifier } from './utils';

const builder = new CteBuilder(sanitizeSqlIdentifier);

// Add CTEs
builder.addCte("active_users", "SELECT * FROM users WHERE status = $1", ["active"]);
builder.addCte(
  "user_stats",
  "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
  [],
  ["user_id", "order_count"] // Optional column names
);

// Build WITH clause
const withClause = builder.buildWithClause();
// "WITH active_users AS (SELECT * FROM users WHERE status = $1), user_stats(user_id, order_count) AS (...) "

const params = builder.getAllParams(); // ["active"]
```

### Subqueries

**`SubqueryBuilder` (src/builders/SubqueryBuilder.ts)**
- Static methods for creating subqueries in WHERE clauses
- Supports IN, NOT IN, EXISTS, NOT EXISTS, ANY, ALL, and comparison operators

Example usage:
```typescript
import { SubqueryBuilder } from './builders';

// IN subquery
const inResult = SubqueryBuilder.createInSubquery(
  "user_id",
  "SELECT id FROM users WHERE active = $1",
  [true]
);
// clause: "user_id IN (SELECT id FROM users WHERE active = $1)"
// params: [true]

// EXISTS subquery
const existsResult = SubqueryBuilder.createExistsSubquery(
  "SELECT 1 FROM orders WHERE orders.user_id = users.id",
  []
);
// clause: "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"

// ANY subquery
const anyResult = SubqueryBuilder.createAnySubquery(
  "salary",
  ">",
  "SELECT salary FROM managers WHERE department = $1",
  ["IT"]
);
// clause: "salary > ANY (SELECT salary FROM managers WHERE department = $1)"
// params: ["IT"]

// ALL subquery
const allResult = SubqueryBuilder.createAllSubquery(
  "salary",
  ">",
  "SELECT salary FROM employees WHERE department = $1",
  ["Sales"]
);
// clause: "salary > ALL (SELECT salary FROM employees WHERE department = $1)"
```

### UPSERT (ON CONFLICT)

**`TypedPg.upsert()` (src/index.ts:431)**
- Inserts rows or updates on conflict
- Supports conflict target (columns or constraint name)
- Allows custom update values or using excluded values

Example usage:
```typescript
// Upsert with conflict on specific columns
await db.upsert("users",
  { id: 1, name: "John", email: "john@example.com" },
  ["id"], // Conflict columns
  { name: "excluded.name", email: "excluded.email" } // Update with excluded values
);

// Upsert with constraint name
await db.upsert("users",
  { id: 1, name: "John", email: "john@example.com" },
  "users_email_key",
  { updated_at: "NOW()" }
);
```

## Error Prevention

The library prevents common mistakes:
- Empty WHERE clauses in UPDATE/DELETE operations (throws error with guidance to use raw SQL if intentional)
- null/undefined values in WHERE conditions (throws error listing invalid columns)
- Empty SET clauses in UPDATE operations

## Development Notes

- The library is built as a CommonJS module targeting ES2020
- TypeScript strict mode is enabled
- Source code is in `src/`, compiled output goes to `dist/`
- The library has minimal dependencies (only pg and its types)
- Bundle size is ~5KB with minimal runtime overhead
