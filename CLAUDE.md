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
