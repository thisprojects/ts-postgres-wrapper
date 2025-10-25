# ts-postgres-wrapper

A TypeScript wrapper for node-postgres (pg) that provides type-safe queries with autocomplete and compile-time type checking.

## The Problem

When using `pg` directly, you lose type safety:

```typescript
const result = await pool.query("SELECT id, name FROM users WHERE id = $1", [
  123,
]);
// result.rows is any[] - no autocomplete, no type checking!
```

## The Solution

```typescript
const user = await db
  .table("users")
  .where("id", "=", 123)
  .select("id", "name")
  .execute();
// Fully typed! TypeScript knows the shape of your data
```

## Installation

```bash
npm install ts-postgres-wrapper pg
npm install --save-dev @types/pg
```

## Quick Start

### 1. Define Your Schema

```typescript
// schema.ts
export interface DatabaseSchema {
  users: {
    id: number;
    name: string;
    email: string;
    created_at: Date;
  };
  posts: {
    id: number;
    user_id: number;
    title: string;
    content: string;
  };
}
```

### 2. Create Typed Connection

```typescript
import { Pool } from "pg";
import { createTypedPg } from "ts-postgres-wrapper";
import type { DatabaseSchema } from "./schema";

const pool = new Pool({
  /* your config */
});
const db = createTypedPg<DatabaseSchema>(pool);
```

### 3. Make Type-Safe Queries

```typescript
// Select all columns
const users = await db.table("users").execute();
// Type: { id: number, name: string, email: string, created_at: Date }[]

// Select specific columns
const names = await db.table("users").select("id", "name").execute();
// Type: { id: number, name: string }[]

// Add WHERE clause
const user = await db.table("users").where("id", "=", 123).first();
// Type: { id: number, name: string, email: string, created_at: Date } | null

// Chain multiple conditions
const posts = await db
  .table("posts")
  .where("user_id", "=", 1)
  .where("published", "=", true)
  .execute();
```

## Features

✅ **Type-safe column selection** - Only select columns that exist  
✅ **Type-safe WHERE clauses** - Column names and values are checked  
✅ **Autocomplete everywhere** - Full IDE support  
✅ **Minimal overhead** - Thin wrapper around pg  
✅ **Escape hatch** - Use raw queries when needed

## API

### `db.table(tableName)`

Start a query on a specific table.

### `.select(...columns)`

Select specific columns (optional - defaults to `*`).

### `.where(column, operator, value)`

Add a WHERE condition. Operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.

### `.execute()`

Execute the query and return all results.

### `.first()`

Execute the query and return the first result or `null`.

### `db.raw<T>(query, params?)`

Execute a raw SQL query with manual type annotation.

### `db.getPool()`

Get the underlying pg Pool for advanced usage (transactions, etc.).

## Roadmap (Future Features)

- [ ] JOIN support
- [ ] ORDER BY, LIMIT, OFFSET
- [ ] INSERT, UPDATE, DELETE queries
- [ ] OR conditions in WHERE
- [ ] Schema generation from database
- [ ] Aggregate functions (COUNT, SUM, etc.)
- [ ] Transaction helpers
- [ ] Query logging/debugging

## Contributing

PRs welcome! This is an MVP - lots of room for improvement.

## License

MIT
