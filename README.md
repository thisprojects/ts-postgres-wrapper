# postgres-query-builder-ts

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
npm install postgres-query-builder-ts pg
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
    age: number;
    is_active: boolean;
    created_at: Date;
  };
  posts: {
    id: number;
    user_id: number;
    title: string;
    content: string;
    published: boolean;
    created_at: Date;
  };
}
```

### 2. Create Typed Connection

```typescript
import { Pool } from "pg";
import { createTypedPg } from "postgres-query-builder-ts";
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
// Type: DatabaseSchema['users'][]

// Select specific columns
const names = await db.table("users").select("id", "name").execute();
// Type: { id: number, name: string }[]

// Add WHERE clause with various operators
const user = await db.table("users").where("id", "=", 123).first();
const adults = await db.table("users").where("age", ">=", 18).execute();
const activeUsers = await db
  .table("users")
  .where("is_active", "=", true)
  .execute();

// IN operator with arrays
const specificUsers = await db
  .table("users")
  .where("id", "IN", [1, 2, 3])
  .execute();

// String matching
const searchResults = await db
  .table("users")
  .where("name", "ILIKE", "%john%")
  .execute();

// Chain multiple conditions with AND
const filteredPosts = await db
  .table("posts")
  .where("user_id", "=", 1)
  .where("published", "=", true)
  .where("created_at", ">", new Date("2024-01-01"))
  .execute();

// OR conditions
const posts = await db
  .table("posts")
  .where("published", "=", true)
  .orWhere("user_id", "=", currentUserId)
  .execute();

// Ordering and pagination
const recentPosts = await db
  .table("posts")
  .where("published", "=", true)
  .orderBy("created_at", "DESC")
  .orderBy("title", "ASC") // Multiple order clauses
  .limit(10)
  .offset(20)
  .execute();

// Count records
const userCount = await db.table("users").where("is_active", "=", true).count();
```

## Full CRUD Operations

### INSERT

```typescript
// Insert single record
const [newUser] = await db.insert("users", {
  name: "John Doe",
  email: "john@example.com",
  age: 30,
  is_active: true,
});

// Insert multiple records
const newUsers = await db.insert("users", [
  { name: "Alice", email: "alice@example.com", age: 25, is_active: true },
  { name: "Bob", email: "bob@example.com", age: 35, is_active: false },
]);
```

### UPDATE

```typescript
// Update records
const updatedUsers = await db.update(
  "users",
  { is_active: false, age: 31 }, // SET clause
  { id: 123 } // WHERE clause
);
```

### DELETE

```typescript
// Delete records
const deletedUsers = await db.delete(
  "users",
  { is_active: false } // WHERE clause
);
```

## Advanced Features

### Transactions

```typescript
const result = await db.transaction(async (tx) => {
  // Create user
  const [user] = await tx.insert("users", {
    name: "John Doe",
    email: "john@example.com",
    age: 30,
    is_active: true,
  });

  // Create their first post
  const [post] = await tx.insert("posts", {
    user_id: user.id,
    title: "Hello World",
    content: "My first post!",
    published: true,
  });

  return { user, post };
});
// Automatically commits on success, rolls back on error
```

### Raw SQL Queries

```typescript
// For complex queries, use raw SQL with type safety
const analyticsData = await db.raw<{
  date: Date;
  user_count: number;
  post_count: number;
}>(
  `
  SELECT 
    DATE_TRUNC('day', created_at) as date,
    COUNT(DISTINCT user_id) as user_count,
    COUNT(*) as post_count
  FROM posts 
  WHERE created_at > $1
  GROUP BY DATE_TRUNC('day', created_at)
  ORDER BY date
`,
  [new Date("2024-01-01")]
);
```

### Connection Management

```typescript
// Access underlying pg Pool
const pool = db.getPool();

// Close connections
await db.close();
```

## Features

✅ **Type-safe column selection** - Only select columns that exist  
✅ **Type-safe WHERE clauses** - Column names and values are checked  
✅ **Full CRUD operations** - INSERT, UPDATE, DELETE with type safety  
✅ **Advanced WHERE conditions** - Support for IN, LIKE, ILIKE operators  
✅ **OR conditions** - Chain OR clauses with orWhere()  
✅ **Ordering and pagination** - ORDER BY, LIMIT, OFFSET support  
✅ **Aggregations** - COUNT, with more coming  
✅ **Transactions** - Built-in transaction support with automatic rollback  
✅ **Raw SQL escape hatch** - Use raw queries when needed  
✅ **Autocomplete everywhere** - Full IDE support  
✅ **Minimal overhead** - Thin wrapper around pg  
✅ **Multiple insert/update/delete** - Batch operations

## API Reference

### Query Building

#### `db.table(tableName)`

Start a query on a specific table.

#### `.select(...columns)`

Select specific columns (optional - defaults to `*`).

#### `.where(column, operator, value)`

Add a WHERE condition.

- **Operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`, `IN`
- **IN operator**: Use with arrays for `WHERE column IN (value1, value2, ...)`

#### `.orWhere(column, operator, value)`

Add an OR WHERE condition.

#### `.orderBy(column, direction?)`

Add ORDER BY clause. Direction: `"ASC"` (default) or `"DESC"`.

#### `.limit(count)`

Add LIMIT clause.

#### `.offset(count)`

Add OFFSET clause.

#### `.execute()`

Execute the query and return all results.

#### `.first()`

Execute the query and return the first result or `null`.

#### `.count()`

Get count of matching rows.

### CRUD Operations

#### `db.insert(tableName, data)`

Insert single record or array of records. Returns inserted records.

#### `db.update(tableName, data, whereClause)`

Update records matching WHERE clause. Returns updated records.

#### `db.delete(tableName, whereClause)`

Delete records matching WHERE clause. Returns deleted records.

### Advanced

#### `db.raw<T>(query, params?)`

Execute a raw SQL query with manual type annotation.

#### `db.transaction(callback)`

Execute operations in a transaction with automatic commit/rollback.

#### `db.getPool()`

Get the underlying pg Pool for advanced usage.

#### `db.close()`

Close the connection pool.

## Examples

### Complex Query Example

```typescript
// Find active users with recent posts, ordered by post count
const activeUsersWithPosts = await db.raw<{
  user_id: number;
  name: string;
  email: string;
  post_count: number;
  latest_post: Date;
}>(
  `
  SELECT 
    u.id as user_id,
    u.name,
    u.email,
    COUNT(p.id) as post_count,
    MAX(p.created_at) as latest_post
  FROM users u
  INNER JOIN posts p ON u.id = p.user_id
  WHERE u.is_active = $1 
    AND p.created_at > $2
    AND p.published = $3
  GROUP BY u.id, u.name, u.email
  HAVING COUNT(p.id) > $4
  ORDER BY post_count DESC, latest_post DESC
`,
  [true, new Date("2024-01-01"), true, 5]
);
```

### Migration from Raw pg

```typescript
// Before (raw pg)
const result = await pool.query(
  "SELECT id, name FROM users WHERE age > $1 AND is_active = $2",
  [18, true]
);
const users = result.rows; // any[]

// After (postgres-query-builder-ts)
const users = await db
  .table("users")
  .select("id", "name")
  .where("age", ">", 18)
  .where("is_active", "=", true)
  .execute(); // { id: number, name: string }[]
```

## Performance

This library adds minimal overhead to your queries:

- Compiles to clean, parameterized SQL
- No entity hydration or change tracking
- Direct access to the underlying pg Pool
- Bundle size: ~5KB

## TypeScript Support

Requires TypeScript 4.1+ for template literal types and advanced type inference.

## Contributing

PRs welcome! Please ensure:

- Type safety is maintained
- Tests pass
- Documentation is updated

## License

MIT
