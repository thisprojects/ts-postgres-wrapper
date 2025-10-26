// test.ts
import { Pool } from "pg";
import { createTypedPg, TypedPg, TypedQuery } from "../src/index";

// Test schema interface
interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
    age: number;
    active: boolean;
    created_at: Date;
  };
  posts: {
    id: number;
    user_id: number;
    title: string;
    content: string;
    published: boolean;
    tags: string[];
    created_at: Date;
  };
  comments: {
    id: number;
    post_id: number;
    user_id: number;
    content: string;
    created_at: Date;
  };
  products: {
    id: number;
    name: string;
    price: number;
    category_id: number;
    in_stock: boolean;
  };
}

// Mock pg Pool for testing
class MockPool {
  private mockResults: any[] = [];
  public queryLog: Array<{ text: string; values: any[] }> = [];

  constructor() {}

  setMockResults(results: any[]) {
    this.mockResults = results;
  }

  getMockResults() {
    return this.mockResults;
  }

  getQueryLog() {
    return this.queryLog;
  }

  clearQueryLog() {
    this.queryLog = [];
  }

  async query<T>(text: string, values: any[] = []): Promise<{ rows: T[] }> {
    this.queryLog.push({ text, values });
    return { rows: this.mockResults as T[] };
  }

  async connect() {
    return {
      query: this.query.bind(this),
      release: () => {},
    };
  }

  async end() {}
}

// Test runner
class TestRunner {
  private tests: Array<() => Promise<void>> = [];
  private passed = 0;
  private failed = 0;

  test(name: string, testFn: () => Promise<void>) {
    this.tests.push(async () => {
      try {
        console.log(`\n🧪 Testing: ${name}`);
        await testFn();
        console.log(`✅ PASSED: ${name}`);
        this.passed++;
      } catch (error) {
        console.error(`❌ FAILED: ${name}`);
        console.error(`   Error: ${(error as Error).message}`);
        this.failed++;
      }
    });
  }

  async run() {
    console.log("🚀 Running TypedPg tests...\n");

    for (const test of this.tests) {
      await test();
    }

    console.log("\n" + "=".repeat(50));
    console.log(
      `📊 Test Results: ${this.passed} passed, ${this.failed} failed`
    );

    if (this.failed === 0) {
      console.log("🎉 All tests passed!");
    } else {
      console.log("💥 Some tests failed!");
    }
  }
}

// Helper function to assert equality
function assert(condition: boolean, message: string) {
  if (!condition) {
    throw new Error(message);
  }
}

function assertDeepEqual(actual: any, expected: any, message: string) {
  const actualStr = JSON.stringify(actual);
  const expectedStr = JSON.stringify(expected);
  if (actualStr !== expectedStr) {
    throw new Error(
      `${message}\nExpected: ${expectedStr}\nActual: ${actualStr}`
    );
  }
}

// Create test instances
const mockPool = new MockPool();
const db = new TypedPg<TestSchema>(mockPool as any);
const runner = new TestRunner();

// ==========================================
// TypedQuery Tests
// ==========================================

runner.test("TypedQuery - Basic select query", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([
    {
      id: 1,
      name: "John",
      email: "john@example.com",
      age: 30,
      active: true,
      created_at: new Date(),
    },
  ]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  const results = await query.execute();

  const queryLog = mockPool.getQueryLog();
  assert(queryLog.length === 1, "Should execute one query");
  assert(
    queryLog[0].text === "SELECT * FROM users",
    "Should generate correct SELECT query"
  );
  assert(queryLog[0].values.length === 0, "Should have no parameters");
  assert(results.length === 1, "Should return mock results");
});

runner.test("TypedQuery - Select specific columns", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([{ id: 1, name: "John", email: "john@example.com" }]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  const results = await query.select("id", "name", "email").execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT id, name, email FROM users",
    "Should select specific columns"
  );
});

runner.test("TypedQuery - WHERE clause with equals", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("id", "=", 123).execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users WHERE id = $1",
    "Should generate WHERE clause"
  );
  assertDeepEqual(queryLog[0].values, [123], "Should pass correct parameters");
});

runner.test("TypedQuery - WHERE clause with comparison operators", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("age", ">", 18).where("age", "<=", 65).execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users WHERE age > $1 AND age <= $2",
    "Should chain WHERE clauses"
  );
  assertDeepEqual(queryLog[0].values, [18, 65], "Should pass all parameters");
});

runner.test("TypedQuery - WHERE IN clause", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("id", "IN", [1, 2, 3]).execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users WHERE id IN ($1, $2, $3)",
    "Should generate IN clause"
  );
  assertDeepEqual(queryLog[0].values, [1, 2, 3], "Should pass array values");
});

runner.test("TypedQuery - OR WHERE clause", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("active", "=", true).orWhere("age", "<", 18).execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users WHERE active = $1 OR age < $2",
    "Should generate OR clause"
  );
  assertDeepEqual(
    queryLog[0].values,
    [true, 18],
    "Should pass correct parameters"
  );
});

runner.test("TypedQuery - LIKE and ILIKE operators", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("name", "ILIKE", "%john%").execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users WHERE name ILIKE $1",
    "Should generate ILIKE clause"
  );
  assertDeepEqual(queryLog[0].values, ["%john%"], "Should pass LIKE pattern");
});

runner.test("TypedQuery - ORDER BY clause", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.orderBy("created_at", "DESC").orderBy("name", "ASC").execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text ===
      "SELECT * FROM users ORDER BY created_at DESC, name ASC",
    "Should generate ORDER BY clause"
  );
});

runner.test("TypedQuery - LIMIT and OFFSET", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.limit(10).offset(20).execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users LIMIT 10 OFFSET 20",
    "Should generate LIMIT and OFFSET"
  );
});

runner.test("TypedQuery - Complex chained query", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"posts", TestSchema["posts"]>(
    mockPool as any,
    "posts"
  );
  await query
    .where("published", "=", true)
    .where("user_id", "IN", [1, 2, 3])
    .orderBy("created_at", "DESC")
    .select("id", "title", "user_id")
    .limit(5)
    .execute();

  const queryLog = mockPool.getQueryLog();
  const expectedSql =
    "SELECT id, title, user_id FROM posts WHERE published = $1 AND user_id IN ($2, $3, $4) ORDER BY created_at DESC LIMIT 5";
  assert(
    queryLog[0].text === expectedSql,
    "Should generate complex chained query"
  );
  assertDeepEqual(
    queryLog[0].values,
    [true, 1, 2, 3],
    "Should pass all parameters in order"
  );
});

runner.test("TypedQuery - first() method", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([
    { id: 1, name: "John" },
    { id: 2, name: "Jane" },
  ]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  const result = await query.where("active", "=", true).first();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("LIMIT 1"),
    "Should add LIMIT 1 for first()"
  );
  assert(result !== null && result.id === 1, "Should return first result");
});

runner.test("TypedQuery - first() returns null when no results", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  const result = await query.where("id", "=", 999).first();

  assert(result === null, "Should return null when no results");
});

runner.test("TypedQuery - count() method", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([{ count: "42" }]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  const count = await query.where("active", "=", true).count();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text ===
      "SELECT COUNT(*) as count FROM users WHERE active = $1",
    "Should generate COUNT query"
  );
  assert(count === 42, "Should return parsed count");
});

// ==========================================
// TypedPg Tests
// ==========================================

runner.test("TypedPg - table() method returns TypedQuery", async () => {
  const query = db.table("users");
  assert(query instanceof TypedQuery, "Should return TypedQuery instance");
});

runner.test("TypedPg - insert single record", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([
    {
      id: 1,
      name: "John",
      email: "john@example.com",
      age: 30,
      active: true,
      created_at: new Date(),
    },
  ]);

  const result = await db.insert("users", {
    name: "John",
    email: "john@example.com",
    age: 30,
    active: true,
  });

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("INSERT INTO users"),
    "Should generate INSERT query"
  );
  assert(
    queryLog[0].text.includes("RETURNING *"),
    "Should include RETURNING clause"
  );
  assert(result.length === 1, "Should return inserted record");
});

runner.test("TypedPg - insert multiple records", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([
    { id: 1, name: "John", email: "john@example.com" },
    { id: 2, name: "Jane", email: "jane@example.com" },
  ]);

  const result = await db.insert("users", [
    { name: "John", email: "john@example.com" },
    { name: "Jane", email: "jane@example.com" },
  ]);

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("VALUES ($1, $2), ($3, $4)"),
    "Should generate multi-value INSERT"
  );
  assert(result.length === 2, "Should return all inserted records");
});

runner.test("TypedPg - insert empty array returns empty array", async () => {
  const result = await db.insert("users", []);
  assert(result.length === 0, "Should return empty array for empty input");
});

runner.test("TypedPg - update records", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([
    { id: 1, name: "John Updated", email: "john@example.com", active: false },
  ]);

  const result = await db.update(
    "users",
    { name: "John Updated", active: false },
    { id: 1 }
  );

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("UPDATE users SET"),
    "Should generate UPDATE query"
  );
  assert(
    queryLog[0].text.includes("WHERE id = $"),
    "Should include WHERE clause"
  );
  assert(
    queryLog[0].text.includes("RETURNING *"),
    "Should include RETURNING clause"
  );
  assert(result.length === 1, "Should return updated record");
});

runner.test("TypedPg - delete records", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([{ id: 1, name: "John", email: "john@example.com" }]);

  const result = await db.delete("users", { id: 1 });

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("DELETE FROM users"),
    "Should generate DELETE query"
  );
  assert(
    queryLog[0].text.includes("WHERE id = $1"),
    "Should include WHERE clause"
  );
  assert(
    queryLog[0].text.includes("RETURNING *"),
    "Should include RETURNING clause"
  );
  assert(result.length === 1, "Should return deleted record");
});

runner.test("TypedPg - raw query", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([{ user_count: 5, avg_age: 30.5 }]);

  const result = await db.raw<{ user_count: number; avg_age: number }>(
    "SELECT COUNT(*) as user_count, AVG(age) as avg_age FROM users WHERE active = $1",
    [true]
  );

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("SELECT COUNT(*) as user_count"),
    "Should execute raw SQL"
  );
  assertDeepEqual(queryLog[0].values, [true], "Should pass parameters");
  assert(result.length === 1, "Should return raw query results");
});

runner.test("TypedPg - getPool() returns underlying pool", async () => {
  const pool = db.getPool();
  assert(pool instanceof MockPool, "Should return underlying pool instance");
});

// ==========================================
// createTypedPg Function Tests
// ==========================================

runner.test("createTypedPg - creates TypedPg with existing Pool", async () => {
  const pool = new Pool();
  const typedPg = createTypedPg<TestSchema>(pool);
  assert(typedPg instanceof TypedPg, "Should create TypedPg instance");
  assert(typedPg.getPool() === pool, "Should use provided pool");
  await pool.end();
});

runner.test(
  "createTypedPg - creates TypedPg with connection string",
  async () => {
    const typedPg = createTypedPg<TestSchema>(
      "postgresql://user:pass@localhost:5432/db"
    );
    assert(
      typedPg instanceof TypedPg,
      "Should create TypedPg instance with connection string"
    );
    await typedPg.close();
  }
);

runner.test("createTypedPg - creates TypedPg with config object", async () => {
  const typedPg = createTypedPg<TestSchema>({
    host: "localhost",
    port: 5432,
    database: "test",
    user: "testuser",
    password: "testpass",
  });
  assert(
    typedPg instanceof TypedPg,
    "Should create TypedPg instance with config"
  );
  await typedPg.close();
});

// ==========================================
// Transaction Tests (Mock Implementation)
// ==========================================

runner.test("TypedPg - transaction success", async () => {
  // Mock the transaction behavior
  let transactionStarted = false;
  let transactionCommitted = false;

  const mockClient = {
    query: async (sql: string) => {
      if (sql === "BEGIN") transactionStarted = true;
      if (sql === "COMMIT") transactionCommitted = true;
      return { rows: [] };
    },
    release: () => {},
  };

  // Override connect method for this test
  const originalConnect = mockPool.connect;
  mockPool.connect = async () => mockClient;

  try {
    await db.transaction(async (tx) => {
      assert(
        tx instanceof TypedPg,
        "Should provide TypedPg instance in callback"
      );
      return "success";
    });

    assert(transactionStarted, "Should start transaction");
    assert(transactionCommitted, "Should commit transaction");
  } finally {
    mockPool.connect = originalConnect;
  }
});

// ==========================================
// Error Handling Tests
// ==========================================

runner.test("TypedQuery - handles query errors", async () => {
  const errorPool = {
    query: async () => {
      throw new Error("Database connection failed");
    },
  };

  const query = new TypedQuery<"users", TestSchema["users"]>(
    errorPool as any,
    "users"
  );

  try {
    await query.execute();
    assert(false, "Should throw error");
  } catch (error) {
    assert(
      (error as Error).message.includes("Database connection failed"),
      "Should propagate database errors"
    );
  }
});

// ==========================================
// Type Safety Tests (Compile-time, documented here)
// ==========================================

runner.test("Type Safety - Column names are type-checked", async () => {
  // These would be compile-time errors in actual TypeScript:

  // ❌ This would fail: db.table("users").where("invalid_column", "=", "value")
  // ❌ This would fail: db.table("users").select("invalid_column")
  // ❌ This would fail: db.table("users").orderBy("invalid_column", "ASC")
  // ❌ This would fail: db.insert("users", { invalid_field: "value" })

  // ✅ These should work:
  const validQuery = db.table("users").where("name", "=", "John");
  const validSelect = db.table("users").select("id", "name", "email");
  const validOrder = db.table("users").orderBy("created_at", "DESC");

  assert(validQuery instanceof TypedQuery, "Should accept valid column names");
  // Note: Actual type checking happens at compile time with TypeScript
});

// ==========================================
// Integration-style Tests
// ==========================================

runner.test("Integration - Complex workflow", async () => {
  mockPool.clearQueryLog();

  // Setup mock responses for a complex workflow
  const mockResponses = [
    [
      {
        id: 1,
        name: "John",
        email: "john@example.com",
        age: 30,
        active: true,
        created_at: new Date(),
      },
    ], // insert user
    [
      {
        id: 1,
        user_id: 1,
        title: "Test Post",
        content: "Content",
        published: true,
        tags: ["test"],
        created_at: new Date(),
      },
    ], // insert post
    [{ id: 1, name: "John", email: "john@example.com" }], // select user
    [{ count: "1" }], // count posts
  ];

  let responseIndex = 0;
  const originalQuery = mockPool.query;
  mockPool.query = async function <T>(
    text: string,
    values: any[] = []
  ): Promise<{ rows: T[] }> {
    const result = mockResponses[responseIndex++] || [];
    this.queryLog.push({ text, values });
    return { rows: result as T[] };
  };

  try {
    // 1. Insert a user
    const [user] = await db.insert("users", {
      name: "John",
      email: "john@example.com",
      age: 30,
      active: true,
    });

    // 2. Insert a post for that user
    const [post] = await db.insert("posts", {
      user_id: user.id,
      title: "Test Post",
      content: "Content",
      published: true,
      tags: ["test"],
    });

    // 3. Find the user
    const foundUser = await db
      .table("users")
      .where("id", "=", user.id)
      .select("id", "name", "email")
      .first();

    // 4. Count their posts
    const postCount = await db
      .table("posts")
      .where("user_id", "=", user.id)
      .count();

    assert(foundUser?.name === "John", "Should find inserted user");
    assert(postCount === 1, "Should count user's posts");

    const queryLog = mockPool.getQueryLog();
    assert(queryLog.length === 4, "Should execute all queries in workflow");
  } finally {
    mockPool.query = originalQuery;
  }
});

// ==========================================
// Edge Cases Tests
// ==========================================

runner.test("Edge Cases - Empty WHERE IN array", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("id", "IN", []).execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT * FROM users WHERE id IN ()",
    "Should handle empty IN array"
  );
});

runner.test("Edge Cases - Special characters in values", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query.where("name", "=", "O'Connor").execute();

  const queryLog = mockPool.getQueryLog();
  assertDeepEqual(
    queryLog[0].values,
    ["O'Connor"],
    "Should handle special characters in parameters"
  );
});

runner.test("Edge Cases - Multiple select calls", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  const finalQuery = query
    .select("id", "name", "email", "age")
    .select("email", "age"); // Second select should override

  await finalQuery.execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text === "SELECT email, age FROM users",
    "Should use most recent select"
  );
});

// ==========================================
// JOIN Tests
// ==========================================

runner.test("TypedQuery - INNER JOIN basic functionality", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([
    { id: 1, name: "John", title: "Hello World", content: "My first post" },
  ]);

  await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .select("users.id", "users.name", "posts.title", "posts.content")
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("INNER JOIN posts ON users.id = posts.user_id"),
    "Should generate INNER JOIN clause"
  );
  assert(
    queryLog[0].text.includes(
      "SELECT users.id, users.name, posts.title, posts.content"
    ),
    "Should select qualified columns"
  );
});

runner.test("TypedQuery - LEFT JOIN functionality", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .leftJoin("posts", "users.id", "posts.user_id")
    .where("users.active", "=", true)
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("LEFT JOIN posts ON users.id = posts.user_id"),
    "Should generate LEFT JOIN clause"
  );
  assert(
    queryLog[0].text.includes("WHERE users.active = $1"),
    "Should qualify column names in WHERE with JOINs"
  );
  assertDeepEqual(queryLog[0].values, [true], "Should pass correct parameters");
});

runner.test("TypedQuery - RIGHT JOIN functionality", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("posts")
    .rightJoin("users", "posts.user_id", "users.id")
    .where("users.active", "=", true)
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("RIGHT JOIN users ON posts.user_id = users.id"),
    "Should generate RIGHT JOIN clause"
  );
});

runner.test("TypedQuery - FULL OUTER JOIN functionality", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .fullJoin("posts", "users.id", "posts.user_id")
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("FULL JOIN posts ON users.id = posts.user_id"),
    "Should generate FULL OUTER JOIN clause"
  );
});

runner.test("TypedQuery - Multiple JOINs", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .leftJoin("comments", "posts.id", "comments.post_id")
    .select("users.name", "posts.title", "comments.content")
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("INNER JOIN posts ON users.id = posts.user_id"),
    "Should include first JOIN"
  );
  assert(
    queryLog[0].text.includes(
      "LEFT JOIN comments ON posts.id = comments.post_id"
    ),
    "Should include second JOIN"
  );
  assert(
    queryLog[0].text.includes(
      "SELECT users.name, posts.title, comments.content"
    ),
    "Should select from all joined tables"
  );
});

runner.test("TypedQuery - JOIN with table aliases", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users", "u")
    .innerJoin("posts", "u.id", "posts.user_id")
    .select("u.name", "posts.title")
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("FROM users AS u"),
    "Should use table alias in FROM clause"
  );
  assert(
    queryLog[0].text.includes("INNER JOIN posts ON u.id = posts.user_id"),
    "Should use alias in JOIN condition"
  );
  assert(
    queryLog[0].text.includes("SELECT u.name, posts.title"),
    "Should use alias in SELECT"
  );
});

runner.test("TypedQuery - JOIN with WHERE and ORDER BY", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("users.active", "=", true)
    .where("posts.published", "=", true)
    .orderBy("users.name", "ASC")
    .orderBy("posts.created_at", "DESC")
    .limit(10)
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes(
      "WHERE users.active = $1 AND posts.published = $2"
    ),
    "Should qualify WHERE columns with JOINs"
  );
  assert(
    queryLog[0].text.includes("ORDER BY users.name ASC, posts.created_at DESC"),
    "Should qualify ORDER BY columns with JOINs"
  );
  assert(queryLog[0].text.includes("LIMIT 10"), "Should include LIMIT clause");
  assertDeepEqual(
    queryLog[0].values,
    [true, true],
    "Should pass correct parameters"
  );
});

runner.test("TypedQuery - JOIN with OR WHERE", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("users.active", "=", true)
    .orWhere("posts.published", "=", false)
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes(
      "WHERE users.active = $1 OR posts.published = $2"
    ),
    "Should generate OR condition with qualified columns"
  );
  assertDeepEqual(
    queryLog[0].values,
    [true, false],
    "Should pass correct parameters"
  );
});

runner.test("TypedQuery - JOIN with IN operator", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("users.id", "IN", [1, 2, 3])
    .where("posts.tags", "IN", ["tech", "programming"])
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes(
      "WHERE users.id IN ($1, $2, $3) AND posts.tags IN ($4, $5)"
    ),
    "Should generate IN clauses with correct parameters"
  );
  assertDeepEqual(
    queryLog[0].values,
    [1, 2, 3, "tech", "programming"],
    "Should pass all IN parameters"
  );
});

runner.test("TypedQuery - JOIN with COUNT", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([{ count: "42" }]);

  const count = await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("posts.published", "=", true)
    .count();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes(
      "SELECT COUNT(*) as count FROM users INNER JOIN posts ON users.id = posts.user_id"
    ),
    "Should generate COUNT with JOIN"
  );
  assert(count === 42, "Should return parsed count");
});

runner.test("TypedQuery - JOIN with first() method", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([{ name: "John", title: "Hello World" }]);

  const result = await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("users.active", "=", true)
    .first();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("LIMIT 1"),
    "Should add LIMIT 1 for first() method"
  );
  assert(result && result.name === "John", "Should return first result");
});

runner.test("TypedQuery - toSQL() method with JOINs", async () => {
  const query = db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("users.active", "=", true)
    .where("posts.published", "=", true)
    .select("users.name", "posts.title")
    .orderBy("posts.created_at", "DESC")
    .limit(5);

  const { query: sql, params } = query.toSQL();

  assert(
    sql.includes(
      "SELECT users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id"
    ),
    "toSQL should return correct SQL structure"
  );
  assert(
    sql.includes("WHERE users.active = $1 AND posts.published = $2"),
    "toSQL should include WHERE clause"
  );
  assert(
    sql.includes("ORDER BY posts.created_at DESC"),
    "toSQL should include ORDER BY clause"
  );
  assert(sql.includes("LIMIT 5"), "toSQL should include LIMIT clause");
  assertDeepEqual(
    params,
    [true, true],
    "toSQL should return correct parameters"
  );
});

runner.test("TypedQuery - Complex multi-table JOIN scenario", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users", "u")
    .innerJoin("posts", "u.id", "posts.user_id")
    .leftJoin("comments", "posts.id", "comments.post_id")
    .where("u.active", "=", true)
    .where("posts.published", "=", true)
    .where("posts.created_at", ">", new Date("2024-01-01"))
    .orderBy("posts.created_at", "DESC")
    .orderBy("comments.created_at", "ASC")
    .limit(20)
    .offset(10)
    .execute();

  const queryLog = mockPool.getQueryLog();

  // Check all expected JOINs
  assert(
    queryLog[0].text.includes("FROM users AS u"),
    "Should use table alias"
  );
  assert(
    queryLog[0].text.includes("INNER JOIN posts ON u.id = posts.user_id"),
    "Should include INNER JOIN"
  );
  assert(
    queryLog[0].text.includes(
      "LEFT JOIN comments ON posts.id = comments.post_id"
    ),
    "Should include LEFT JOIN"
  );

  // Check WHERE conditions
  assert(
    queryLog[0].text.includes(
      "WHERE u.active = $1 AND posts.published = $2 AND posts.created_at > $3"
    ),
    "Should include all WHERE conditions with qualified columns"
  );

  // Check ORDER BY
  assert(
    queryLog[0].text.includes(
      "ORDER BY posts.created_at DESC, comments.created_at ASC"
    ),
    "Should include multiple ORDER BY clauses"
  );

  // Check LIMIT and OFFSET
  assert(queryLog[0].text.includes("LIMIT 20"), "Should include LIMIT");
  assert(queryLog[0].text.includes("OFFSET 10"), "Should include OFFSET");

  // Check parameter count
  assert(
    queryLog[0].values.length === 3,
    "Should have correct number of parameters"
  );
});

runner.test(
  "TypedQuery - Backward compatibility with non-JOIN queries",
  async () => {
    mockPool.clearQueryLog();
    mockPool.setMockResults([]);

    // Test that queries without JOINs still work exactly as before
    const query = new TypedQuery<"users", TestSchema["users"]>(
      mockPool as any,
      "users"
    );
    await query
      .where("id", "=", 123)
      .where("active", "=", true)
      .orderBy("name", "ASC")
      .select("id", "name", "email")
      .execute();

    const queryLog = mockPool.getQueryLog();
    assert(
      queryLog[0].text ===
        "SELECT id, name, email FROM users WHERE id = $1 AND active = $2 ORDER BY name ASC",
      "Should generate unqualified columns for non-JOIN queries (backward compatibility)"
    );
    assertDeepEqual(
      queryLog[0].values,
      [123, true],
      "Should pass correct parameters"
    );
  }
);

runner.test("TypedQuery - Already qualified column names", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  await db
    .table("users")
    .innerJoin("posts", "users.id", "posts.user_id")
    .where("users.active", "=", true) // Already qualified
    .where("posts.published", "=", true) // Already qualified
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes(
      "WHERE users.active = $1 AND posts.published = $2"
    ),
    "Should pass through already qualified column names unchanged"
  );
});

// ==========================================
// Performance Tests (Basic)
// ==========================================

runner.test("Performance - Parameter numbering", async () => {
  mockPool.clearQueryLog();
  mockPool.setMockResults([]);

  const query = new TypedQuery<"users", TestSchema["users"]>(
    mockPool as any,
    "users"
  );
  await query
    .where("name", "=", "John")
    .where("age", ">", 18)
    .where("email", "LIKE", "%@example.com")
    .where("active", "=", true)
    .execute();

  const queryLog = mockPool.getQueryLog();
  assert(
    queryLog[0].text.includes("$1") &&
      queryLog[0].text.includes("$2") &&
      queryLog[0].text.includes("$3") &&
      queryLog[0].text.includes("$4"),
    "Should number parameters correctly"
  );
  assert(
    queryLog[0].values.length === 4,
    "Should have correct number of parameters"
  );
});

// ==========================================
// Run All Tests
// ==========================================

async function runTests() {
  await runner.run();
}

// Export for external use
export { runTests, TestRunner, MockPool, assert, assertDeepEqual };

// Run tests if this file is executed directly
if (require.main === module) {
  runTests().catch(console.error);
}
