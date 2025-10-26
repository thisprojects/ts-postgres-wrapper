// integration-test.ts
import { createTypedPg } from "../src/index";

// Test schema matching the seed file
interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
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
}

// Database configuration - adjust as needed
const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  port: parseInt(process.env.DB_PORT || "5432"),
  database: process.env.DB_NAME || "postgres",
  user: process.env.DB_USER || "postgres",
  password: process.env.DB_PASSWORD || "password",
};

async function runIntegrationTests() {
  console.log("🚀 Running integration tests against real database...\n");

  const db = createTypedPg<TestSchema>(dbConfig);
  let testsPassed = 0;
  let testsFailed = 0;

  async function test(name: string, testFn: () => Promise<void>) {
    try {
      console.log(`🧪 Testing: ${name}`);
      await testFn();
      console.log(`✅ PASSED: ${name}`);
      testsPassed++;
    } catch (error) {
      console.error(`❌ FAILED: ${name}`);
      console.error(`   Error: ${error}`);
      testsFailed++;
    }
  }

  try {
    // Clean up any existing test data
    await db.raw("DELETE FROM comments WHERE true");
    await db.raw("DELETE FROM posts WHERE true");
    await db.raw("DELETE FROM users WHERE email LIKE '%@test.com'");

    // Test 1: Insert and retrieve user
    await test("Insert and retrieve user", async () => {
      const [user] = await db.insert("users", {
        name: "Test User",
        email: "test@test.com",
        active: true,
      });

      if (!user || !user.id) {
        throw new Error("Failed to insert user");
      }

      const retrievedUser = await db
        .table("users")
        .where("id", "=", user.id)
        .select("id", "name", "email", "active")
        .first();

      if (!retrievedUser || retrievedUser.email !== "test@test.com") {
        throw new Error("Failed to retrieve user");
      }
    });

    // Test 2: Complex query with joins using raw SQL
    await test("Complex query with raw SQL", async () => {
      // First ensure we have test data
      const [user] = await db.insert("users", {
        name: "Author User",
        email: "author@test.com",
        active: true,
      });

      const [post] = await db.insert("posts", {
        user_id: user.id,
        title: "Test Post",
        content: "Test content",
        published: true,
        tags: ["test", "integration"],
      });

      // Test raw query with joins
      const results = await db.raw<{
        user_name: string;
        post_title: string;
        post_count: number;
      }>(
        `SELECT 
          u.name as user_name, 
          p.title as post_title,
          COUNT(p.id) OVER (PARTITION BY u.id) as post_count
         FROM users u 
         JOIN posts p ON u.id = p.user_id 
         WHERE u.email = $1`,
        ["author@test.com"]
      );

      if (results.length === 0 || results[0].user_name !== "Author User") {
        throw new Error("Raw query with joins failed");
      }
    });

    // Test 3: Transaction rollback
    await test("Transaction rollback", async () => {
      const initialUserCount = await db.table("users").count();

      try {
        await db.transaction(async (tx) => {
          await tx.insert("users", {
            name: "Transaction Test",
            email: "rollback@test.com",
            active: true,
          });

          // Force an error to trigger rollback
          throw new Error("Intentional error for rollback test");
        });
      } catch (error) {
        // Expected error
      }

      const finalUserCount = await db.table("users").count();

      if (finalUserCount !== initialUserCount) {
        throw new Error("Transaction was not rolled back properly");
      }
    });

    // Test 4: Transaction commit
    await test("Transaction commit", async () => {
      const result = await db.transaction(async (tx) => {
        const [user] = await tx.insert("users", {
          name: "Transaction Success",
          email: "commit@test.com",
          active: true,
        });

        const [post] = await tx.insert("posts", {
          user_id: user.id,
          title: "Transaction Post",
          content: "Created in transaction",
          published: true,
          tags: ["transaction"],
        });

        return { user, post };
      });

      // Verify data was committed
      const user = await db
        .table("users")
        .where("email", "=", "commit@test.com")
        .first();

      if (!user) {
        throw new Error("Transaction was not committed properly");
      }
    });

    // Test 5: Array operations (PostgreSQL specific)
    await test("PostgreSQL array operations", async () => {
      // Insert post with tags
      const [user] = await db.insert("users", {
        name: "Array Test User",
        email: "array@test.com",
        active: true,
      });

      await db.insert("posts", {
        user_id: user.id,
        title: "Array Test Post",
        content: "Testing arrays",
        published: true,
        tags: ["typescript", "postgresql", "arrays"],
      });

      // Test array contains (ANY)
      const postsWithTypeScript = await db.raw<{
        id: number;
        title: string;
        tags: string[];
      }>(
        `SELECT id, title, tags 
         FROM posts 
         WHERE 'typescript' = ANY(tags) 
         AND user_id = $1`,
        [user.id]
      );

      if (postsWithTypeScript.length === 0) {
        throw new Error("Array ANY operation failed");
      }

      // Test array overlap (&&)
      const postsWithOverlap = await db.raw<{
        id: number;
        title: string;
        tags: string[];
      }>(
        `SELECT id, title, tags 
         FROM posts 
         WHERE tags && $1 
         AND user_id = $2`,
        [["typescript", "javascript"], user.id]
      );

      if (postsWithOverlap.length === 0) {
        throw new Error("Array overlap operation failed");
      }
    });

    // Test 6: Pagination
    await test("Pagination", async () => {
      // Ensure we have enough test data
      const users = [];
      for (let i = 0; i < 15; i++) {
        const [user] = await db.insert("users", {
          name: `Pagination User ${i}`,
          email: `pagination${i}@test.com`,
          active: true,
        });
        users.push(user);
      }

      // Test first page
      const page1 = await db
        .table("users")
        .where("email", "LIKE", "%@test.com")
        .orderBy("id", "ASC")
        .limit(10)
        .execute();

      // Test second page
      const page2 = await db
        .table("users")
        .where("email", "LIKE", "%@test.com")
        .orderBy("id", "ASC")
        .limit(10)
        .offset(10)
        .execute();

      if (page1.length !== 10) {
        throw new Error(`Expected 10 users on page 1, got ${page1.length}`);
      }

      if (page2.length < 5) {
        throw new Error(
          `Expected at least 5 users on page 2, got ${page2.length}`
        );
      }

      // Ensure no overlap
      const page1Ids = page1.map((u) => u.id);
      const page2Ids = page2.map((u) => u.id);
      const overlap = page1Ids.filter((id) => page2Ids.includes(id));

      if (overlap.length > 0) {
        throw new Error("Pages have overlapping results");
      }
    });

    // Test 7: Date range queries
    await test("Date range queries", async () => {
      const now = new Date();
      const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);

      // Create user for this test
      const [user] = await db.insert("users", {
        name: "Date Test User",
        email: "date@test.com",
        active: true,
      });

      // Test date range
      const recentUsers = await db
        .table("users")
        .where("created_at", ">=", yesterday)
        .where("created_at", "<=", tomorrow)
        .where("email", "=", "date@test.com")
        .execute();

      if (recentUsers.length === 0) {
        throw new Error("Date range query failed");
      }
    });

    // Test 8: Update and delete operations
    await test("Update and delete operations", async () => {
      // Insert test user
      const [user] = await db.insert("users", {
        name: "Update Test",
        email: "update@test.com",
        active: true,
      });

      // Update user
      const [updatedUser] = await db.update(
        "users",
        { name: "Updated Name", active: false },
        { id: user.id }
      );

      if (updatedUser.name !== "Updated Name" || updatedUser.active !== false) {
        throw new Error("Update operation failed");
      }

      // Delete user
      const [deletedUser] = await db.delete("users", { id: user.id });

      if (!deletedUser || deletedUser.id !== user.id) {
        throw new Error("Delete operation failed");
      }

      // Verify deletion
      const foundUser = await db
        .table("users")
        .where("id", "=", user.id)
        .first();

      if (foundUser !== null) {
        throw new Error("User was not actually deleted");
      }
    });

    console.log("\n" + "=".repeat(50));
    console.log(
      `📊 Integration Test Results: ${testsPassed} passed, ${testsFailed} failed`
    );

    if (testsFailed === 0) {
      console.log("🎉 All integration tests passed!");
    } else {
      console.log("💥 Some integration tests failed!");
    }
  } finally {
    // Clean up test data
    try {
      await db.raw("DELETE FROM comments WHERE true");
      await db.raw("DELETE FROM posts WHERE true");
      await db.raw("DELETE FROM users WHERE email LIKE '%@test.com'");
    } catch (error) {
      console.warn("Failed to clean up test data:", error);
    }

    await db.close();
  }
}

// Run integration tests if this file is executed directly
if (require.main === module) {
  runIntegrationTests().catch(console.error);
}

export { runIntegrationTests };
