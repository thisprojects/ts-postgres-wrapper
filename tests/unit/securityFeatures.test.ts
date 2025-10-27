import { TypedPg, DatabaseError, validateQueryComplexity, stripSqlComments, SecurityOptions } from "../../src";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
  };
  posts: {
    id: number;
    user_id: number;
    title: string;
  };
}

describe("Security Features", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
  });

  describe("stripSqlComments", () => {
    it("should strip block comments", () => {
      const sql = "SELECT * FROM users /* this is a comment */ WHERE id = 1";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users  WHERE id = 1");
    });

    it("should strip multiple block comments", () => {
      const sql = "/* start */ SELECT /* middle */ * FROM users /* end */";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT  * FROM users");
    });

    it("should strip line comments", () => {
      const sql = "SELECT * FROM users -- get all users\nWHERE id = 1";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users \nWHERE id = 1");
    });

    it("should preserve strings with comment-like content", () => {
      const sql = "SELECT 'http://example.com' FROM users";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'http://example.com' FROM users");
    });

    it("should handle simple block comments", () => {
      const sql = "SELECT /* comment */ * FROM users";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT  * FROM users");
    });

    it("should handle multiline block comments", () => {
      const sql = `SELECT * FROM users
/* This is a
   multiline
   comment */
WHERE id = 1`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("SELECT * FROM users");
      expect(stripped).toContain("WHERE id = 1");
      expect(stripped).not.toContain("multiline");
    });
  });

  describe("validateQueryComplexity", () => {
    it("should reject queries exceeding maximum length", () => {
      const security: SecurityOptions = {
        maxQueryLength: 50
      };
      const longQuery = "SELECT * FROM users WHERE name = 'very long name that exceeds the limit'";

      expect(() => {
        validateQueryComplexity(longQuery, security);
      }).toThrow("Query exceeds maximum length of 50 characters");
    });

    it("should accept queries within length limit", () => {
      const security: SecurityOptions = {
        maxQueryLength: 100
      };
      const shortQuery = "SELECT * FROM users";

      expect(() => {
        validateQueryComplexity(shortQuery, security);
      }).not.toThrow();
    });

    it("should reject queries with too many JOINs", () => {
      const security: SecurityOptions = {
        maxJoins: 2,
        allowComments: true
      };
      const query = `
        SELECT * FROM users
        INNER JOIN posts ON posts.user_id = users.id
        LEFT JOIN comments ON comments.post_id = posts.id
        RIGHT JOIN likes ON likes.comment_id = comments.id
      `;

      expect(() => {
        validateQueryComplexity(query, security);
      }).toThrow("Query has 3 JOINs, maximum allowed is 2");
    });

    it("should accept queries within JOIN limit", () => {
      const security: SecurityOptions = {
        maxJoins: 5,
        allowComments: true
      };
      const query = `
        SELECT * FROM users
        INNER JOIN posts ON posts.user_id = users.id
        LEFT JOIN comments ON comments.post_id = posts.id
      `;

      expect(() => {
        validateQueryComplexity(query, security);
      }).not.toThrow();
    });

    it("should reject queries with too many WHERE conditions", () => {
      const security: SecurityOptions = {
        maxWhereConditions: 3
      };
      const query = "SELECT * FROM users WHERE id = 1 AND name = 'test' AND email = 'test@example.com' AND age > 18 AND status = 'active'";

      expect(() => {
        validateQueryComplexity(query, security);
      }).toThrow("Query has 5 WHERE conditions, maximum allowed is 3");
    });

    it("should accept queries within WHERE condition limit", () => {
      const security: SecurityOptions = {
        maxWhereConditions: 10
      };
      const query = "SELECT * FROM users WHERE id = 1 AND name = 'test'";

      expect(() => {
        validateQueryComplexity(query, security);
      }).not.toThrow();
    });

    it("should reject queries with comments when not allowed", () => {
      const security: SecurityOptions = {
        allowComments: false
      };
      const query = "SELECT * FROM users /* comment */ WHERE id = 1";

      expect(() => {
        validateQueryComplexity(query, security);
      }).toThrow("SQL comments are not allowed");
    });

    it("should accept queries with comments when allowed", () => {
      const security: SecurityOptions = {
        allowComments: true
      };
      const query = "SELECT * FROM users /* comment */ WHERE id = 1";

      expect(() => {
        validateQueryComplexity(query, security);
      }).not.toThrow();
    });

    it("should detect line comments when not allowed", () => {
      const security: SecurityOptions = {
        allowComments: false
      };
      const query = "SELECT * FROM users -- comment\nWHERE id = 1";

      expect(() => {
        validateQueryComplexity(query, security);
      }).toThrow("SQL comments are not allowed");
    });

    it("should handle multiple validation rules", () => {
      const security: SecurityOptions = {
        maxQueryLength: 1000,
        maxJoins: 3,
        maxWhereConditions: 5,
        allowComments: false
      };
      const query = "SELECT * FROM users WHERE id = 1 AND name = 'test'";

      expect(() => {
        validateQueryComplexity(query, security);
      }).not.toThrow();
    });
  });

  describe("Query complexity validation integration", () => {
    it("should validate query complexity before execution", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxQueryLength: 30
        }
      });

      await expect(async () => {
        await db.raw("SELECT * FROM users WHERE id = 1 AND name = 'test' AND email = 'test@example.com'");
      }).rejects.toThrow("Query exceeds maximum length of 30 characters");
    });

    it("should execute queries that pass validation", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxQueryLength: 100,
          maxJoins: 5,
          maxWhereConditions: 10
        }
      });

      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);

      const result = await db.raw<TestSchema["users"]>("SELECT * FROM users WHERE id = 1");
      expect(result).toHaveLength(1);
    });
  });

  describe("Batch operation size limits", () => {
    it("should reject batchInsert exceeding maxBatchSize", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxBatchSize: 5
        }
      });

      const data = Array.from({ length: 10 }, (_, i) => ({
        name: `User ${i}`,
        email: `user${i}@example.com`
      }));

      await expect(async () => {
        await db.batchInsert("users", data);
      }).rejects.toThrow("Batch insert size 10 exceeds maximum allowed 5");
    });

    it("should accept batchInsert within maxBatchSize", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxBatchSize: 10
        }
      });

      const data = Array.from({ length: 5 }, (_, i) => ({
        id: i,
        name: `User ${i}`,
        email: `user${i}@example.com`
      }));

      mockPool.setMockResults([data]);

      const result = await db.batchInsert("users", data);
      expect(result).toHaveLength(5);
    });

    it("should reject batchUpdate exceeding maxBatchSize", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxBatchSize: 3
        }
      });

      const updates = Array.from({ length: 5 }, (_, i) => ({
        set: { name: `Updated ${i}` },
        where: { id: i }
      }));

      await expect(async () => {
        await db.batchUpdate("users", updates);
      }).rejects.toThrow("Batch update size 5 exceeds maximum allowed 3");
    });

    it("should reject batchDelete exceeding maxBatchSize", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxBatchSize: 2
        }
      });

      const conditions = Array.from({ length: 4 }, (_, i) => ({ id: i }));

      await expect(async () => {
        await db.batchDelete("users", conditions);
      }).rejects.toThrow("Batch delete size 4 exceeds maximum allowed 2");
    });
  });

  describe("Rate limiting for batch operations", () => {
    it("should enforce rate limiting on batchInsert", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          rateLimitBatch: true,
          batchRateLimit: 2 // 2 operations per second
        }
      });

      const data = [
        { id: 1, name: "User 1", email: "user1@example.com" },
        { id: 2, name: "User 2", email: "user2@example.com" }
      ];

      mockPool.setMockResults([[data[0]], [data[1]], [data[0]]]);

      const startTime = Date.now();

      // Execute 3 batch operations
      await db.batchInsert("users", [data[0]]);
      await db.batchInsert("users", [data[1]]);
      await db.batchInsert("users", [data[0]]); // This should be rate limited

      const duration = Date.now() - startTime;

      // Should take at least 1 second due to rate limiting (2 ops/sec, 3rd op needs to wait)
      expect(duration).toBeGreaterThanOrEqual(900); // Allow some margin
    }, 10000);

    it("should not rate limit when disabled", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          rateLimitBatch: false
        }
      });

      const data = [
        { id: 1, name: "User 1", email: "user1@example.com" },
        { id: 2, name: "User 2", email: "user2@example.com" }
      ];

      mockPool.setMockResults([[data[0]], [data[1]], [data[0]]]);

      const startTime = Date.now();

      // Execute 3 batch operations
      await db.batchInsert("users", [data[0]]);
      await db.batchInsert("users", [data[1]]);
      await db.batchInsert("users", [data[0]]);

      const duration = Date.now() - startTime;

      // Should be fast without rate limiting
      expect(duration).toBeLessThan(500);
    });

    it("should enforce rate limiting on batchUpdate", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          rateLimitBatch: true,
          batchRateLimit: 2
        }
      });

      mockPool.setMockResults([
        [{ id: 1, name: "Updated 1", email: "user1@example.com" }],
        [{ id: 2, name: "Updated 2", email: "user2@example.com" }],
        [{ id: 3, name: "Updated 3", email: "user3@example.com" }]
      ]);

      const startTime = Date.now();

      await db.batchUpdate("users", [{ set: { name: "Updated 1" }, where: { id: 1 } }]);
      await db.batchUpdate("users", [{ set: { name: "Updated 2" }, where: { id: 2 } }]);
      await db.batchUpdate("users", [{ set: { name: "Updated 3" }, where: { id: 3 } }]);

      const duration = Date.now() - startTime;
      expect(duration).toBeGreaterThanOrEqual(900);
    }, 10000);

    it("should enforce rate limiting on batchDelete", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          rateLimitBatch: true,
          batchRateLimit: 2
        }
      });

      mockPool.setMockResults([
        [{ id: 1, name: "User 1", email: "user1@example.com" }],
        [{ id: 2, name: "User 2", email: "user2@example.com" }],
        [{ id: 3, name: "User 3", email: "user3@example.com" }]
      ]);

      const startTime = Date.now();

      await db.batchDelete("users", [{ id: 1 }]);
      await db.batchDelete("users", [{ id: 2 }]);
      await db.batchDelete("users", [{ id: 3 }]);

      const duration = Date.now() - startTime;
      expect(duration).toBeGreaterThanOrEqual(900);
    }, 10000);
  });

  describe("Combined security features", () => {
    it("should enforce all security options together", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxQueryLength: 1000,
          maxJoins: 3,
          maxWhereConditions: 10,
          maxBatchSize: 100,
          allowComments: true,
          rateLimitBatch: true,
          batchRateLimit: 5
        }
      });

      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);

      // Should pass all validations
      const result = await db.raw<TestSchema["users"]>("SELECT * FROM users WHERE id = 1");
      expect(result).toHaveLength(1);
    });

    it("should reject queries that fail any security check", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        security: {
          maxQueryLength: 1000,
          maxJoins: 1,
          maxWhereConditions: 10,
          allowComments: true
        }
      });

      // This query has 2 JOINs, exceeding the limit
      const query = `SELECT * FROM users INNER JOIN posts ON posts.user_id = users.id LEFT JOIN comments ON comments.post_id = posts.id`;

      await expect(async () => {
        await db.raw(query);
      }).rejects.toThrow("Query has 2 JOINs, maximum allowed is 1");
    });
  });
});
