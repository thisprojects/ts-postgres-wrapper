import { TypedQuery } from "../../src/index";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
  };
}

describe("SQL Injection Prevention", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
    mockPool.setMockResults([]);
  });

  describe("String-based AS clause injection prevention", () => {
    it("should not parse AS from string columns", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Attempt to inject SQL via "AS" in column string
      // This should be rejected because it contains SQL injection patterns
      await expect(async () => {
        await query
          .select("id", "name; DROP TABLE users; -- AS malicious")
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should throw error when column name contains SQL injection patterns", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // These should all throw errors because they contain injection patterns
      await expect(async () => {
        await query.select("id; DROP TABLE users; --").execute();
      }).rejects.toThrow("Invalid SQL identifier");

      await expect(async () => {
        await query.select("id' OR '1'='1").execute();
      }).rejects.toThrow("Invalid SQL identifier");

      await expect(async () => {
        await query.select("id/* comment */").execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should require object syntax for aliases", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Safe way: using object syntax
      await query
        .select("id", { column: "name", as: "user_name" })
        .execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT id, name AS user_name FROM users");
    });

    it("should sanitize alias names in object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Attempt to inject via alias field
      await expect(async () => {
        await query
          .select("id", { column: "name", as: "alias; DROP TABLE users; --" })
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should treat string with 'AS' as a single column name with space", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // String with space and AS should be treated as single identifier
      // Spaces require quoting, which our implementation allows
      await query
        .select("column AS something")
        .execute();

      const executedQuery = mockPool.getLastQuery();

      // Should be quoted as a single identifier with the space
      expect(executedQuery.text).toContain('"column AS something"');
    });
  });

  describe("Complex injection attempts", () => {
    it("should prevent injection in qualified column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Try to inject via qualified column syntax
      await expect(async () => {
        await query.select("users.id; DROP TABLE users; --").execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should prevent injection in column part of object alias", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Column part still gets sanitized even in object syntax
      // Note: spaces are invalid, so this will be rejected
      await expect(async () => {
        await query
          .select({ column: "name; DROP TABLE", as: "safe_alias" })
          .execute();
      }).rejects.toThrow(); // Will throw due to invalid identifier (space or semicolon)
    });

    it("should handle legitimate special characters in column names", async () => {
      // Test that we can still use legitimately special column names with object syntax
      interface SpecialSchema {
        users: {
          id: number;
          "user-name": string;
          "名前": string;
        };
      }

      const query = new TypedQuery<"users", SpecialSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .select("id", { column: "user-name", as: "userName" })
        .execute();

      const executedQuery = mockPool.getLastQuery();

      // Hyphenated column names are quoted
      expect(executedQuery.text).toContain('"user-name"');
      expect(executedQuery.text).toContain("AS userName");
    });
  });

  describe("Comparison with safe object syntax", () => {
    it("should generate identical safe queries with object syntax", async () => {
      const query1 = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query1
        .select("id", { column: "name", as: "user_name" }, { column: "email", as: "user_email" })
        .execute();

      const safeQuery = mockPool.getLastQuery();

      expect(safeQuery.text).toBe(
        "SELECT id, name AS user_name, email AS user_email FROM users"
      );

      // Verify no dangerous patterns exist
      expect(safeQuery.text).not.toMatch(/;/);
      expect(safeQuery.text).not.toMatch(/--/);
      expect(safeQuery.text).not.toMatch(/\/\*/);
      expect(safeQuery.text).not.toMatch(/DROP/i);
    });
  });

  describe("orderBy injection prevention", () => {
    it("should sanitize orderBy column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .orderBy("id; DROP TABLE users; --")
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should allow safe orderBy column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .select("id", "name")
        .orderBy("name", "DESC")
        .execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT id, name FROM users ORDER BY name DESC");
    });
  });

  describe("WHERE clause injection prevention", () => {
    it("should parameterize WHERE values to prevent injection", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Malicious value should be safely parameterized
      await query
        .where("name", "=", "'; DROP TABLE users; --")
        .execute();

      const executedQuery = mockPool.getLastQuery();

      // Should use parameterized query
      expect(executedQuery.text).toBe("SELECT * FROM users WHERE name = $1");
      expect(executedQuery.values).toEqual(["'; DROP TABLE users; --"]);

      // SQL injection should be in parameters, not in query text
      expect(executedQuery.text).not.toContain("DROP TABLE");
    });

    it("should sanitize WHERE column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .where("id; DROP TABLE users; --" as any, "=", 1)
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("Aggregate function injection prevention", () => {
    it("should sanitize column names in aggregate methods", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.min("id; DROP TABLE users; --" as any).execute();
      }).rejects.toThrow("Invalid SQL identifier");

      await expect(async () => {
        await query.max("id; DROP TABLE users; --" as any).execute();
      }).rejects.toThrow("Invalid SQL identifier");

      await expect(async () => {
        await query.sum("id; DROP TABLE users; --" as any).execute();
      }).rejects.toThrow("Invalid SQL identifier");

      await expect(async () => {
        await query.avg("id; DROP TABLE users; --" as any).execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });
});
