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

  describe("Reserved keyword handling", () => {
    it("should quote reserved keywords as column names", async () => {
      interface ReservedSchema {
        orders: {
          id: number;
          user: string;  // 'user' is a reserved keyword
          select: string; // 'select' is a reserved keyword
          order: string;  // 'order' is a reserved keyword
        };
      }

      const query = new TypedQuery<"orders", ReservedSchema["orders"]>(
        mockPool as any,
        "orders"
      );

      await query.select("user", "select", "order").execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain('"user"');
      expect(executedQuery.text).toContain('"select"');
      expect(executedQuery.text).toContain('"order"');
    });

    it("should quote reserved keywords in qualified names", async () => {
      interface ReservedSchema {
        orders: {
          id: number;
          user: string;
        };
      }

      const query = new TypedQuery<"orders", ReservedSchema["orders"]>(
        mockPool as any,
        "orders"
      );

      await query.select("orders.user", "orders.id").execute();

      const executedQuery = mockPool.getLastQuery();
      // 'user' is reserved, should be quoted
      expect(executedQuery.text).toContain('orders."user"');
    });

    it("should handle reserved keywords in WHERE clauses", async () => {
      interface ReservedSchema {
        products: {
          id: number;
          key: string;  // 'key' is a reserved keyword
          value: number;
        };
      }

      const query = new TypedQuery<"products", ReservedSchema["products"]>(
        mockPool as any,
        "products"
      );

      await query.where("key", "=", "test").execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain('"key"');
    });

    it("should handle reserved keywords in ORDER BY", async () => {
      interface ReservedSchema {
        items: {
          id: number;
          order: number;  // 'order' is a reserved keyword
        };
      }

      const query = new TypedQuery<"items", ReservedSchema["items"]>(
        mockPool as any,
        "items"
      );

      await query.orderBy("order", "ASC").execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain('"order"');
    });
  });

  describe("Quoted identifier handling", () => {
    it("should handle pre-quoted identifiers safely", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.select('"name"', '"email"').execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe('SELECT "name", "email" FROM users');
    });

    it("should reject quoted identifiers with SQL injection", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select('"name"; DROP TABLE users; --"').execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should properly escape internal quotes in quoted identifiers", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Identifier with internal quote should be properly escaped
      await query.select('"user""name"').execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain('"user""name"');
    });

    it("should handle special characters by quoting", async () => {
      interface SpecialSchema {
        data: {
          id: number;
          "column-with-hyphen": string;
          "column with space": string;
          "列名": string; // Unicode column name
        };
      }

      const query = new TypedQuery<"data", SpecialSchema["data"]>(
        mockPool as any,
        "data"
      );

      await query.select("column-with-hyphen", "column with space", "列名").execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain('"column-with-hyphen"');
      expect(executedQuery.text).toContain('"column with space"');
      expect(executedQuery.text).toContain('"列名"');
    });

    it("should reject quoted identifiers with backslashes", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select('"name\\escape"').execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should reject quoted identifiers with comment patterns", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select('"name--comment"').execute();
      }).rejects.toThrow("Invalid SQL identifier");

      await expect(async () => {
        await query.select('"name/*comment*/"').execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });
});
