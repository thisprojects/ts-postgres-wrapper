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
      }).rejects.toThrow("Invalid column name");
    });

    it("should throw error when column name contains SQL injection patterns", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // These should all throw errors because they contain injection patterns
      await expect(async () => {
        await query.select("id; DROP TABLE users; --").execute();
      }).rejects.toThrow("Invalid column name");

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
      }).rejects.toThrow("Invalid column name");
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
          select: string;  // 'select' is a reserved keyword
          value: number;
        };
      }

      const query = new TypedQuery<"products", ReservedSchema["products"]>(
        mockPool as any,
        "products"
      );

      await query.where("select", "=", "test").execute();

      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain('"select"');
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
      }).rejects.toThrow("Invalid column name");
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
      }).rejects.toThrow("Invalid column name");

      await expect(async () => {
        await query.select('"name/*comment*/"').execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("select() method SQL injection prevention", () => {
    it("should reject column names with semicolons", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("id; DROP TABLE users--").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject column names with DROP keyword", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("id DROP TABLE users").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject column names with DELETE keyword", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("id DELETE FROM users").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject column names with UNION keyword", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("id UNION SELECT password").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject column names with SQL comment markers", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("id-- comment").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject function calls in string columns and suggest expr()", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("COUNT(*)").execute();
      }).rejects.toThrow(/expr\(\) helper/);

      await expect(async () => {
        await query.select("SUM(id)").execute();
      }).rejects.toThrow(/expr\(\) helper/);
    });

    it("should allow safe column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.select("id", "name", "email").execute();
      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT id, name, email FROM users");
    });

    it("should allow qualified column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.select("users.id", "users.name").execute();
      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT users.id, users.name FROM users");
    });

    it("should reject SQL injection in object syntax column names", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Object syntax validates the column name before processing
      await expect(async () => {
        await query.select({ column: "id; DROP TABLE users--", as: "user_id" }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);
    });

    it("should allow safe column names in object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Simple column names work
      await query.select({ column: "id", as: "user_id" }, { column: "name", as: "user_name" }).execute();
      let executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT id AS user_id, name AS user_name FROM users");
    });
  });

  describe("Object syntax SQL injection prevention (HIGH SEVERITY)", () => {
    it("should reject DROP TABLE injection via object syntax column", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Attempt to inject DROP TABLE via object column
      await expect(async () => {
        await query.select({ column: "id); DROP TABLE users; --", as: "evil" }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);
    });

    it("should reject semicolon injection via object syntax column", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select({ column: "id; DELETE FROM sessions--", as: "malicious" }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);
    });

    it("should reject UNION injection via object syntax column", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select({ column: "id UNION SELECT password FROM admin", as: "data" }).execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject comment marker injection via object syntax column", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // SQL comment markers should be rejected
      await expect(async () => {
        await query.select({ column: "id-- inject", as: "result" }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);

      await expect(async () => {
        await query.select({ column: "id/* comment */", as: "result" }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);
    });

    it("should reject function calls in object syntax without expr() marker", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Function calls should require expr() helper
      await expect(async () => {
        await query.select({ column: "COUNT(*)", as: "total" }).execute();
      }).rejects.toThrow(/expr\(\) helper/);

      await expect(async () => {
        await query.select({ column: "MAX(id)", as: "max_id" }).execute();
      }).rejects.toThrow(/expr\(\) helper/);

      await expect(async () => {
        await query.select({ column: "SUM(amount)", as: "total_amount" }).execute();
      }).rejects.toThrow(/expr\(\) helper/);
    });

    it("should allow expressions with expr() helper in select()", async () => {
      const { expr } = await import("../../src/types");

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // With expr() helper, expressions should work
      await query.select(expr("COUNT(*)", "total")).execute();
      let executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT COUNT(*) AS total FROM users");

      mockPool.clearQueryLog();
      await query.select(expr("MAX(id)", "max_id"), expr("MIN(id)", "min_id")).execute();
      executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT MAX(id) AS max_id, MIN(id) AS min_id FROM users");
    });

    it("should reject DELETE keyword injection via object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select({ column: "id DELETE FROM users WHERE 1=1", as: "bad" }).execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject INSERT keyword injection via object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select({ column: "id INSERT INTO admin VALUES('hacker')", as: "bad" }).execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should reject UPDATE keyword injection via object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select({ column: "id UPDATE users SET role='admin'", as: "bad" }).execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should enforce validation on object syntax even with complex patterns", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Multiple injection attempts combined
      await expect(async () => {
        await query.select({
          column: "id); DROP TABLE users; DELETE FROM sessions; --",
          as: "totally_safe"
        }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);
    });

    it("should validate both column and alias in object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Valid column but malicious alias
      await expect(async () => {
        await query.select({ column: "id", as: "name; DROP TABLE users--" }).execute();
      }).rejects.toThrow("Invalid SQL identifier");

      // Malicious column but valid alias
      await expect(async () => {
        await query.select({ column: "id; DROP TABLE users--", as: "user_id" }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);

      // Both malicious
      await expect(async () => {
        await query.select({
          column: "id; DROP TABLE users--",
          as: "name; DROP TABLE sessions--"
        }).execute();
      }).rejects.toThrow(/Invalid column name|Invalid SQL identifier/);
    });

    it("should allow simple qualified column names in object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Qualified column names should work
      await query.select({ column: "users.id", as: "user_id" }).execute();
      const executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toBe("SELECT users.id AS user_id FROM users");
    });

    it("should protect against subquery injection in object syntax", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Subqueries without proper markers should be restricted
      await expect(async () => {
        await query.select({
          column: "(SELECT password FROM admin)",
          as: "leaked_password"
        }).execute();
      }).rejects.toThrow("Invalid column name");
    });
  });

  describe("Complex expression injection prevention", () => {
    it("should prevent injection in complex expressions via semicolon", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Try to inject SQL via complex expression
      await expect(async () => {
        await query.select("COUNT(*) ; DROP TABLE x; --").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should prevent injection in complex expressions via comment markers", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("COUNT(*) -- comment").execute();
      }).rejects.toThrow("Invalid column name");

      await expect(async () => {
        await query.select("COUNT(*) /* comment */").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should prevent injection in complex expressions via quotes", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("COUNT(*) ' OR '1'='1").execute();
      }).rejects.toThrow("Invalid column name");

      await expect(async () => {
        await query.select('COUNT(*) " OR 1=1 --').execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should prevent injection in complex expressions via backslash", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("COUNT(*) \\ DROP TABLE users").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should prevent injection in subqueries via semicolon", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("(SELECT id FROM users); DROP TABLE x; --").execute();
      }).rejects.toThrow(/Invalid column name/);
    });

    it("should prevent injection in subqueries via comment markers", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("(SELECT id FROM users) -- drop table").execute();
      }).rejects.toThrow(/Invalid column name/);

      await expect(async () => {
        await query.select("(SELECT id FROM users) /* malicious */").execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should prevent injection in subqueries via quotes", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query.select("(SELECT id FROM users WHERE name = 'x' OR '1'='1')").execute();
      }).rejects.toThrow(/Invalid column name/);
    });

    it("should require expr() for aggregate functions", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Aggregate functions as strings should be rejected
      await expect(async () => {
        await query.select("COUNT(*)").execute();
      }).rejects.toThrow(/expr\(\) helper/);

      await expect(async () => {
        await query.select("MAX(id)").execute();
      }).rejects.toThrow(/expr\(\) helper/);

      await expect(async () => {
        await query.select("SUM(id)").execute();
      }).rejects.toThrow(/expr\(\) helper/);
    });

    it("should allow legitimate JSON operators", async () => {
      interface JsonSchema {
        data: {
          id: number;
          metadata: { type: string };
        };
      }

      const query = new TypedQuery<"data", JsonSchema["data"]>(
        mockPool as any,
        "data"
      );

      // Legitimate JSON operators should work
      await query.select("metadata->type").execute();
      let executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain("metadata->type");

      await query.select("metadata->>type").execute();
      executedQuery = mockPool.getLastQuery();
      expect(executedQuery.text).toContain("metadata->>type");
    });
  });
});
