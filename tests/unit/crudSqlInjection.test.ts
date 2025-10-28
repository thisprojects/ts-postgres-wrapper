import { TypedPg } from "../../src/index";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
    "user-name"?: string; // Special characters
  };
  posts: {
    id: number;
    title: string;
  };
}

describe("CRUD SQL Injection Protection", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
    mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);
  });

  describe("INSERT operations", () => {
    it("should sanitize column names in INSERT", async () => {
      // Create data with suspicious column name via object key
      const maliciousData: any = {
        name: "John",
        "email; DROP TABLE users; --": "hacker@example.com"
      };

      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize table names in INSERT", async () => {
      const data = { name: "John", email: "john@example.com" };

      // TypeScript prevents this, but test runtime protection
      await expect(async () => {
        await db.insert("users; DROP TABLE posts; --" as any, data);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should handle legitimate special characters in column names", async () => {
      // Legitimate column with hyphen (needs quoting)
      const data: any = {
        id: 1,
        "user-name": "John Doe"
      };

      await db.insert("users", data);

      const query = mockPool.getLastQuery();
      // Should be quoted because of the hyphen
      expect(query.text).toContain('"user-name"');
      expect(query.text).toContain("INSERT INTO users");
    });

    it("should protect against comment injection in column names", async () => {
      const maliciousData: any = {
        name: "John",
        "email/*comment*/": "test@example.com"
      };

      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should protect against single quote injection", async () => {
      const maliciousData: any = {
        name: "John",
        "email'OR'1'='1": "test@example.com"
      };

      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("UPDATE operations", () => {
    it("should sanitize SET column names in UPDATE", async () => {
      const maliciousData: any = {
        "name; DROP TABLE users; --": "hacker"
      };
      const where = { id: 1 };

      await expect(async () => {
        await db.update("users", maliciousData, where);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize WHERE column names in UPDATE", async () => {
      const data = { name: "John" };
      const maliciousWhere: any = {
        "id; DROP TABLE users; --": 1
      };

      await expect(async () => {
        await db.update("users", data, maliciousWhere);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize table names in UPDATE", async () => {
      const data = { name: "John" };
      const where = { id: 1 };

      await expect(async () => {
        await db.update("users; DROP TABLE posts; --" as any, data, where);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should handle legitimate column names in UPDATE", async () => {
      const data = { name: "John Doe" };
      const where = { id: 1 };

      await db.update("users", data, where);

      const query = mockPool.getLastQuery();
      expect(query.text).toContain("UPDATE users");
      expect(query.text).toContain("name = $1");
      expect(query.text).toContain("id = $2");
      expect(query.values).toEqual(["John Doe", 1]);
    });

    it("should protect against backslash injection", async () => {
      const maliciousData: any = {
        "name\\": "test"
      };

      await expect(async () => {
        await db.update("users", maliciousData, { id: 1 });
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("DELETE operations", () => {
    it("should sanitize WHERE column names in DELETE", async () => {
      const maliciousWhere: any = {
        "id; DROP TABLE users; --": 1
      };

      await expect(async () => {
        await db.delete("users", maliciousWhere);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize table names in DELETE", async () => {
      const where = { id: 1 };

      await expect(async () => {
        await db.delete("users; DROP TABLE posts; --" as any, where);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should handle legitimate column names in DELETE", async () => {
      const where = { id: 1, name: "John" };

      await db.delete("users", where);

      const query = mockPool.getLastQuery();
      expect(query.text).toContain("DELETE FROM users");
      expect(query.text).toContain("id = $1");
      expect(query.text).toContain("name = $2");
      expect(query.values).toEqual([1, "John"]);
    });

    it("should protect against line comment injection", async () => {
      const maliciousWhere: any = {
        "id--": 1
      };

      await expect(async () => {
        await db.delete("users", maliciousWhere);
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("UPSERT operations", () => {
    it("should sanitize column names in UPSERT", async () => {
      const maliciousData: any = {
        id: 1,
        "name; DROP TABLE users; --": "hacker"
      };

      await expect(async () => {
        await db.upsert("users", maliciousData, ["id"]);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize conflict column names in UPSERT", async () => {
      const data = { id: 1, name: "John" };

      await expect(async () => {
        await db.upsert("users", data, ["id; DROP TABLE users; --" as any]);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize update column names in UPSERT", async () => {
      const data = { id: 1, name: "John" };

      await expect(async () => {
        await db.upsert("users", data, ["id"], ["name; DROP TABLE users; --" as any]);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should handle legitimate UPSERT operations", async () => {
      const data = { id: 1, name: "John", email: "john@example.com" };

      await db.upsert("users", data, ["id"], ["name", "email"]);

      const query = mockPool.getLastQuery();
      expect(query.text).toContain("INSERT INTO users");
      expect(query.text).toContain("ON CONFLICT");
      expect(query.text).toContain("DO UPDATE SET");
      expect(query.text).toContain("name = EXCLUDED.name");
      expect(query.text).toContain("email = EXCLUDED.email");
    });

    it("should protect against EXCLUDED injection", async () => {
      // Even though EXCLUDED is a PostgreSQL keyword, our column names should be sanitized
      const maliciousData: any = {
        id: 1,
        "name'); DROP TABLE users; --": "hacker"
      };

      await expect(async () => {
        await db.upsert("users", maliciousData, ["id"]);
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("Reserved keywords", () => {
    it("should quote reserved keyword column names", async () => {
      // 'user' is a reserved keyword
      const data: any = {
        id: 1,
        user: "admin" // Reserved keyword
      };

      await db.insert("users", data);

      const query = mockPool.getLastQuery();
      // Should be quoted because 'user' is a reserved keyword
      expect(query.text).toContain('"user"');
    });

    it("should quote reserved keyword table names", async () => {
      // This would fail at TypeScript level normally, but tests runtime protection
      const data = { id: 1 };

      // "order" is a reserved keyword
      await db.insert("order" as any, data);

      const query = mockPool.getLastQuery();
      expect(query.text).toContain('"order"');
    });
  });

  describe("Complex injection attempts", () => {
    it("should protect against multi-statement injection", async () => {
      const maliciousData: any = {
        name: "John",
        "email; DELETE FROM users WHERE 1=1; --": "test@example.com"
      };

      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should protect against UNION injection in column names", async () => {
      const maliciousData: any = {
        name: "John",
        "email UNION SELECT * FROM passwords--": "test@example.com"
      };

      // Contains space, which requires quoting, but also injection patterns
      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should protect against subquery injection", async () => {
      const maliciousData: any = {
        name: "John",
        "email) VALUES ((SELECT password FROM admin)); --": "test@example.com"
      };

      // Contains injection pattern with semicolon
      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("Edge cases", () => {
    it("should handle empty string column names by quoting them", async () => {
      const emptyColData: any = {
        "": "value"
      };

      // Empty string gets quoted as ""
      await db.insert("users", emptyColData);

      const query = mockPool.getLastQuery();
      // Should be quoted as empty string
      expect(query.text).toContain('""');
    });

    it("should handle numeric-only column names by quoting them", async () => {
      const numericData: any = {
        "123": "value"
      };

      // Numeric identifiers can't start with numbers, so they get quoted
      await db.insert("users", numericData);

      const query = mockPool.getLastQuery();
      expect(query.text).toContain('"123"');
    });

    it("should handle Unicode column names correctly", async () => {
      const unicodeData: any = {
        id: 1,
        "名前": "Tanaka" // Japanese characters
      };

      // Unicode characters should be quoted but allowed
      await db.insert("users", unicodeData);

      const query = mockPool.getLastQuery();
      expect(query.text).toContain('"名前"');
    });

    it("should reject quoted identifiers with quotes in the column name", async () => {
      const quotedData: any = {
        id: 1,
        '"quoted_name"': "test"
      };

      // This contains double quotes which triggers injection check
      await expect(async () => {
        await db.insert("users", quotedData);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should reject malicious quoted identifiers", async () => {
      const maliciousData: any = {
        id: 1,
        '"name"; DROP TABLE users; --': "test"
      };

      await expect(async () => {
        await db.insert("users", maliciousData);
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });

  describe("Batch operations", () => {
    it("should protect batchInsert against injection", async () => {
      const maliciousData: any[] = [
        {
          name: "John",
          "email; DROP TABLE users; --": "test@example.com"
        }
      ];

      await expect(async () => {
        await db.batchInsert("users", maliciousData, 100);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should protect batchUpdate against injection", async () => {
      const maliciousUpdates: any[] = [
        {
          set: { "name; DROP TABLE users; --": "hacker" },
          where: { id: 1 }
        }
      ];

      await expect(async () => {
        await db.batchUpdate("users", maliciousUpdates);
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should protect batchDelete against injection", async () => {
      const maliciousConditions: any[] = [
        {
          "id; DROP TABLE users; --": 1
        }
      ];

      await expect(async () => {
        await db.batchDelete("users", maliciousConditions);
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });
});
