import { TypedQuery, TypedPg } from "../../src/index";
import { MockPool, TestSchema } from "../test_utils";

describe("Data Safety", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
  });

  describe("SQL Injection Prevention", () => {
    it("should safely handle malicious input in WHERE clause", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .where("name", "=", "Robert'; DROP TABLE users; --")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name = $1",
        ["Robert'; DROP TABLE users; --"]
      );
    });

    it("should safely handle malicious input in ORDER BY", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // SQL injection attempts in column names should be rejected
      await expect(async () => {
        await query
          .where("active", "=", true)
          .orderBy("name; DROP TABLE users; --")
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should safely handle malicious input in SELECT", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // SQL injection attempts in column names should be rejected
      await expect(async () => {
        await query
          .select("id", "name; DROP TABLE users; --" as any)
          .execute();
      }).rejects.toThrow("Invalid column name");
    });

    it("should safely handle malicious input in IN clause", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .where("id", "IN", ["1", "2; DROP TABLE users; --", "3"])
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE id IN ($1, $2, $3)",
        ["1", "2; DROP TABLE users; --", "3"]
      );
    });

    it("should safely handle malicious input in LIKE patterns", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .where("name", "LIKE", "%'; DROP TABLE users; --")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name LIKE $1",
        ["%'; DROP TABLE users; --"]
      );
    });

    it("should safely handle malicious input in INSERT", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      await db.insert("users", {
        id: 1,
        name: "Robert'); DROP TABLE users; --",
        email: "robert@example.com",
        age: 30,
        active: true,
        created_at: new Date()
      });

      const query = mockPool.getLastQuery();
      expect(query.text).toBe("INSERT INTO users (id, name, email, age, active, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *");
      expect(query.values).toEqual(
        expect.arrayContaining([
          1,
          "Robert'); DROP TABLE users; --",
          "robert@example.com",
          30,
          true,
          expect.any(String)
        ])
      );
    });

    it("should safely handle malicious input in UPDATE", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      await db.update(
        "users",
        { name: "Robert'); DROP TABLE users; --" },
        { id: 1 }
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "UPDATE users SET name = $1 WHERE id = $2 RETURNING *",
        ["Robert'); DROP TABLE users; --", 1]
      );
    });

    it("should safely handle malicious input in raw queries", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      await db.raw(
        "SELECT * FROM users WHERE name = $1",
        ["Robert'; DROP TABLE users; --"]
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name = $1",
        ["Robert'; DROP TABLE users; --"]
      );
    });

    it("should safely handle special characters in column names", async () => {
      interface SchemaWithSpecialChars {
        users: {
          id: number;
          "special-name": string;
          "weird.column": string;
          "名前": string;
        };
      }

      const db = new TypedPg<SchemaWithSpecialChars>(mockPool as any);

      await db.table("users")
        .select("id", "special-name", "weird.column", "名前")
        .where("special-name", "=", "test")
        .execute();

      // Note: "weird.column" looks like a qualified name (table.column) so it won't be quoted
      // In real usage, columns with dots should be avoided or explicitly quoted in the schema
      expect(mockPool).toHaveExecutedQueryWithParams(
        'SELECT id, "special-name", weird.column, "名前" FROM users WHERE "special-name" = $1',
        ["test"]
      );
    });

    it("should safely handle table names with special characters", async () => {
      interface SchemaWithSpecialTable {
        "special-table": {
          id: number;
          name: string;
        };
      }

      const db = new TypedPg<SchemaWithSpecialTable>(mockPool as any);

      await db.table("special-table")
        .select("id", "name")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        'SELECT id, name FROM "special-table"'
      );
    });
  });

  describe("UTF-8 Character Handling", () => {
    it("should handle valid UTF-8 characters in text fields", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      const testData = {
        id: 1,
        name: "José María 🌟 张伟",
        email: "josé@example.com",
        age: 30,
        active: true,
        created_at: new Date()
      };

      await db.insert("users", testData);

      const query = mockPool.getLastQuery();
      expect(query.text).toBe("INSERT INTO users (id, name, email, age, active, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *");
      expect(query.values).toEqual(
        expect.arrayContaining([
          1,
          "José María 🌟 张伟",
          "josé@example.com",
          30,
          true,
          expect.any(String)
        ])
      );
    });

    it("should handle high and low surrogate pairs", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      const testString = "𝌆"; // Mathematical symbol using surrogate pair
      await db.update(
        "users",
        { name: testString },
        { id: 1 }
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "UPDATE users SET name = $1 WHERE id = $2 RETURNING *",
        [testString, 1]
      );
    });

    it("should handle characters from different Unicode planes", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Test characters from various Unicode planes
      const testStrings = [
        "Hello", // Basic Latin
        "Здравствуйте", // Cyrillic
        "שָׁלוֹם", // Hebrew with points
        "こんにちは", // Hiragana
        "안녕하세요", // Hangul
        "你好", // CJK
        "𠀋", // CJK Extension B
        "🌟", // Emoji
        "🏳️‍🌈", // Complex emoji with ZWJ sequence
        "👨‍👩‍👧‍👦", // Family emoji with multiple ZWJ sequences
      ];

      await Promise.all(testStrings.map(str => {
        const newQuery = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        return newQuery.where("name", "=", str).execute();
      }));

      // Verify all queries executed with proper parameter binding
      expect(mockPool.getQueryLog()).toEqual(
        testStrings.map(str => ({
          text: "SELECT * FROM users WHERE name = $1",
          values: [str]
        }))
      );
    });

    it("should handle control characters and zero-width spaces", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      const testStrings = [
        "Hello\u200BWorld", // Zero-width space
        "Line1\u2028Line2", // Line separator
        "Para1\u2029Para2", // Paragraph separator
        "Text\u0000Null", // Null character
        "Tab\u0009Here", // Tab
        "Line\u000AFeed", // Line feed
        "Carriage\u000DReturn", // Carriage return
      ];

      await Promise.all(testStrings.map(str =>
        db.insert("users", {
          id: 1,
          name: str,
          email: "test@example.com",
          age: 30,
          active: true,
          created_at: new Date()
        })
      ));

      // Verify all queries executed with proper parameter binding
      expect(mockPool.getQueryLog()).toEqual(
        testStrings.map(str => ({
          text: "INSERT INTO users (id, name, email, age, active, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *",
          values: [1, str, "test@example.com", 30, true, expect.any(String)]
        }))
      );
    });

    it("should handle combining characters and diacritical marks", async () => {
      const testStrings = [
        "e\u0301", // é using combining acute accent
        "a\u0308", // ä using combining diaeresis
        "n\u0303", // ñ using combining tilde
        "o\u0302", // ô using combining circumflex
        "u\u0306", // ŭ using combining breve
        "i\u0307", // i with dot above
        "c\u0327", // ç using combining cedilla
      ];

      await Promise.all(testStrings.map(str => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        return query.where("name", "=", str).execute();
      }));

      expect(mockPool.getQueryLog()).toEqual(
        testStrings.map(str => ({
          text: "SELECT * FROM users WHERE name = $1",
          values: [str]
        }))
      );
    });

    it("should handle bidirectional text", async () => {
      const testStrings = [
        "Hello مرحبا", // Mixed LTR and RTL
        "שלום Hello", // RTL with LTR
        "ABC مرحبا 123", // LTR with RTL with numbers
        "Hello \u202Eمرحبا\u202C World", // With explicit RTL markers
      ];

      await Promise.all(testStrings.map(str => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        return query.where("name", "=", str).execute();
      }));

      expect(mockPool.getQueryLog()).toEqual(
        testStrings.map(str => ({
          text: "SELECT * FROM users WHERE name = $1",
          values: [str]
        }))
      );
    });
  });

  describe("Field Size Limits", () => {
    it("should handle large text fields", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      // Generate a large string (1MB)
      const largeString = "x".repeat(1024 * 1024);

      await db.insert("users", {
        id: 1,
        name: largeString,
        email: "test@example.com",
        age: 30,
        active: true,
        created_at: new Date()
      });

      const query = mockPool.getLastQuery();
      expect(query.text).toBe("INSERT INTO users (id, name, email, age, active, created_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *");
      expect(query.values).toEqual(
        expect.arrayContaining([
          1,
          largeString,
          "test@example.com",
          30,
          true,
          expect.any(String)
        ])
      );
    });

    it("should handle large arrays", async () => {
      interface SchemaWithArrays {
        test_table: {
          id: number;
          string_array: string[];
          number_array: number[];
        };
      }

      const db = new TypedPg<SchemaWithArrays>(mockPool as any);

      // Generate large arrays
      const largeStringArray = Array(10000).fill("test");
      const largeNumberArray = Array(10000).fill(42);

      await db.insert("test_table", {
        id: 1,
        string_array: largeStringArray,
        number_array: largeNumberArray
      });

      expect(mockPool).toHaveExecutedQueryWithParams(
        "INSERT INTO test_table (id, string_array, number_array) VALUES ($1, $2, $3) RETURNING *",
        [1, largeStringArray, largeNumberArray]
      );
    });

    it("should handle large JSON objects", async () => {
      interface SchemaWithJson {
        test_table: {
          id: number;
          json_data: Record<string, any>;
        };
      }

      const db = new TypedPg<SchemaWithJson>(mockPool as any);

      // Generate a large nested JSON object
      const generateNestedObject = (depth: number, width: number): any => {
        if (depth === 0) {
          return "value";
        }
        const obj: Record<string, any> = {};
        for (let i = 0; i < width; i++) {
          obj[`key${i}`] = generateNestedObject(depth - 1, width);
        }
        return obj;
      };

      const largeJsonObject = generateNestedObject(5, 10); // Creates a deeply nested object

      await db.insert("test_table", {
        id: 1,
        json_data: largeJsonObject
      });

      expect(mockPool).toHaveExecutedQueryWithParams(
        "INSERT INTO test_table (id, json_data) VALUES ($1, $2) RETURNING *",
        [1, largeJsonObject]
      );
    });

    it("should handle many parameters in a single query", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Generate large number of parameters
      const values = Array(1000).fill(1);
      await query.where("id", "IN", values).execute();

      const expectedPlaceholders = values.map((_, i) => `$${i + 1}`).join(", ");
      expect(mockPool).toHaveExecutedQueryWithParams(
        `SELECT * FROM users WHERE id IN (${expectedPlaceholders})`,
        values
      );
    });

    it("should handle large number of WHERE conditions", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Add 100 WHERE conditions
      let currentQuery = query;
      for (let i = 0; i < 100; i++) {
        currentQuery = currentQuery.where("id", "!=", i);
      }

      await currentQuery.execute();

      const expectedSql = "SELECT * FROM users WHERE " +
        Array(100).fill("id != $").map((str, i) => `${str}${i + 1}`).join(" AND ");

      expect(mockPool).toHaveExecutedQueryWithParams(
        expectedSql,
        Array(100).fill(0).map((_, i) => i)
      );
    });

    it("should handle large ORDER BY clauses", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Add multiple ORDER BY clauses
      let currentQuery = query;
      const columns = ["id", "name", "email", "age", "created_at"];
      const directions = ["ASC", "DESC"] as const;

      for (const col of columns) {
        for (const dir of directions) {
          currentQuery = currentQuery.orderBy(col, dir);
        }
      }

      await currentQuery.execute();

      const expectedOrderBy = columns
        .map(col => `${col} ASC, ${col} DESC`)
        .join(", ");

      expect(mockPool).toHaveExecutedQuery(
        `SELECT * FROM users ORDER BY ${expectedOrderBy}`
      );
    });

    it("should handle large number of JOINs", async () => {
      interface SchemaWithManyTables {
        table_0: { id: number };
        table_1: { id: number, parent_id: number };
        table_2: { id: number, parent_id: number };
        table_3: { id: number, parent_id: number };
        table_4: { id: number, parent_id: number };
        table_5: { id: number, parent_id: number };
      }

      const db = new TypedPg<SchemaWithManyTables>(mockPool as any);

      let query = db.table("table_0");

      // Add 5 JOINs
      for (let i = 1; i <= 5; i++) {
        query = query.innerJoin(
          `table_${i}` as any,
          `table_${i-1}.id`,
          `table_${i}.parent_id`
        );
      }

      await query.execute();

      const expectedSql = [
        "SELECT * FROM table_0",
        ...Array(5).fill(0).map((_, i) =>
          `INNER JOIN table_${i+1} ON table_${i}.id = table_${i+1}.parent_id`
        )
      ].join(" ");

      expect(mockPool).toHaveExecutedQuery(expectedSql);
    });
  });
});