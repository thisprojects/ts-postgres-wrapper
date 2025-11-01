import { WindowFunctionBuilder } from "../../src/builders/WindowFunctionBuilder";
import { DatabaseError } from "../../src/errors";
import { sanitizeSqlIdentifier } from "../../src/utils";

describe("WindowFunctionBuilder Security", () => {
  let builder: WindowFunctionBuilder;
  let qualifyColumn: (col: string) => string;
  let validateExpression: (expr: string, context: string) => void;

  beforeEach(() => {
    // Mock qualifyColumn to just sanitize
    qualifyColumn = (col: string) => sanitizeSqlIdentifier(col);

    // Mock validateExpression with actual validation logic from TypedQuery
    validateExpression = (expr: string, context: string): void => {
      // Check for statement separators and comments
      if (/;/.test(expr)) {
        throw new DatabaseError(
          `${context} cannot contain semicolons (statement separators)`,
          'INVALID_EXPRESSION',
          { query: expr, params: [], detail: `expr: ${expr}` }
        );
      }

      if (/--/.test(expr)) {
        throw new DatabaseError(
          `${context} cannot contain SQL comments (--)`,
          'INVALID_EXPRESSION',
          { query: expr, params: [], detail: `expr: ${expr}` }
        );
      }

      if (/\/\*|\*\//.test(expr)) {
        throw new DatabaseError(
          `${context} cannot contain multi-line SQL comments (/* */)`,
          'INVALID_EXPRESSION',
          { query: expr, params: [], detail: `expr: ${expr}` }
        );
      }

      // Check for dangerous SQL keywords
      const dangerousKeywords = /\b(DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|DELETE|INSERT|UPDATE)\b/i;
      if (dangerousKeywords.test(expr)) {
        throw new DatabaseError(
          `${context} cannot contain DDL or DML keywords (DROP, CREATE, ALTER, DELETE, INSERT, UPDATE, etc.)`,
          'INVALID_EXPRESSION',
          { query: expr, params: [], detail: `expr: ${expr}` }
        );
      }

      // Check for UNION
      if (/\bUNION\b/i.test(expr)) {
        throw new DatabaseError(
          `${context} cannot contain UNION statements. Use the raw() method for complex queries.`,
          'INVALID_EXPRESSION',
          { query: expr, params: [], detail: `expr: ${expr}` }
        );
      }

      // Check for suspicious quote patterns
      if (/''\s*(?:OR|AND)\s|'\s*(?:OR|AND)\s*'[^']*'=|';|"[^"]*;/i.test(expr)) {
        throw new DatabaseError(
          `${context} contains suspicious quote patterns`,
          'INVALID_EXPRESSION',
          { query: expr, params: [], detail: `expr: ${expr}` }
        );
      }
    };

    builder = new WindowFunctionBuilder(qualifyColumn, validateExpression);
  });

  describe("Custom Window Function Validation", () => {
    it("should allow valid custom window functions", () => {
      builder.addCustomWindow("SUM(amount)", "PARTITION BY department ORDER BY date");
      const result = builder.buildWindowColumns();

      expect(result).toContain("SUM(amount) OVER (PARTITION BY department ORDER BY date)");
    });

    it("should reject custom window function with semicolons", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount); DROP TABLE users", "PARTITION BY department");
      }).toThrow(DatabaseError);

      expect(() => {
        builder.addCustomWindow("SUM(amount); DROP TABLE users", "PARTITION BY department");
      }).toThrow(/cannot contain semicolons/i);
    });

    it("should reject OVER clause with semicolons", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount)", "PARTITION BY department; DROP TABLE users");
      }).toThrow(DatabaseError);

      expect(() => {
        builder.addCustomWindow("SUM(amount)", "PARTITION BY department; DROP TABLE users");
      }).toThrow(/cannot contain semicolons/i);
    });

    it("should reject custom window function with SQL comments", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount) -- DROP TABLE users", "PARTITION BY department");
      }).toThrow(/cannot contain SQL comments/i);
    });

    it("should reject custom window function with multi-line comments", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount) /* DROP TABLE */ ", "PARTITION BY department");
      }).toThrow(/cannot contain multi-line SQL comments/i);
    });

    it("should reject custom window function with DROP keyword", () => {
      expect(() => {
        builder.addCustomWindow("DROP TABLE users", "PARTITION BY department");
      }).toThrow(/cannot contain DDL or DML keywords/i);
    });

    it("should reject custom window function with DELETE keyword", () => {
      expect(() => {
        builder.addCustomWindow("DELETE FROM users", "PARTITION BY department");
      }).toThrow(/cannot contain DDL or DML keywords/i);
    });

    it("should reject custom window function with INSERT keyword", () => {
      expect(() => {
        builder.addCustomWindow("INSERT INTO admins VALUES (1)", "PARTITION BY department");
      }).toThrow(/cannot contain DDL or DML keywords/i);
    });

    it("should reject custom window function with UPDATE keyword", () => {
      expect(() => {
        builder.addCustomWindow("UPDATE users SET admin = true", "PARTITION BY department");
      }).toThrow(/cannot contain DDL or DML keywords/i);
    });

    it("should reject custom window function with UNION keyword", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount) UNION SELECT 1", "PARTITION BY department");
      }).toThrow(/cannot contain UNION statements/i);
    });

    it("should reject OVER clause with DROP keyword", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount)", "DROP TABLE users");
      }).toThrow(/cannot contain DDL or DML keywords/i);
    });

    it("should reject custom window function with SQL injection quote pattern", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount) WHERE '1'='1' OR '1'='1", "PARTITION BY department");
      }).toThrow(/contains suspicious quote patterns/i);
    });

    it("should reject OVER clause with SQL injection quote pattern", () => {
      expect(() => {
        builder.addCustomWindow("SUM(amount)", "PARTITION BY dept WHERE '' OR ''");
      }).toThrow(/contains suspicious quote patterns/i);
    });
  });

  describe("Built-in Window Function Column Sanitization", () => {
    it("should sanitize column names in ROW_NUMBER", () => {
      builder.addRowNumber(["department"], [["salary", "DESC"]]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("PARTITION BY department");
      expect(result).toContain("ORDER BY salary DESC");
    });

    it("should reject malicious column names in ROW_NUMBER partitionBy", () => {
      expect(() => {
        builder.addRowNumber(["department; DROP TABLE users"], [["salary", "DESC"]]);
      }).toThrow(/Invalid SQL identifier/i);
    });

    it("should reject malicious column names in ROW_NUMBER orderBy", () => {
      expect(() => {
        builder.addRowNumber(["department"], [["salary; DROP TABLE users", "DESC"]]);
      }).toThrow(/Invalid SQL identifier/i);
    });

    it("should sanitize column names in RANK", () => {
      builder.addRank(["department"], [["salary", "ASC"]]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("PARTITION BY department");
      expect(result).toContain("ORDER BY salary ASC");
    });

    it("should reject malicious column names in RANK", () => {
      expect(() => {
        builder.addRank(["dept' OR '1'='1"], [["salary", "ASC"]]);
      }).toThrow(/Invalid SQL identifier/i);
    });

    it("should sanitize column names in DENSE_RANK", () => {
      builder.addDenseRank(["category"], [["score", "DESC"]]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("PARTITION BY category");
      expect(result).toContain("ORDER BY score DESC");
    });

    it("should reject malicious column names in DENSE_RANK", () => {
      expect(() => {
        builder.addDenseRank(["category; --"], [["score", "DESC"]]);
      }).toThrow(/Invalid SQL identifier/i);
    });

    it("should sanitize column names in LAG", () => {
      builder.addLag("price", 1, undefined, ["product_id"]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 1)");
      expect(result).toContain("PARTITION BY product_id");
    });

    it("should reject malicious column names in LAG", () => {
      expect(() => {
        builder.addLag("price; DROP TABLE products", 1);
      }).toThrow(/Invalid SQL identifier/i);
    });

    it("should reject malicious partition column in LAG", () => {
      expect(() => {
        builder.addLag("price", 1, undefined, ["product_id' OR '1'='1"]);
      }).toThrow(/Invalid SQL identifier/i);
    });

    it("should sanitize column names in LEAD", () => {
      builder.addLead("price", 1, undefined, ["product_id"]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LEAD(price, 1)");
      expect(result).toContain("PARTITION BY product_id");
    });

    it("should reject malicious column names in LEAD", () => {
      expect(() => {
        builder.addLead("price; --", 1);
      }).toThrow(/Invalid SQL identifier/i);
    });
  });

  describe("LAG/LEAD defaultValue Validation", () => {
    it("should handle numeric default values safely", () => {
      builder.addLag("price", 1, 0);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 1, 0)");
    });

    it("should handle negative numeric default values", () => {
      builder.addLag("price", 1, -100);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 1, -100)");
    });

    it("should handle boolean default values", () => {
      builder.addLag("active", 1, true);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(active, 1, true)");
    });

    it("should handle null default values", () => {
      builder.addLag("price", 1, null);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 1, NULL)");
    });

    it("should handle undefined default values", () => {
      builder.addLag("price", 1, undefined);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 1)");
      expect(result).not.toContain("undefined");
    });

    it("should reject string default values to prevent SQL injection", () => {
      expect(() => {
        builder.addLag("price", 1, "0");
      }).toThrow(DatabaseError);

      expect(() => {
        builder.addLag("price", 1, "0");
      }).toThrow(/cannot be a string/i);
    });

    it("should reject malicious string injection in LAG defaultValue", () => {
      const maliciousDefault = "0); DROP TABLE users; --";

      expect(() => {
        builder.addLag("price", 1, maliciousDefault);
      }).toThrow(DatabaseError);

      expect(() => {
        builder.addLag("price", 1, maliciousDefault);
      }).toThrow(/cannot be a string/i);
    });

    it("should reject malicious string injection in LEAD defaultValue", () => {
      const maliciousDefault = "NULL) OVER (); DROP TABLE users; --";

      expect(() => {
        builder.addLead("price", 1, maliciousDefault);
      }).toThrow(DatabaseError);

      expect(() => {
        builder.addLead("price", 1, maliciousDefault);
      }).toThrow(/cannot be a string/i);
    });

    it("should reject Infinity in default values", () => {
      expect(() => {
        builder.addLag("price", 1, Infinity);
      }).toThrow(/must be a finite number/i);
    });

    it("should reject NaN in default values", () => {
      expect(() => {
        builder.addLag("price", 1, NaN);
      }).toThrow(/must be a finite number/i);
    });

    it("should reject object default values", () => {
      expect(() => {
        builder.addLag("price", 1, { value: 0 });
      }).toThrow(/must be a number, boolean, or null/i);
    });

    it("should reject array default values", () => {
      expect(() => {
        builder.addLag("price", 1, [0]);
      }).toThrow(/must be a number, boolean, or null/i);
    });
  });

  describe("Window Function Offset Validation", () => {
    it("should handle valid numeric offset in LAG", () => {
      builder.addLag("price", 5);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 5)");
    });

    it("should handle valid numeric offset in LEAD", () => {
      builder.addLead("price", 3);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LEAD(price, 3)");
    });

    it("should handle offset of 0", () => {
      builder.addLag("price", 0);
      const result = builder.buildWindowColumns();

      expect(result).toContain("LAG(price, 0)");
    });

    it("should reject negative offset in LAG", () => {
      expect(() => {
        builder.addLag("price", -1);
      }).toThrow(/must be a non-negative integer/i);
    });

    it("should reject negative offset in LEAD", () => {
      expect(() => {
        builder.addLead("price", -5);
      }).toThrow(/must be a non-negative integer/i);
    });

    it("should reject fractional offset in LAG", () => {
      expect(() => {
        builder.addLag("price", 1.5);
      }).toThrow(/must be a non-negative integer/i);
    });

    it("should reject fractional offset in LEAD", () => {
      expect(() => {
        builder.addLead("price", 2.7);
      }).toThrow(/must be a non-negative integer/i);
    });

    it("should reject NaN offset in LAG", () => {
      expect(() => {
        builder.addLag("price", NaN);
      }).toThrow(/must be a non-negative integer/i);
    });

    it("should reject Infinity offset in LEAD", () => {
      expect(() => {
        builder.addLead("price", Infinity);
      }).toThrow(/must be a non-negative integer/i);
    });
  });

  describe("Multiple Window Functions", () => {
    it("should handle multiple window functions safely", () => {
      builder.addRowNumber(["department"], [["salary", "DESC"]]);
      builder.addRank(["department"], [["salary", "DESC"]]);
      builder.addLag("salary", 1, 0, ["department"]);

      const result = builder.buildWindowColumns();

      expect(result).toContain("ROW_NUMBER()");
      expect(result).toContain("RANK()");
      expect(result).toContain("LAG(salary, 1, 0)");
      expect(result).toContain("as window_1");
      expect(result).toContain("as window_2");
      expect(result).toContain("as window_3");
    });
  });

  describe("Clone Functionality", () => {
    it("should clone builder with all state", () => {
      builder.addRowNumber(["department"]);
      builder.addRank(["category"]);

      const cloned = builder.clone();
      cloned.addLag("price", 1);

      // Original should have 2 functions
      expect(builder.getFunctions().length).toBe(2);

      // Cloned should have 3 functions
      expect(cloned.getFunctions().length).toBe(3);
    });

    it("should maintain validation in cloned builder", () => {
      const cloned = builder.clone();

      expect(() => {
        cloned.addCustomWindow("DROP TABLE users", "");
      }).toThrow(/cannot contain DDL or DML keywords/i);
    });
  });

  describe("Edge Cases", () => {
    it("should handle empty partition and order clauses", () => {
      builder.addRowNumber();
      const result = builder.buildWindowColumns();

      expect(result).toMatch(/ROW_NUMBER\(\) OVER \(\s*\)/);
    });

    it("should handle partition without order", () => {
      builder.addRank(["department"]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("PARTITION BY department");
      expect(result).not.toContain("ORDER BY");
    });

    it("should handle order without partition", () => {
      builder.addRank(undefined, [["salary", "DESC"]]);
      const result = builder.buildWindowColumns();

      expect(result).toContain("ORDER BY salary DESC");
      expect(result).not.toContain("PARTITION BY");
    });

    it("should return empty string when no functions added", () => {
      const result = builder.buildWindowColumns();
      expect(result).toBe("");
    });

    it("should correctly report hasFunctions", () => {
      expect(builder.hasFunctions()).toBe(false);

      builder.addRowNumber();
      expect(builder.hasFunctions()).toBe(true);
    });
  });
});
