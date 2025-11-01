import { SubqueryBuilder } from "../../src/builders";
import { DatabaseError } from "../../src/errors";

describe("SubqueryBuilder Security", () => {
  describe("Column Name Sanitization", () => {
    it("should sanitize column names in IN subquery", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "SELECT id FROM users",
        []
      );

      expect(result.clause).toBe("user_id IN (SELECT id FROM users)");
    });

    it("should reject malicious column names with semicolons", () => {
      expect(() => {
        SubqueryBuilder.createInSubquery(
          "user_id; DROP TABLE users",
          "SELECT id FROM users",
          []
        );
      }).toThrow(/Invalid SQL identifier/);
    });

    it("should reject column names with SQL injection attempts", () => {
      expect(() => {
        SubqueryBuilder.createInSubquery(
          "id' OR '1'='1",
          "SELECT id FROM users",
          []
        );
      }).toThrow(/Invalid SQL identifier/);
    });

    it("should quote reserved keywords in column names", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user",
        "SELECT id FROM users",
        []
      );

      // "user" is a reserved keyword and should be quoted
      expect(result.clause).toContain('"user"');
    });

    it("should handle qualified column names", () => {
      const result = SubqueryBuilder.createInSubquery(
        "users.id",
        "SELECT id FROM active_users",
        []
      );

      expect(result.clause).toBe("users.id IN (SELECT id FROM active_users)");
    });
  });

  describe("Subquery Validation", () => {
    it("should reject empty subqueries", () => {
      expect(() => {
        SubqueryBuilder.createInSubquery("user_id", "", []);
      }).toThrow(DatabaseError);

      expect(() => {
        SubqueryBuilder.createInSubquery("user_id", "", []);
      }).toThrow(/cannot be empty/i);
    });

    it("should reject subqueries with stacked queries (DROP)", () => {
      expect(() => {
        SubqueryBuilder.createInSubquery(
          "user_id",
          "SELECT id FROM users; DROP TABLE users",
          []
        );
      }).toThrow(DatabaseError);

      expect(() => {
        SubqueryBuilder.createInSubquery(
          "user_id",
          "SELECT id FROM users; DROP TABLE users",
          []
        );
      }).toThrow(/dangerous SQL statements/i);
    });

    it("should reject subqueries with stacked queries (DELETE)", () => {
      expect(() => {
        SubqueryBuilder.createExistsSubquery(
          "SELECT 1 FROM orders; DELETE FROM orders",
          []
        );
      }).toThrow(/dangerous SQL statements/i);
    });

    it("should reject subqueries with stacked queries (UPDATE)", () => {
      expect(() => {
        SubqueryBuilder.createExistsSubquery(
          "SELECT 1; UPDATE users SET admin = true",
          []
        );
      }).toThrow(/dangerous SQL statements/i);
    });

    it("should reject subqueries with stacked queries (INSERT)", () => {
      expect(() => {
        SubqueryBuilder.createExistsSubquery(
          "SELECT 1; INSERT INTO admins VALUES (1)",
          []
        );
      }).toThrow(/dangerous SQL statements/i);
    });

    it("should reject subqueries that don't start with SELECT", () => {
      expect(() => {
        SubqueryBuilder.createInSubquery(
          "user_id",
          "DELETE FROM users WHERE id = 1",
          []
        );
      }).toThrow(DatabaseError);

      expect(() => {
        SubqueryBuilder.createInSubquery(
          "user_id",
          "DELETE FROM users WHERE id = 1",
          []
        );
      }).toThrow(/must start with SELECT or WITH/i);
    });

    it("should allow subqueries starting with WITH (CTEs)", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "WITH active AS (SELECT id FROM users WHERE active = $1) SELECT id FROM active",
        [true]
      );

      expect(result.clause).toContain("WITH active AS");
    });

    it("should reject overly long subqueries (DoS prevention)", () => {
      const longSubquery = "SELECT " + "id,".repeat(5000) + " id FROM users";

      expect(() => {
        SubqueryBuilder.createInSubquery("user_id", longSubquery, []);
      }).toThrow(DatabaseError);

      expect(() => {
        SubqueryBuilder.createInSubquery("user_id", longSubquery, []);
      }).toThrow(/exceeds maximum length/i);
    });

    it("should allow valid parameterized subqueries", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "SELECT id FROM users WHERE active = $1 AND role = $2",
        [true, "admin"]
      );

      expect(result.clause).toBe(
        "user_id IN (SELECT id FROM users WHERE active = $1 AND role = $2)"
      );
      expect(result.params).toEqual([true, "admin"]);
    });
  });

  describe("Operator Validation", () => {
    it("should allow valid comparison operators", () => {
      const validOperators = ["=", "!=", "<>", "<", ">", "<=", ">="];

      validOperators.forEach((op) => {
        const result = SubqueryBuilder.createComparisonSubquery(
          "salary",
          op,
          "SELECT AVG(salary) FROM employees",
          []
        );

        expect(result.clause).toContain(op);
      });
    });

    it("should allow LIKE operators", () => {
      const result = SubqueryBuilder.createComparisonSubquery(
        "name",
        "LIKE",
        "SELECT pattern FROM patterns",
        []
      );

      expect(result.clause).toContain("LIKE");
    });

    it("should allow JSONB operators", () => {
      const result = SubqueryBuilder.createComparisonSubquery(
        "data",
        "@>",
        "SELECT config FROM configs",
        []
      );

      expect(result.clause).toContain("@>");
    });

    it("should reject invalid operators", () => {
      expect(() => {
        SubqueryBuilder.createComparisonSubquery(
          "salary",
          "INVALID",
          "SELECT AVG(salary) FROM employees",
          []
        );
      }).toThrow(DatabaseError);

      expect(() => {
        SubqueryBuilder.createComparisonSubquery(
          "salary",
          "INVALID",
          "SELECT AVG(salary) FROM employees",
          []
        );
      }).toThrow(/Invalid operator/i);
    });

    it("should reject operators with SQL injection attempts", () => {
      expect(() => {
        SubqueryBuilder.createComparisonSubquery(
          "salary",
          "> OR 1=1 --",
          "SELECT AVG(salary) FROM employees",
          []
        );
      }).toThrow(/Invalid operator/i);
    });

    it("should handle case-insensitive operators", () => {
      const result = SubqueryBuilder.createComparisonSubquery(
        "name",
        "like",
        "SELECT pattern FROM patterns",
        []
      );

      expect(result.clause).toContain("like");
    });
  });

  describe("NOT IN Subquery", () => {
    it("should sanitize column names in NOT IN", () => {
      const result = SubqueryBuilder.createNotInSubquery(
        "user_id",
        "SELECT id FROM banned_users",
        []
      );

      expect(result.clause).toBe("user_id NOT IN (SELECT id FROM banned_users)");
    });

    it("should reject malicious column names in NOT IN", () => {
      expect(() => {
        SubqueryBuilder.createNotInSubquery(
          "id'; DROP TABLE users; --",
          "SELECT id FROM banned",
          []
        );
      }).toThrow(/Invalid SQL identifier/);
    });
  });

  describe("EXISTS and NOT EXISTS", () => {
    it("should validate EXISTS subqueries", () => {
      const result = SubqueryBuilder.createExistsSubquery(
        "SELECT 1 FROM orders WHERE orders.user_id = users.id",
        []
      );

      expect(result.clause).toBe(
        "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
      );
    });

    it("should reject malicious EXISTS subqueries", () => {
      expect(() => {
        SubqueryBuilder.createExistsSubquery(
          "SELECT 1; DROP TABLE orders",
          []
        );
      }).toThrow(/dangerous SQL statements/i);
    });

    it("should validate NOT EXISTS subqueries", () => {
      const result = SubqueryBuilder.createNotExistsSubquery(
        "SELECT 1 FROM orders WHERE orders.user_id = users.id",
        []
      );

      expect(result.clause).toBe(
        "NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
      );
    });
  });

  describe("ANY and ALL Subqueries", () => {
    it("should sanitize column names in ANY", () => {
      const result = SubqueryBuilder.createAnySubquery(
        "salary",
        ">",
        "SELECT salary FROM managers",
        []
      );

      expect(result.clause).toBe("salary > ANY (SELECT salary FROM managers)");
    });

    it("should sanitize column names in ALL", () => {
      const result = SubqueryBuilder.createAllSubquery(
        "salary",
        ">",
        "SELECT salary FROM employees",
        []
      );

      expect(result.clause).toBe("salary > ALL (SELECT salary FROM employees)");
    });

    it("should reject invalid operators in ANY", () => {
      expect(() => {
        SubqueryBuilder.createAnySubquery(
          "salary",
          "MALICIOUS",
          "SELECT salary FROM managers",
          []
        );
      }).toThrow(/Invalid operator/i);
    });

    it("should reject malicious column names in ALL", () => {
      expect(() => {
        SubqueryBuilder.createAllSubquery(
          "salary; DROP TABLE salaries",
          ">",
          "SELECT salary FROM employees",
          []
        );
      }).toThrow(/Invalid SQL identifier/);
    });
  });

  describe("Integration with Parameters", () => {
    it("should preserve parameters in IN subquery", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "SELECT id FROM users WHERE active = $1 AND role = $2",
        [true, "admin"]
      );

      expect(result.params).toEqual([true, "admin"]);
    });

    it("should preserve parameters in comparison subquery", () => {
      const result = SubqueryBuilder.createComparisonSubquery(
        "salary",
        ">",
        "SELECT AVG(salary) FROM employees WHERE department = $1",
        ["IT"]
      );

      expect(result.params).toEqual(["IT"]);
    });
  });

  describe("Edge Cases", () => {
    it("should handle whitespace in subqueries", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "  SELECT id FROM users  ",
        []
      );

      expect(result.clause).toContain("SELECT id FROM users");
    });

    it("should handle newlines in subqueries", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "SELECT id\nFROM users\nWHERE active = $1",
        [true]
      );

      expect(result.clause).toContain("SELECT id");
    });

    it("should handle complex nested subqueries", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "SELECT id FROM users WHERE department_id IN (SELECT id FROM departments WHERE active = $1)",
        [true]
      );

      expect(result.clause).toContain("SELECT id FROM users WHERE department_id IN");
    });
  });
});
