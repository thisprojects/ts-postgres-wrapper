/**
 * Tests for advanced SQL features:
 * - UNION/INTERSECT/EXCEPT
 * - CTEs (WITH clauses)
 * - Subqueries
 * - UPSERT (ON CONFLICT)
 */

import {
  SetOperationsBuilder,
  CteBuilder,
  SubqueryBuilder,
} from "../../src/builders";
import { sanitizeSqlIdentifier } from "../../src/utils";

describe("Advanced SQL Features", () => {
  describe("Set Operations (UNION/INTERSECT/EXCEPT)", () => {
    it("should build UNION query", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("UNION", "SELECT id FROM admins", []);

      const result = builder.buildQuery("SELECT id FROM users", []);

      expect(result.query).toBe("SELECT id FROM users UNION SELECT id FROM admins");
      expect(result.params).toEqual([]);
    });

    it("should build UNION ALL query", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("UNION ALL", "SELECT id FROM admins WHERE active = $1", [true]);

      const result = builder.buildQuery("SELECT id FROM users WHERE status = $1", ["active"]);

      expect(result.query).toBe(
        "SELECT id FROM users WHERE status = $1 UNION ALL SELECT id FROM admins WHERE active = $2"
      );
      expect(result.params).toEqual(["active", true]);
    });

    it("should build INTERSECT query", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("INTERSECT", "SELECT user_id FROM premium_users", []);

      const result = builder.buildQuery("SELECT user_id FROM active_users", []);

      expect(result.query).toBe(
        "SELECT user_id FROM active_users INTERSECT SELECT user_id FROM premium_users"
      );
    });

    it("should build EXCEPT query", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("EXCEPT", "SELECT id FROM deleted_users", []);

      const result = builder.buildQuery("SELECT id FROM all_users", []);

      expect(result.query).toBe(
        "SELECT id FROM all_users EXCEPT SELECT id FROM deleted_users"
      );
    });

    it("should chain multiple set operations", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("UNION", "SELECT id FROM admins", []);
      builder.addOperation("UNION", "SELECT id FROM moderators", []);

      const result = builder.buildQuery("SELECT id FROM users", []);

      expect(result.query).toBe(
        "SELECT id FROM users UNION SELECT id FROM admins UNION SELECT id FROM moderators"
      );
    });

    it("should handle parameters across multiple operations", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("UNION", "SELECT id FROM admins WHERE role = $1", ["admin"]);
      builder.addOperation("UNION", "SELECT id FROM mods WHERE role = $1", ["moderator"]);

      const result = builder.buildQuery(
        "SELECT id FROM users WHERE status = $1",
        ["active"]
      );

      expect(result.params).toEqual(["active", "admin", "moderator"]);
    });

    it("should clone set operations builder", () => {
      const builder = new SetOperationsBuilder();
      builder.addOperation("UNION", "SELECT id FROM admins", []);

      const cloned = builder.clone();
      cloned.addOperation("INTERSECT", "SELECT id FROM active", []);

      expect(builder.getOperations().length).toBe(1);
      expect(cloned.getOperations().length).toBe(2);
    });
  });

  describe("CTEs (Common Table Expressions)", () => {
    it("should build simple CTE", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);
      builder.addCte("active_users", "SELECT * FROM users WHERE status = $1", ["active"]);

      const withClause = builder.buildWithClause();

      expect(withClause).toBe("WITH active_users AS (SELECT * FROM users WHERE status = $1) ");
      expect(builder.getAllParams()).toEqual(["active"]);
    });

    it("should build CTE with column names", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);
      builder.addCte(
        "user_stats",
        "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
        [],
        ["user_id", "order_count"]
      );

      const withClause = builder.buildWithClause();

      expect(withClause).toBe(
        "WITH user_stats(user_id, order_count) AS (SELECT user_id, COUNT(*) FROM orders GROUP BY user_id) "
      );
    });

    it("should build multiple CTEs", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);
      builder.addCte("active_users", "SELECT * FROM users WHERE status = $1", ["active"]);
      builder.addCte("recent_orders", "SELECT * FROM orders WHERE created_at > $1", [
        "2024-01-01",
      ]);

      const withClause = builder.buildWithClause();

      expect(withClause).toContain("WITH active_users AS");
      expect(withClause).toContain(", recent_orders AS");
      expect(builder.getAllParams()).toEqual(["active", "2024-01-01"]);
    });

    it("should sanitize CTE names", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);
      builder.addCte("user-stats", "SELECT * FROM users", []);

      const withClause = builder.buildWithClause();

      expect(withClause).toContain('"user-stats"');
    });

    it("should handle empty CTEs", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);

      expect(builder.hasCtes()).toBe(false);
      expect(builder.buildWithClause()).toBe("");
    });

    it("should clone CTE builder", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);
      builder.addCte("cte1", "SELECT 1", []);

      const cloned = builder.clone();
      cloned.addCte("cte2", "SELECT 2", []);

      expect(builder.getCtes().length).toBe(1);
      expect(cloned.getCtes().length).toBe(2);
    });
  });

  describe("Subqueries", () => {
    it("should create IN subquery", () => {
      const result = SubqueryBuilder.createInSubquery(
        "user_id",
        "SELECT id FROM users WHERE active = $1",
        [true]
      );

      expect(result.clause).toBe(
        "user_id IN (SELECT id FROM users WHERE active = $1)"
      );
      expect(result.params).toEqual([true]);
    });

    it("should create NOT IN subquery", () => {
      const result = SubqueryBuilder.createNotInSubquery(
        "user_id",
        "SELECT id FROM banned_users",
        []
      );

      expect(result.clause).toBe("user_id NOT IN (SELECT id FROM banned_users)");
    });

    it("should create EXISTS subquery", () => {
      const result = SubqueryBuilder.createExistsSubquery(
        "SELECT 1 FROM orders WHERE orders.user_id = users.id",
        []
      );

      expect(result.clause).toBe(
        "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
      );
    });

    it("should create NOT EXISTS subquery", () => {
      const result = SubqueryBuilder.createNotExistsSubquery(
        "SELECT 1 FROM orders WHERE orders.user_id = users.id",
        []
      );

      expect(result.clause).toBe(
        "NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
      );
    });

    it("should create comparison subquery", () => {
      const result = SubqueryBuilder.createComparisonSubquery(
        "salary",
        ">",
        "SELECT AVG(salary) FROM employees",
        []
      );

      expect(result.clause).toBe("salary > (SELECT AVG(salary) FROM employees)");
    });

    it("should create ANY subquery", () => {
      const result = SubqueryBuilder.createAnySubquery(
        "salary",
        ">",
        "SELECT salary FROM managers WHERE department = $1",
        ["IT"]
      );

      expect(result.clause).toBe(
        "salary > ANY (SELECT salary FROM managers WHERE department = $1)"
      );
      expect(result.params).toEqual(["IT"]);
    });

    it("should create ALL subquery", () => {
      const result = SubqueryBuilder.createAllSubquery(
        "salary",
        ">",
        "SELECT salary FROM employees WHERE department = $1",
        ["Sales"]
      );

      expect(result.clause).toBe(
        "salary > ALL (SELECT salary FROM employees WHERE department = $1)"
      );
      expect(result.params).toEqual(["Sales"]);
    });
  });

  describe("Complex Query Examples", () => {
    it("should combine CTEs with UNION", () => {
      const cteBuilder = new CteBuilder(sanitizeSqlIdentifier);
      cteBuilder.addCte("admin_users", "SELECT * FROM users WHERE role = $1", ["admin"]);

      const setBuilder = new SetOperationsBuilder();
      setBuilder.addOperation("UNION", "SELECT * FROM moderators", []);

      const withClause = cteBuilder.buildWithClause();
      const { query, params } = setBuilder.buildQuery(
        "SELECT * FROM admin_users",
        cteBuilder.getAllParams()
      );

      const fullQuery = withClause + query;

      expect(fullQuery).toBe(
        "WITH admin_users AS (SELECT * FROM users WHERE role = $1) SELECT * FROM admin_users UNION SELECT * FROM moderators"
      );
      expect(params).toEqual(["admin"]);
    });

    it("should handle nested CTEs pattern", () => {
      const builder = new CteBuilder(sanitizeSqlIdentifier);
      builder.addCte("active_users", "SELECT * FROM users WHERE status = $1", ["active"]);
      builder.addCte(
        "user_orders",
        "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
        []
      );

      const withClause = builder.buildWithClause();

      expect(withClause).toContain("WITH active_users AS");
      expect(withClause).toContain(", user_orders AS");
    });
  });
});
