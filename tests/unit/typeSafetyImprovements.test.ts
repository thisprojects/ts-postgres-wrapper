import { TypedQuery, col, expr } from "../../src/index";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    firstName: string;
    lastName: string;
    email: string;
    age: number;
  };
  posts: {
    id: number;
    userId: number;
    title: string;
    content: string;
  };
}

describe("Type Safety Improvements", () => {
  let pool: MockPool;
  let query: TypedQuery<"users", TestSchema["users"], TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery<"users", TestSchema["users"], TestSchema>(
      pool as any,
      "users"
    );
  });

  describe("Helper functions for typed selections", () => {
    it("should provide type-safe column aliases with col() helper", async () => {
      // Using the col() helper provides better type inference
      await query
        .select(
          col<TestSchema["users"], "firstName">("firstName", "givenName"),
          col<TestSchema["users"], "lastName">("lastName", "surname")
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT firstName AS givenName, lastName AS surname FROM users"
      );
    });

    it("should handle expression aliases with expr() helper", async () => {
      await query
        .select(
          expr("COUNT(*)", "total"),
          expr("AVG(age)", "averageAge")
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT COUNT(*) AS total, AVG(age) AS averageAge FROM users"
      );
    });

    it("should mix column names, col() and expr() helpers", async () => {
      await query
        .select(
          "id",
          col<TestSchema["users"], "firstName">("firstName", "name"),
          expr("MAX(age)", "maxAge")
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT id, firstName AS name, MAX(age) AS maxAge FROM users"
      );
    });
  });

  describe("Type inference for different selection patterns", () => {
    it("should maintain type safety for simple column selection", async () => {
      const result = await query.select("id", "firstName", "email").execute();

      // Type inference should work - these should be accessible
      // In actual TypeScript code, this would be type-checked at compile time
      expect(pool).toHaveExecutedQuery(
        "SELECT id, firstName, email FROM users"
      );
    });

    it("should handle object syntax for aliases", async () => {
      await query
        .select(
          { column: "firstName", as: "name" },
          { column: "lastName", as: "surname" }
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT firstName AS name, lastName AS surname FROM users"
      );
    });

    it("should support aggregate functions with proper types", async () => {
      // Using aggregate methods provides better type inference
      const minResult = await query.min("age").execute();
      const maxResult = await query.max("age").execute();
      const sumResult = await query.sum("age").execute();
      const avgResult = await query.avg("age").execute();

      // All these should have proper types based on the column type
      expect(pool.getQueryLog().length).toBe(4);
    });
  });

  describe("Expression handling", () => {
    it("should handle aggregate expressions in select", async () => {
      await query
        .select("firstName", expr("COUNT(*)", "count"))
        .groupBy("firstName")
        .execute();

      // For aggregate functions with aliases, use expr() helper
      expect(pool).toHaveExecutedQuery(
        'SELECT firstName, COUNT(*) AS count FROM users GROUP BY firstName'
      );
    });

    it("should handle qualified column names", async () => {
      await query
        .innerJoin("posts", "users.id", "posts.userId")
        .select("users.firstName", "posts.title")
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT users.firstName, posts.title FROM users INNER JOIN posts ON users.id = posts.userId"
      );
    });

    it("should handle complex expressions with calculations", async () => {
      await query
        .select(
          expr("age * 2", "doubleAge"),
          expr("UPPER(firstName)", "upperName")
        )
        .execute();

      // The expr() helper allows passing SQL expressions
      // Expressions with special chars like * are quoted, functions with () are not
      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain('"age * 2" AS doubleAge');
      expect(executedQuery.text).toContain('UPPER(firstName) AS upperName');
    });
  });

  describe("Type narrowing with select", () => {
    it("should narrow return type when selecting specific columns", async () => {
      // This demonstrates that select() changes the Row type
      // The returned TypedQuery should have a different Row type
      const narrowedQuery = query.select("id", "firstName");

      await narrowedQuery.execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT id, firstName FROM users"
      );
    });

    it("should handle aliased columns in return type", async () => {
      await query
        .select(
          { column: "firstName", as: "name" },
          { column: "age", as: "years" }
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT firstName AS name, age AS years FROM users"
      );
    });
  });

  describe("Fallback to Record<string, any> for complex cases", () => {
    it("should fall back to Record<string, any> for string expressions", async () => {
      // When using arbitrary string expressions, use expr() helper
      await query.select(expr("COUNT(*)", "count"), expr("MAX(age)", "max_age")).execute();

      expect(pool).toHaveExecutedQuery(
        'SELECT COUNT(*) AS count, MAX(age) AS max_age FROM users'
      );
    });

    it("should handle mixed typed and untyped selections", async () => {
      await query
        .select(
          "id",  // Typed: known column
          { column: "firstName", as: "name" },  // Typed: known column with alias
          expr("RANDOM()", "rand")  // Untyped: expression (use expr() helper)
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        'SELECT id, firstName AS name, RANDOM() AS rand FROM users'
      );
    });
  });

  describe("Reserved keyword handling in type-safe selections", () => {
    it("should quote reserved keywords when used as aliases", async () => {
      interface SchemaWithKeywords {
        data: {
          id: number;
          value: string;
        };
      }

      const keywordQuery = new TypedQuery<"data", SchemaWithKeywords["data"]>(
        pool as any,
        "data"
      );

      await keywordQuery
        .select(
          { column: "value", as: "user" },  // 'user' is reserved
          { column: "value", as: "select" }  // 'select' is reserved
        )
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain('"user"');
      expect(executedQuery.text).toContain('"select"');
    });
  });
});
