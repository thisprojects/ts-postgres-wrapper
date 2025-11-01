import { TypedQuery } from "../../src";
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

describe("Column Aliases", () => {
  let pool: MockPool;
  let query: TypedQuery<"users", TestSchema["users"], TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery<"users", TestSchema["users"], TestSchema>(
      pool as any,
      "users"
    );
  });

  describe("Simple column selection", () => {
    it("should support single column alias", async () => {
      await query
        .select({ column: "firstName", as: "name" })
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT firstName AS name FROM users"
      );
    });

    it("should support multiple column aliases", async () => {
      await query
        .select(
          { column: "firstName", as: "givenName" },
          { column: "lastName", as: "surname" }
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT firstName AS givenName, lastName AS surname FROM users"
      );
    });

    it("should support mixing regular columns and aliases", async () => {
      await query
        .select(
          "id",
          { column: "firstName", as: "name" },
          "email"
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT id, firstName AS name, email FROM users"
      );
    });
  });

  describe("With JOINs", () => {
    it("should properly qualify aliased columns in JOINs", async () => {
      await query
        .innerJoin("posts", "users.id", "posts.userId")
        .select(
          { column: "users.firstName", as: "authorName" },
          { column: "posts.title", as: "articleTitle" }
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT users.firstName AS authorName, posts.title AS articleTitle " +
        "FROM users INNER JOIN posts ON users.id = posts.userId"
      );
    });

    it("should handle mix of qualified and unqualified columns", async () => {
      await query
        .innerJoin("posts", "users.id", "posts.userId")
        .select(
          "users.id",
          { column: "firstName", as: "name" },
          { column: "posts.title", as: "articleTitle" }
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT users.id, users.firstName AS name, posts.title AS articleTitle " +
        "FROM users INNER JOIN posts ON users.id = posts.userId"
      );
    });
  });

  describe("With WHERE clauses", () => {
    it("should support filtering on aliased columns", async () => {
      await query
        .select(
          { column: "firstName", as: "name" },
          { column: "age", as: "userAge" }
        )
        .where("age", ">", 18)
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT firstName AS name, age AS userAge FROM users WHERE age > $1",
        [18]
      );
    });
  });

  describe("With ORDER BY", () => {
    it("should support ordering by original column names", async () => {
      await query
        .select(
          { column: "firstName", as: "name" },
          { column: "age", as: "userAge" }
        )
        .orderBy("age", "DESC")
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT firstName AS name, age AS userAge FROM users ORDER BY age DESC"
      );
    });
  });

  describe("Complex scenarios", () => {
    it("should handle all query features with aliases", async () => {
      await query
        .innerJoin("posts", "users.id", "posts.userId")
        .select(
          { column: "users.firstName", as: "authorName" },
          { column: "users.lastName", as: "authorSurname" },
          { column: "posts.title", as: "articleTitle" }
        )
        .where("users.age", ">", 21)
        .orderBy("users.lastName", "ASC")
        .limit(10)
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT users.firstName AS authorName, users.lastName AS authorSurname, " +
        "posts.title AS articleTitle FROM users " +
        "INNER JOIN posts ON users.id = posts.userId " +
        "WHERE users.age > $1 " +
        "ORDER BY users.lastName ASC LIMIT 10",
        [21]
      );
    });

    it("should support aliases with aggregate functions using expr()", async () => {
      const { expr } = await import("../../src/types");

      await query
        .select(
          expr("COUNT(*)", "total"),
          expr("AVG(age)", "averageAge")
        )
        .groupBy("age")
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT COUNT(*) AS total, AVG(age) AS averageAge " +
        "FROM users GROUP BY age"
      );
    });
  });
});