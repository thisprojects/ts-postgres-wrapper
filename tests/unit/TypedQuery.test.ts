import { TypedQuery, TypedPg, expr } from "../../src/index";
import { MockPool, TestSchema, createMockUser } from "../test_utils";

describe("TypedQuery", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
  });

  describe("Basic SELECT queries", () => {
    it("should execute basic select query", async () => {
      const mockUser = createMockUser();
      mockPool.setMockResults([mockUser]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const results = await query.execute();

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users");
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual(mockUser);
    });

    it("should select specific columns", async () => {
      const mockData = { id: 1, name: "John", email: "john@example.com" };
      mockPool.setMockResults([mockData]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const results = await query.select("id", "name", "email").execute();

      expect(mockPool).toHaveExecutedQuery("SELECT id, name, email FROM users");
      expect(results).toEqual([mockData]);
    });

    it("should return first result with first() method", async () => {
      const mockUser = createMockUser();
      mockPool.setMockResults([mockUser]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.first();

      expect(result).toEqual(mockUser);
    });

    it("should return null when no results with first() method", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.first();

      expect(result).toBeNull();
    });
  });

  describe("WHERE clauses", () => {
    it("should handle equals operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("id", "=", 123).execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE id = $1",
        [123]
      );
    });

    it("should handle comparison operators", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("age", ">", 18).where("age", "<=", 65).execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE age > $1 AND age <= $2",
        [18, 65]
      );
    });

    it("should handle IS NULL operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("deleted_at", "IS NULL", null).execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users WHERE deleted_at IS NULL"
      );
    });

    it("should handle IS NOT NULL operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("deleted_at", "IS NOT NULL", null).execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users WHERE deleted_at IS NOT NULL"
      );
    });

    it("should handle IS NULL with qualified columns", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .innerJoin("posts", "users.id", "posts.user_id")
        .where("users.deleted_at", "IS NULL", null)
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.deleted_at IS NULL"
      );
    });

    it("should handle mixed NULL and regular conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .where("deleted_at", "IS NULL", null)
        .where("active", "=", true)
        .where("manager_id", "IS NOT NULL", null)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE deleted_at IS NULL AND active = $1 AND manager_id IS NOT NULL",
        [true]
      );
    });

    it("should handle BETWEEN operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("age", "BETWEEN", [20, 30]).execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE age BETWEEN $1 AND $2",
        [20, 30]
      );
    });

    it("should handle BETWEEN operator with qualified columns", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .innerJoin("posts", "users.id", "posts.user_id")
        .where("users.age", "BETWEEN", [20, 30])
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.age BETWEEN $1 AND $2",
        [20, 30]
      );
    });

    it("should handle BETWEEN operator with multiple conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .where("age", "BETWEEN", [20, 30])
        .where("salary", "BETWEEN", [50000, 100000])
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE age BETWEEN $1 AND $2 AND salary BETWEEN $3 AND $4",
        [20, 30, 50000, 100000]
      );
    });

    it("should handle IN operator with arrays", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("id", "IN", [1, 2, 3]).execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE id IN ($1, $2, $3)",
        [1, 2, 3]
      );
    });

    it("should handle LIKE operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("name", "LIKE", "%john%").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name LIKE $1",
        ["%john%"]
      );
    });

    it("should handle ILIKE operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("email", "ILIKE", "%EXAMPLE.COM%").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE email ILIKE $1",
        ["%EXAMPLE.COM%"]
      );
    });

    it("should handle multiple WHERE conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .where("active", "=", true)
        .where("age", ">=", 18)
        .where("name", "LIKE", "%john%")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE active = $1 AND age >= $2 AND name LIKE $3",
        [true, 18, "%john%"]
      );
    });
  });

  describe("OR WHERE clauses", () => {
    it("should handle OR WHERE clause", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("active", "=", true).orWhere("id", "=", 1).execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE active = $1 OR id = $2",
        [true, 1]
      );
    });

    it("should handle complex OR WHERE combinations", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .where("active", "=", true)
        .where("age", ">=", 18)
        .orWhere("id", "IN", [1, 2, 3])
        .orWhere("email", "LIKE", "%admin%")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE active = $1 AND age >= $2 OR id IN ($3, $4, $5) OR email LIKE $6",
        [true, 18, 1, 2, 3, "%admin%"]
      );
    });
  });

  describe("ORDER BY clauses", () => {
    it("should handle single ORDER BY ascending", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.orderBy("name", "ASC").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users ORDER BY name ASC"
      );
    });

    it("should handle single ORDER BY descending", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.orderBy("created_at", "DESC").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users ORDER BY created_at DESC"
      );
    });

    it("should handle multiple ORDER BY clauses", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .orderBy("name", "ASC")
        .orderBy("created_at", "DESC")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users ORDER BY name ASC, created_at DESC"
      );
    });

    it("should default to ASC when direction not specified", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.orderBy("name").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users ORDER BY name ASC"
      );
    });
  });

  describe("LIMIT and OFFSET", () => {
    it("should handle LIMIT clause", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.limit(10).execute();

      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users LIMIT 10");
    });

    it("should handle OFFSET clause", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.offset(20).execute();

      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users OFFSET 20");
    });

    it("should handle LIMIT and OFFSET together", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.limit(10).offset(20).execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users LIMIT 10 OFFSET 20"
      );
    });
  });

  describe("COUNT queries", () => {
    it("should execute count query", async () => {
      mockPool.setMockResults([{ count: "42" }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const count = await query.count();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT COUNT(*) as count FROM users"
      );
      expect(count).toBe(42);
    });

    it("should execute count query with WHERE clause", async () => {
      mockPool.setMockResults([{ count: "5" }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const count = await query.where("active", "=", true).count();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT COUNT(*) as count FROM users WHERE active = $1",
        [true]
      );
      expect(count).toBe(5);
    });
  });

  describe("JOIN queries", () => {
    describe("INNER JOIN", () => {
      it("should handle INNER JOIN without alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id"
        );
      });

      it("should handle INNER JOIN with alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "p.user_id", "p")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN posts AS p ON users.id = p.user_id"
        );
      });

      it("should qualify columns when INNER JOIN is used with WHERE", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .where("active", "=", true)
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1",
          [true]
        );
      });

      it("should handle INNER JOIN with SELECT specific columns", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .select("users.id", "users.name", "posts.title")
          .innerJoin("posts", "users.id", "posts.user_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT users.id, users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id"
        );
      });

      it("should handle INNER JOIN with ORDER BY", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .orderBy("name", "ASC")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id ORDER BY users.name ASC"
        );
      });
    });

    describe("LEFT JOIN", () => {
      it("should handle LEFT JOIN without alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .leftJoin("posts", "users.id", "posts.user_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users LEFT JOIN posts ON users.id = posts.user_id"
        );
      });

      it("should handle LEFT JOIN with alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .leftJoin("posts", "users.id", "p.user_id", "p")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users LEFT JOIN posts AS p ON users.id = p.user_id"
        );
      });
    });

    describe("RIGHT JOIN", () => {
      it("should handle RIGHT JOIN without alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .rightJoin("posts", "users.id", "posts.user_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users RIGHT JOIN posts ON users.id = posts.user_id"
        );
      });

      it("should handle RIGHT JOIN with alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .rightJoin("posts", "users.id", "p.user_id", "p")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users RIGHT JOIN posts AS p ON users.id = p.user_id"
        );
      });
    });

    describe("FULL JOIN", () => {
      it("should handle FULL JOIN without alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .fullJoin("posts", "users.id", "posts.user_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users FULL JOIN posts ON users.id = posts.user_id"
        );
      });

      it("should handle FULL JOIN with alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .fullJoin("posts", "users.id", "p.user_id", "p")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users FULL JOIN posts AS p ON users.id = p.user_id"
        );
      });
    });

    describe("Self JOINs", () => {
      it("should handle self join with alias", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("users", "users.manager_id", "mgr.id", "mgr")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN users AS mgr ON users.manager_id = mgr.id"
        );
      });

      it("should handle self join with complex conditions", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .select("users.id", "users.name", { column: "mgr.name", as: "manager_name" })
          .innerJoin("users", "users.manager_id", "mgr.id", "mgr")
          .where("users.department", "=", "Engineering")
          .orderBy("users.name")
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT users.id, users.name, mgr.name AS manager_name FROM users INNER JOIN users AS mgr ON users.manager_id = mgr.id WHERE users.department = $1 ORDER BY users.name ASC",
          ["Engineering"]
        );
      });

      it("should handle multiple self joins", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .select("users.name", { column: "mgr.name", as: "manager_name" }, { column: "dir.name", as: "director_name" })
          .innerJoin("users", "users.manager_id", "mgr.id", "mgr")
          .innerJoin("users", "mgr.manager_id", "dir.id", "dir")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT users.name, mgr.name AS manager_name, dir.name AS director_name FROM users INNER JOIN users AS mgr ON users.manager_id = mgr.id INNER JOIN users AS dir ON mgr.manager_id = dir.id"
        );
      });

      it("should handle self join with multiple conditions in WHERE clause", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("users", "users.manager_id", "mgr.id", "mgr")
          .where("users.department", "=", "Engineering")
          .where("mgr.department", "=", "Engineering")
          .where("users.level", "<", 5) // Using a concrete value instead of unparameterized column reference
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT * FROM users INNER JOIN users AS mgr ON users.manager_id = mgr.id WHERE users.department = $1 AND mgr.department = $2 AND users.level < $3",
          ["Engineering", "Engineering", 5]
        );
      });
    });

    describe("Multiple JOINs", () => {
      it("should handle multiple JOINs", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .innerJoin("comments", "posts.id", "comments.post_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id INNER JOIN comments ON posts.id = comments.post_id"
        );
      });

      it("should handle mixed JOIN types", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .leftJoin("posts", "users.id", "posts.user_id")
          .innerJoin("comments", "posts.id", "comments.post_id")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users LEFT JOIN posts ON users.id = posts.user_id INNER JOIN comments ON posts.id = comments.post_id"
        );
      });

      it("should handle multiple JOINs with aliases", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "p.user_id", "p")
          .innerJoin("comments", "p.id", "c.post_id", "c")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN posts AS p ON users.id = p.user_id INNER JOIN comments AS c ON p.id = c.post_id"
        );
      });
    });

    describe("JOINs with complex queries", () => {
      it("should handle JOIN with WHERE and ORDER BY", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .where("active", "=", true)
          .where("published", "=", true)
          .orderBy("name", "DESC")
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1 AND users.published = $2 ORDER BY users.name DESC",
          [true, true]
        );
      });

      it("should handle JOIN with LIMIT and OFFSET", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .limit(10)
          .offset(5)
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id LIMIT 10 OFFSET 5"
        );
      });

      it("should handle JOIN with all query clauses", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .select("users.id", "users.name", "posts.title")
          .innerJoin("posts", "users.id", "posts.user_id")
          .where("active", "=", true)
          .orWhere("id", "IN", [1, 2, 3])
          .orderBy("name", "ASC")
          .limit(20)
          .offset(10)
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT users.id, users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1 OR users.id IN ($2, $3, $4) ORDER BY users.name ASC LIMIT 20 OFFSET 10",
          [true, 1, 2, 3]
        );
      });

      it("should handle JOIN with COUNT", async () => {
        mockPool.setMockResults([{ count: "15" }]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        const count = await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .where("active", "=", true)
          .count();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT COUNT(*) as count FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1",
          [true]
        );
        expect(count).toBe(15);
      });

      it("should handle already qualified columns with JOINs", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );
        await query
          .innerJoin("posts", "users.id", "posts.user_id")
          .where("posts.published", "=", true)
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id WHERE posts.published = $1",
          [true]
        );
      });
    });

    describe("JOINs with table aliases", () => {
      it("should handle table alias for main table", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users",
          undefined,
          "u"
        );
        await query
          .innerJoin("posts", "u.id", "posts.user_id")
          .where("active", "=", true)
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT * FROM users AS u INNER JOIN posts ON u.id = posts.user_id WHERE u.active = $1",
          [true]
        );
      });

      it("should qualify columns with main table alias when present", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users",
          undefined,
          "u"
        );
        await query
          .innerJoin("posts", "u.id", "p.user_id", "p")
          .where("name", "LIKE", "%john%")
          .orderBy("created_at", "DESC")
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT * FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id WHERE u.name LIKE $1 ORDER BY u.created_at DESC",
          ["%john%"]
        );
      });
    });
  });

  describe("Advanced JOIN Operations", () => {
    it("should handle JOINs with complex ON conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("users.id", "users.name", "posts.title")
        .innerJoin("posts", "users.id", "posts.user_id")
        .where("users.active", "=", true)
        .where("posts.published", "=", true)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT users.id, users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1 AND posts.published = $2",
        [true, true]
      );
    });

    it("should handle aggregate functions with JOINs", async () => {
      mockPool.setMockResults([{ total_posts: "5" }]);

      const db = new TypedPg<TestSchema>(mockPool as any);
      await db.raw(
        "SELECT users.id, users.name, COUNT(posts.id) as total_posts FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id, users.name"
      );

      expect(mockPool).toHaveExecutedQuery(
        "SELECT users.id, users.name, COUNT(posts.id) as total_posts FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id, users.name"
      );
    });

    it("should handle multiple aggregate functions with JOINs", async () => {
      mockPool.setMockResults([{
        total_posts: "10",
        avg_likes: "25.5",
        max_comments: "100"
      }]);

      const db = new TypedPg<TestSchema>(mockPool as any);
      await db.raw(
        "SELECT users.id, users.name, COUNT(DISTINCT posts.id) as total_posts, AVG(posts.likes) as avg_likes, MAX(posts.comment_count) as max_comments FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id, users.name HAVING COUNT(posts.id) > $1 ORDER BY avg_likes DESC",
        [5]
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT users.id, users.name, COUNT(DISTINCT posts.id) as total_posts, AVG(posts.likes) as avg_likes, MAX(posts.comment_count) as max_comments FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id, users.name HAVING COUNT(posts.id) > $1 ORDER BY avg_likes DESC",
        [5]
      );
    });

    it("should handle window functions with JOINs", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select(
          "users.id",
          "users.name",
          "posts.title",
          expr("ROW_NUMBER() OVER (PARTITION BY users.id ORDER BY posts.created_at DESC)", "post_number")
        )
        .innerJoin("posts", "users.id", "posts.user_id")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT users.id, users.name, posts.title, ROW_NUMBER() OVER (PARTITION BY users.id ORDER BY posts.created_at DESC) AS post_number FROM users INNER JOIN posts ON users.id = posts.user_id"
      );
    });

    it("should handle JOINs with subqueries", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("users.id", "users.name", "recent_posts.title")
        .innerJoin(
          "(SELECT * FROM posts WHERE created_at >= NOW() - INTERVAL '7 days')",
          "users.id",
          "recent_posts.user_id",
          "recent_posts"
        )
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT users.id, users.name, recent_posts.title FROM users INNER JOIN (SELECT * FROM posts WHERE created_at >= NOW() - INTERVAL '7 days') AS recent_posts ON users.id = recent_posts.user_id"
      );
    });
  });

  describe("GROUP BY and HAVING", () => {
    it("should handle basic GROUP BY", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .groupBy("department")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT department, COUNT(*) AS user_count FROM users GROUP BY department"
      );
    });

    it("should handle GROUP BY with WHERE", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .where("active", "=", true)
        .groupBy("department")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT department, COUNT(*) AS user_count FROM users WHERE active = $1 GROUP BY department",
        [true]
      );
    });

    it("should handle GROUP BY with multiple columns", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", "role", expr("COUNT(*)", "user_count"))
        .groupBy("department", "role")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT department, role, COUNT(*) AS user_count FROM users GROUP BY department, role"
      );
    });

    it("should handle HAVING clause", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .groupBy("department")
        .having("COUNT(*)", ">", 5)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT department, COUNT(*) AS user_count FROM users GROUP BY department HAVING COUNT(*) > $1",
        [5]
      );
    });

    it("should handle HAVING with multiple conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"), expr("AVG(age)", "avg_age"))
        .groupBy("department")
        .having("COUNT(*)", ">", 5)
        .having("AVG(age)", ">=", 30)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT department, COUNT(*) AS user_count, AVG(age) AS avg_age FROM users GROUP BY department HAVING COUNT(*) > $1 AND AVG(age) >= $2",
        [5, 30]
      );
    });

    it("should handle GROUP BY with JOINs", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("users.department", expr("COUNT(DISTINCT posts.id)", "post_count"))
        .innerJoin("posts", "users.id", "posts.user_id")
        .groupBy("users.department")
        .having("COUNT(DISTINCT posts.id)", ">", 10)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT users.department, COUNT(DISTINCT posts.id) AS post_count FROM users INNER JOIN posts ON users.id = posts.user_id GROUP BY users.department HAVING COUNT(DISTINCT posts.id) > $1",
        [10]
      );
    });

    it("should handle GROUP BY with ORDER BY", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .groupBy("department")
        .orderBy("user_count", "DESC")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT department, COUNT(*) AS user_count FROM users GROUP BY department ORDER BY user_count DESC"
      );
    });

    it("should handle GROUP BY with HAVING and ORDER BY", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .groupBy("department")
        .having("COUNT(*)", ">", 10)
        .orderBy("user_count", "DESC")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT department, COUNT(*) AS user_count FROM users GROUP BY department HAVING COUNT(*) > $1 ORDER BY user_count DESC",
        [10]
      );
    });

    it("should handle GROUP BY with LIMIT and OFFSET", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .groupBy("department")
        .orderBy("user_count", "DESC")
        .limit(5)
        .offset(10)
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT department, COUNT(*) AS user_count FROM users GROUP BY department ORDER BY user_count DESC LIMIT 5 OFFSET 10"
      );
    });

    it("should handle HAVING with IN operator", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", expr("COUNT(*)", "user_count"))
        .groupBy("department")
        .having("COUNT(*)", "IN", [5, 10, 15])
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT department, COUNT(*) AS user_count FROM users GROUP BY department HAVING COUNT(*) IN ($1, $2, $3)",
        [5, 10, 15]
      );
    });

    it("should throw error when using HAVING without GROUP BY", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("department", expr("COUNT(*)", "user_count"))
          .having("COUNT(*)", ">", 5)
          .execute();
      }).rejects.toThrow("HAVING clause requires GROUP BY");
    });
  });

  describe("Aggregate Functions", () => {
    it("should handle MIN aggregate", async () => {
      mockPool.setMockResults([{ min: 10 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.min("age").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT MIN(age) AS min FROM users"
      );
      expect(result[0].min).toBe(10);
    });

    it("should handle MAX aggregate", async () => {
      mockPool.setMockResults([{ max: 50 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.max("age").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT MAX(age) AS max FROM users"
      );
      expect(result[0].max).toBe(50);
    });

    it("should handle SUM aggregate", async () => {
      mockPool.setMockResults([{ sum: 1000 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.sum("age").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT SUM(age) AS sum FROM users"
      );
      expect(result[0].sum).toBe(1000);
    });

    it("should handle AVG aggregate", async () => {
      mockPool.setMockResults([{ avg: 25.5 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.avg("age").execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT AVG(age) AS avg FROM users"
      );
      expect(result[0].avg).toBe(25.5);
    });

    it("should handle aggregate with WHERE clause", async () => {
      mockPool.setMockResults([{ min: 20 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .min("age")
        .where("active", "=", true)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT MIN(age) AS min FROM users WHERE active = $1",
        [true]
      );
    });

    it("should handle aggregate with GROUP BY", async () => {
      mockPool.setMockResults([{ min: 18 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .min("age")
        .groupBy("department")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT MIN(age) AS min FROM users GROUP BY department"
      );
    });

    it("should handle aggregate with JOIN", async () => {
      mockPool.setMockResults([{ max: 100 }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .max("posts.likes")
        .innerJoin("posts", "users.id", "posts.user_id")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT MAX(posts.likes) AS max FROM users INNER JOIN posts ON users.id = posts.user_id"
      );
    });

    it("should handle multiple aggregates using aggregate method", async () => {
      mockPool.setMockResults([{
        total_users: "100",
        avg_age: 25.5,
        min_age: 18,
        max_age: 65
      }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query.aggregate({
        total_users: "COUNT(*)",
        avg_age: "AVG(age)",
        min_age: "MIN(age)",
        max_age: "MAX(age)"
      }).execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT COUNT(*) AS total_users, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM users"
      );
      expect(result[0]).toEqual({
        total_users: "100",
        avg_age: 25.5,
        min_age: 18,
        max_age: 65
      });
    });

    it("should handle aggregate with HAVING clause", async () => {
      mockPool.setMockResults([{
        dept: "Engineering",
        avg_salary: 75000,
        headcount: 10
      }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .aggregate({
          dept: "department",
          avg_salary: "AVG(salary)",
          headcount: "COUNT(*)"
        })
        .groupBy("department")
        .having("COUNT(*)", ">", 5)
        .having("AVG(salary)", ">", 50000)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT department AS dept, AVG(salary) AS avg_salary, COUNT(*) AS headcount FROM users GROUP BY department HAVING COUNT(*) > $1 AND AVG(salary) > $2",
        [5, 50000]
      );
    });

    it("should handle aggregate with GROUP BY and ORDER BY", async () => {
      mockPool.setMockResults([{
        dept: "Engineering",
        total_salary: 1000000
      }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .aggregate({
          dept: "department",
          total_salary: "SUM(salary)"
        })
        .groupBy("department")
        .orderBy("total_salary", "DESC")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT department AS dept, SUM(salary) AS total_salary FROM users GROUP BY department ORDER BY total_salary DESC"
      );
    });

    it("should handle aggregate with type validation", async () => {
      interface DeptStats {
        dept: string;
        avg_salary: number;
        headcount: number;
      }

      mockPool.setMockResults([{
        dept: "Engineering",
        avg_salary: 75000,
        headcount: 10
      }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query
        .aggregate<DeptStats>({
          dept: "department",
          avg_salary: "AVG(salary)",
          headcount: "COUNT(*)"
        })
        .groupBy("department")
        .execute();

      expect(result[0].avg_salary).toBe(75000);
      expect(result[0].headcount).toBe(10);
      expect(typeof result[0].dept).toBe("string");
    });

    it("should handle aggregate with null/zero results", async () => {
      mockPool.setMockResults([{
        min_salary: null,
        max_salary: null,
        avg_salary: null,
        total_salary: 0,
        headcount: 0
      }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      const result = await query
        .aggregate({
          min_salary: "MIN(salary)",
          max_salary: "MAX(salary)",
          avg_salary: "AVG(salary)",
          total_salary: "COALESCE(SUM(salary), 0)",
          headcount: "COUNT(*)"
        })
        .where("department", "=", "NonexistentDept")
        .execute();

      expect(result[0].min_salary).toBeNull();
      expect(result[0].max_salary).toBeNull();
      expect(result[0].avg_salary).toBeNull();
      expect(result[0].total_salary).toBe(0);
      expect(result[0].headcount).toBe(0);
    });

    it("should handle aggregate with complex expressions", async () => {
      mockPool.setMockResults([{
        dept: "Engineering",
        avg_tenure: 5.5,
        senior_count: 8,
        salary_variance: 1500.75
      }]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .aggregate({
          dept: "department",
          avg_tenure: "AVG(EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM hire_date))",
          senior_count: "COUNT(CASE WHEN role = 'senior' THEN 1 END)",
          salary_variance: "VARIANCE(salary)"
        })
        .groupBy("department")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT department AS dept, AVG(EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM hire_date)) AS avg_tenure, COUNT(CASE WHEN role = 'senior' THEN 1 END) AS senior_count, VARIANCE(salary) AS salary_variance FROM users GROUP BY department"
      );
    });
  });

  describe("Window Functions", () => {
    it("should handle basic ROW_NUMBER()", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department")
        .rowNumber(["department"], [["salary", "DESC"]])
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as window_1");
    });

    it("should handle RANK()", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department")
        .rank(["department"], [["salary", "DESC"]])
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("RANK() OVER (PARTITION BY department ORDER BY salary DESC) as window_1");
    });

    it("should handle DENSE_RANK()", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department")
        .denseRank(["department"], [["salary", "DESC"]])
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as window_1");
    });

    it("should handle LAG()", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department", "salary")
        .lag("salary", 1, 0, ["department"])
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("LAG(salary, 1, 0) OVER (PARTITION BY department) as window_1");
    });

    it("should handle LEAD()", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department", "salary")
        .lead("salary", 1, 0, ["department"])
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("LEAD(salary, 1, 0) OVER (PARTITION BY department) as window_1");
    });

    it("should handle multiple window functions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department", "salary")
        .rowNumber(["department"], [["salary", "DESC"]])
        .lag("salary", 1, 0, ["department"])
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as window_1");
      expect(sql).toContain("LAG(salary, 1, 0) OVER (PARTITION BY department) as window_2");
    });

    it("should handle custom window function", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("name", "department")
        .window(
          "FIRST_VALUE(salary)",
          "PARTITION BY department ORDER BY salary DESC"
        )
        .execute();

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) as window_1");
    });
  });

  describe("Expression security validation", () => {
    describe("aggregate() validation", () => {
      it("should reject expressions with semicolons", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*); DROP TABLE users; --"
          });
        }).toThrow(/cannot contain semicolons/);
      });

      it("should reject expressions with SQL comments", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*) -- comment"
          });
        }).toThrow(/cannot contain SQL comments/);
      });

      it("should reject expressions with multi-line comments", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*) /* comment */"
          });
        }).toThrow(/cannot contain multi-line SQL comments/);
      });

      it("should reject expressions with DROP keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "DROP TABLE users"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with DELETE keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "DELETE FROM users"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with INSERT keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "INSERT INTO admin VALUES (1)"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with UPDATE keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "UPDATE users SET admin = true"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with CREATE keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "CREATE TABLE evil (id INT)"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with ALTER keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "ALTER TABLE users ADD admin BOOLEAN"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with TRUNCATE keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "TRUNCATE TABLE users"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with GRANT keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "GRANT ALL ON users TO attacker"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with REVOKE keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "REVOKE SELECT ON users FROM public"
          });
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject expressions with UNION keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "1 UNION SELECT password FROM admin"
          });
        }).toThrow(/cannot contain UNION statements/);
      });

      it("should reject expressions with invalid backslash escapes", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*)\\n"
          });
        }).toThrow(/invalid escape sequences/);
      });

      it("should reject expressions with backslash attempts", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*)\\\\"
          });
        }).toThrow(/invalid escape sequences/);
      });

      it("should reject expressions with suspicious quote patterns - OR injection", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*) WHERE name = '' OR '1'='1'"
          });
        }).toThrow(/suspicious quote patterns/);
      });

      it("should reject expressions with suspicious quote patterns - AND injection", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*) WHERE id = '1' AND '1'='1'"
          });
        }).toThrow(/suspicious quote patterns/);
      });

      it("should reject expressions with quote-semicolon injection pattern", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.aggregate({
            total: "COUNT(*) WHERE name = 'admin';"
          });
        }).toThrow(/cannot contain semicolons/);  // Semicolons are checked first
      });

      it("should accept legitimate aggregate expressions", async () => {
        mockPool.setMockResults([{ total: 5, avg_age: 30 }]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        const result = await query.aggregate({
          total: "COUNT(*)",
          avg_age: "AVG(age)",
          max_salary: "MAX(salary)",
          complex: "COALESCE(SUM(salary), 0)"
        }).execute();

        expect(result).toEqual([{ total: 5, avg_age: 30 }]);
      });

      it("should accept complex CASE expressions", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        await query.aggregate({
          senior_count: "COUNT(CASE WHEN role = 'senior' THEN 1 END)"
        }).execute();

        const sql = mockPool.getLastQuery().text;
        expect(sql).toContain("COUNT(CASE WHEN role = 'senior' THEN 1 END)");
      });
    });

    describe("window() validation", () => {
      it("should reject window functions with semicolons", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "ROW_NUMBER(); DROP TABLE users",
            "ORDER BY id"
          );
        }).toThrow(/cannot contain semicolons/);
      });

      it("should reject OVER clause with SQL injection", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "ROW_NUMBER()",
            "ORDER BY id); DROP TABLE users; --"
          );
        }).toThrow(/cannot contain semicolons/);
      });

      it("should reject window functions with DROP keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "DROP TABLE users",
            "ORDER BY id"
          );
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject window functions with DELETE keyword", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "DELETE FROM users",
            "ORDER BY id"
          );
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject window functions with CREATE in OVER clause", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "ROW_NUMBER()",
            "CREATE TABLE evil (id INT)"
          );
        }).toThrow(/cannot contain DDL or DML keywords/);
      });

      it("should reject window functions with UNION in function", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "UNION SELECT password FROM admin",
            "ORDER BY id"
          );
        }).toThrow(/cannot contain UNION/);
      });

      it("should reject window functions with backslash escapes", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "ROW_NUMBER()\\n",
            "ORDER BY id"
          );
        }).toThrow(/invalid escape sequences/);
      });

      it("should reject window functions with suspicious quotes in OVER clause", () => {
        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        expect(() => {
          query.select("name").window(
            "ROW_NUMBER()",
            "PARTITION BY dept WHERE '' OR '1'='1'"
          );
        }).toThrow(/suspicious quote patterns/);
      });

      it("should accept legitimate window functions", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        await query.select("name").window(
          "FIRST_VALUE(salary)",
          "PARTITION BY department ORDER BY salary DESC"
        ).execute();

        const sql = mockPool.getLastQuery().text;
        expect(sql).toContain("FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC)");
      });

      it("should accept PERCENT_RANK window function", async () => {
        mockPool.setMockResults([]);

        const query = new TypedQuery<"users", TestSchema["users"]>(
          mockPool as any,
          "users"
        );

        await query.select("name").window(
          "PERCENT_RANK()",
          "ORDER BY score"
        ).execute();

        const sql = mockPool.getLastQuery().text;
        expect(sql).toContain("PERCENT_RANK() OVER (ORDER BY score)");
      });
    });
  });

  describe("Case sensitivity", () => {
    it("should handle case sensitive equals by default", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.where("name", "=", "John").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name = $1",
        ["John"]
      );
    });

    it("should convert equals to ILIKE when case insensitive", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.ignoreCase().where("name", "=", "John").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name ILIKE $1",
        ["John"]
      );
    });

    it("should convert not equals to NOT ILIKE when case insensitive", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.ignoreCase().where("name", "!=", "John").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name NOT ILIKE $1",
        ["John"]
      );
    });

    it("should convert LIKE to ILIKE when case insensitive", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.ignoreCase().where("name", "LIKE", "%John%").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name ILIKE $1",
        ["%John%"]
      );
    });

    it("should allow switching between case sensitive and insensitive", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .ignoreCase()
        .where("name", "=", "John")
        .matchCase()
        .where("email", "=", "JOHN@example.com")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name ILIKE $1 AND email = $2",
        ["John", "JOHN@example.com"]
      );
    });

    it("should handle case sensitivity with multiple conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .ignoreCase()
        .where("name", "LIKE", "%John%")
        .where("email", "=", "john@example.com")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name ILIKE $1 AND email ILIKE $2",
        ["%John%", "john@example.com"]
      );
    });

    it("should preserve case sensitivity setting after cloning", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      ).ignoreCase();

      const clonedQuery = query.clone();
      await clonedQuery.where("name", "=", "John").execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name ILIKE $1",
        ["John"]
      );
    });
  });

  describe("DISTINCT queries", () => {
    it("should handle basic DISTINCT query", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query.distinct().execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT DISTINCT * FROM users"
      );
    });

    it("should handle DISTINCT with selected columns", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department", "role")
        .distinct()
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT DISTINCT department, role FROM users"
      );
    });

    it("should handle DISTINCT with WHERE clause", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department")
        .distinct()
        .where("active", "=", true)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT DISTINCT department FROM users WHERE active = $1",
        [true]
      );
    });

    it("should handle DISTINCT with ORDER BY", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("department")
        .distinct()
        .orderBy("department", "ASC")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT DISTINCT department FROM users ORDER BY department ASC"
      );
    });

    it("should handle DISTINCT with JOINs", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("users.department", "posts.title")
        .distinct()
        .innerJoin("posts", "users.id", "posts.user_id")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT DISTINCT users.department, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id"
      );
    });

    it("should handle DISTINCT with complex query", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("users.department", "posts.title")
        .distinct()
        .innerJoin("posts", "users.id", "posts.user_id")
        .where("users.active", "=", true)
        .orderBy("users.department", "ASC")
        .limit(10)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT DISTINCT users.department, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1 ORDER BY users.department ASC LIMIT 10",
        [true]
      );
    });
  });

  describe("Complex query combinations", () => {
    it("should handle complex query with all clauses", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );
      await query
        .select("id", "name", "email")
        .where("active", "=", true)
        .where("age", ">=", 18)
        .orWhere("id", "IN", [1, 2, 3])
        .orderBy("name", "ASC")
        .orderBy("created_at", "DESC")
        .limit(10)
        .offset(20)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT id, name, email FROM users WHERE active = $1 AND age >= $2 OR id IN ($3, $4, $5) ORDER BY name ASC, created_at DESC LIMIT 10 OFFSET 20",
        [true, 18, 1, 2, 3]
      );
    });
  });

  describe("Query Composition", () => {
    it("should build queries incrementally with WHERE clauses", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      const q1 = query.where("age", ">", 20);
      await q1.execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE age > $1",
        [20]
      );

      const q2 = q1.where("active", "=", true);
      await q2.execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE age > $1 AND active = $2",
        [20, true]
      );
    });

    it("should build queries incrementally with multiple clauses", async () => {
      mockPool.setMockResults([]);

      const baseQuery = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      ).select("id", "name");

      const q1 = baseQuery.where("active", "=", true);
      const q2 = q1.orderBy("name", "ASC");
      const q3 = q2.limit(10);

      await q3.execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT id, name FROM users WHERE active = $1 ORDER BY name ASC LIMIT 10",
        [true]
      );
    });

    it("should build queries incrementally with JOINs", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      const joinedQuery = query.innerJoin("posts", "users.id", "posts.user_id");
      await joinedQuery.execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id"
      );

      const filteredJoinedQuery = joinedQuery.where("posts.published", "=", true);
      await filteredJoinedQuery.execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id WHERE posts.published = $1",
        [true]
      );
    });

    it("should accumulate WHERE conditions", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      ).where("active", "=", true);

      const q1 = query.where("age", ">", 20);
      const q2 = q1.where("name", "LIKE", "%john%");

      await q2.execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE active = $1 AND age > $2 AND name LIKE $3",
        [true, 20, "%john%"]
      );
    });
  });

  describe("Window Function Column Sanitization", () => {
    it("should sanitize columns in rowNumber partitionBy", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Test that columns are sanitized
      await query
        .select("name")
        .rowNumber(["department"], [["salary", "DESC"]])
        .execute();

      const sql = mockPool.getLastQuery().text;
      expect(sql).toContain("PARTITION BY department");
      expect(sql).toContain("ORDER BY salary DESC");
    });

    it("should reject SQL injection in rowNumber partitionBy", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("name")
          .rowNumber(["dept; DROP TABLE users; --"], [["salary", "DESC"]])
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should reject SQL injection in rowNumber orderBy", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("name")
          .rowNumber(["department"], [["salary; DROP TABLE audit; --", "DESC"]])
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize columns in rank window function", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .select("name")
        .rank(["department"], [["salary", "DESC"]])
        .execute();

      const sql = mockPool.getLastQuery().text;
      expect(sql).toContain("RANK() OVER (PARTITION BY department ORDER BY salary DESC)");
    });

    it("should reject SQL injection in rank partitionBy", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("name")
          .rank(["dept'; DROP TABLE logs; --"], [["salary", "DESC"]])
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize columns in denseRank window function", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .select("name")
        .denseRank(["department"], [["salary", "DESC"]])
        .execute();

      const sql = mockPool.getLastQuery().text;
      expect(sql).toContain("DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC)");
    });

    it("should sanitize column in lag window function", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .select("name")
        .lag("salary", 1, 0, ["department"])
        .execute();

      const sql = mockPool.getLastQuery().text;
      expect(sql).toContain("LAG(salary, 1, 0) OVER (PARTITION BY department)");
    });

    it("should reject SQL injection in lag column", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("name")
          .lag("salary; DROP TABLE metrics; --", 1, 0, ["department"])
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should reject SQL injection in lag partitionBy", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("name")
          .lag("salary", 1, 0, ["dept'; DELETE FROM sessions; --"])
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });

    it("should sanitize column in lead window function", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .select("name")
        .lead("salary", 1, 0, ["department"])
        .execute();

      const sql = mockPool.getLastQuery().text;
      expect(sql).toContain("LEAD(salary, 1, 0) OVER (PARTITION BY department)");
    });

    it("should reject SQL injection in lead column", async () => {
      mockPool.setMockResults([]);

      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await expect(async () => {
        await query
          .select("name")
          .lead("salary/* comment */; DROP TABLE data; --", 1, 0, ["department"])
          .execute();
      }).rejects.toThrow("Invalid SQL identifier");
    });
  });
});
