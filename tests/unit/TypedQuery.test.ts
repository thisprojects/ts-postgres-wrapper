import { TypedQuery, TypedPg } from "../../src/index";
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
          .select("users.id", "users.name", "mgr.name as manager_name")
          .innerJoin("users", "users.manager_id", "mgr.id", "mgr")
          .where("users.department", "=", "Engineering")
          .orderBy("users.name")
          .execute();

        expect(mockPool).toHaveExecutedQueryWithParams(
          "SELECT users.id, users.name, mgr.name as manager_name FROM users INNER JOIN users AS mgr ON users.manager_id = mgr.id WHERE users.department = $1 ORDER BY users.name ASC",
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
          .select("users.name", "mgr.name as manager_name", "dir.name as director_name")
          .innerJoin("users", "users.manager_id", "mgr.id", "mgr")
          .innerJoin("users", "mgr.manager_id", "dir.id", "dir")
          .execute();

        expect(mockPool).toHaveExecutedQuery(
          "SELECT users.name, mgr.name as manager_name, dir.name as director_name FROM users INNER JOIN users AS mgr ON users.manager_id = mgr.id INNER JOIN users AS dir ON mgr.manager_id = dir.id"
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
          "ROW_NUMBER() OVER (PARTITION BY users.id ORDER BY posts.created_at DESC) as post_number"
        )
        .innerJoin("posts", "users.id", "posts.user_id")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT users.id, users.name, posts.title, ROW_NUMBER() OVER (PARTITION BY users.id ORDER BY posts.created_at DESC) as post_number FROM users INNER JOIN posts ON users.id = posts.user_id"
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
});
