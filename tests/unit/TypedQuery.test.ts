import { TypedQuery } from "../../src/index";
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
});
