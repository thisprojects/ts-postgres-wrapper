import { TypedPg } from "../../src/index";
import {
  MockPool,
  TestSchema,
  createMockUser,
  createMockPost,
} from "../test_utils";

describe("TypedPg CRUD Operations", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  describe("INSERT operations", () => {
    it("should insert single record", async () => {
      const mockUser = createMockUser();
      mockPool.setMockResults([mockUser]);

      const result = await db.insert("users", {
        name: "John",
        email: "john@example.com",
        age: 30,
        active: true,
      });

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("INSERT INTO users");
      expect(mockPool.getLastQuery().text).toContain("RETURNING *");
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual(mockUser);
    });

    it("should insert multiple records", async () => {
      const mockUsers = [
        createMockUser({ id: 1, name: "John", email: "john@example.com" }),
        createMockUser({ id: 2, name: "Jane", email: "jane@example.com" }),
      ];
      mockPool.setMockResults(mockUsers);

      const result = await db.insert("users", [
        { name: "John", email: "john@example.com", age: 30, active: true },
        { name: "Jane", email: "jane@example.com", age: 25, active: true },
      ]);

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain(
        "VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"
      );
      expect(result).toHaveLength(2);
      expect(result).toEqual(mockUsers);
    });

    it("should handle empty array insert", async () => {
      const result = await db.insert("users", []);

      expect(mockPool).toHaveExecutedQueries(0);
      expect(result).toHaveLength(0);
    });

    it("should handle array fields correctly", async () => {
      const mockPost = createMockPost();
      mockPool.setMockResults([mockPost]);

      const result = await db.insert("posts", {
        user_id: 1,
        title: "Test Post",
        content: "Test content",
        published: true,
        tags: ["test", "jest"],
      });

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("INSERT INTO posts");
      expect(mockPool.getLastQuery().values).toEqual(
        expect.arrayContaining([["test", "jest"]])
      );
      expect(result[0]).toEqual(mockPost);
    });
  });

  describe("UPDATE operations", () => {
    it("should update records with WHERE clause", async () => {
      const updatedUser = createMockUser({
        id: 1,
        name: "John Updated",
        active: false,
      });
      mockPool.setMockResults([updatedUser]);

      const result = await db.update(
        "users",
        { name: "John Updated", active: false },
        { id: 1 }
      );

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("UPDATE users SET");
      expect(mockPool.getLastQuery().text).toContain("WHERE id = $");
      expect(mockPool.getLastQuery().text).toContain("RETURNING *");
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual(updatedUser);
    });

    it("should update multiple fields", async () => {
      const updatedUser = createMockUser({
        name: "John Updated",
        email: "john.updated@example.com",
        age: 31,
        active: false,
      });
      mockPool.setMockResults([updatedUser]);

      const result = await db.update(
        "users",
        {
          name: "John Updated",
          email: "john.updated@example.com",
          age: 31,
          active: false,
        },
        { id: 1 }
      );

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("UPDATE users SET");
      expect(mockPool.getLastQuery().text).toContain("name = $");
      expect(mockPool.getLastQuery().text).toContain("email = $");
      expect(mockPool.getLastQuery().text).toContain("age = $");
      expect(mockPool.getLastQuery().text).toContain("active = $");
      expect(result[0]).toEqual(updatedUser);
    });

    it("should update with complex WHERE clause", async () => {
      const updatedUsers = [createMockUser({ active: false })];
      mockPool.setMockResults(updatedUsers);

      const result = await db.update(
        "users",
        { active: false },
        { age: 30, email: "john@example.com" }
      );

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("WHERE age = $");
      expect(mockPool.getLastQuery().text).toContain("AND email = $");
      expect(result).toEqual(updatedUsers);
    });
  });

  describe("UPSERT operations", () => {
    it("should handle single row upsert", async () => {
      const mockUser = createMockUser();
      mockPool.setMockResults([mockUser]);

      await db.upsert(
        "users",
        { id: 1, email: "test@example.com", name: "Test User" },
        ["email"]
      );

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("INSERT INTO users");
      expect(sql).toContain("ON CONFLICT (email)");
      expect(sql).toContain("RETURNING *");
      expect(mockPool.getLastQuery().values).toEqual([1, "test@example.com", "Test User"]);
    });

    it("should handle multiple row upsert", async () => {
      const mockUsers = [createMockUser(), createMockUser()];
      mockPool.setMockResults(mockUsers);

      await db.upsert(
        "users",
        [
          { id: 1, email: "test1@example.com", name: "Test User 1" },
          { id: 2, email: "test2@example.com", name: "Test User 2" }
        ],
        ["email"]
      );

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("INSERT INTO users");
      expect(sql).toContain("VALUES ($1, $2, $3), ($4, $5, $6)");
      expect(sql).toContain("ON CONFLICT (email)");
      expect(mockPool.getLastQuery().values).toEqual([
        1, "test1@example.com", "Test User 1",
        2, "test2@example.com", "Test User 2"
      ]);
    });

    it("should handle multiple conflict columns", async () => {
      const mockUser = createMockUser();
      mockPool.setMockResults([mockUser]);

      await db.upsert(
        "users",
        { id: 1, email: "test@example.com", name: "Test User" },
        ["email", "name"]
      );

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("ON CONFLICT (email, name)");
      expect(sql).toContain("DO UPDATE SET");
      expect(sql).toContain("RETURNING *");
      expect(mockPool.getLastQuery().values).toEqual([1, "test@example.com", "Test User"]);
    });

    it("should handle specific update columns", async () => {
      const mockUser = createMockUser();
      mockPool.setMockResults([mockUser]);

      await db.upsert(
        "users",
        { id: 1, email: "test@example.com", name: "Test User", age: 25 },
        ["email"],
        ["name"] // Only update name on conflict
      );

      const sql = mockPool.getLastQuery().text.replace(/\s+/g, " ").trim();
      expect(sql).toContain("ON CONFLICT (email)");
      expect(sql).toContain("DO UPDATE SET name = EXCLUDED.name");
      expect(sql).toContain("RETURNING *");
      expect(mockPool.getLastQuery().values).toEqual([1, "test@example.com", "Test User", 25]);
    });

    it("should throw error with no conflict columns", async () => {
      await expect(
        db.upsert("users", { id: 1, name: "Test" }, [])
      ).rejects.toThrow("At least one conflict column must be specified");
    });

    it("should throw error with no update columns", async () => {
      await expect(
        db.upsert(
          "users",
          { id: 1, email: "test@example.com" },
          ["id", "email"] // All columns are in conflict
        )
      ).rejects.toThrow("No columns to update in UPSERT operation");
    });

    it("should return empty array for empty input", async () => {
      const result = await db.upsert("users", [], ["email"]);
      expect(result).toEqual([]);
      expect(mockPool).not.toHaveExecutedQuery(expect.anything());
    });
  });

  describe("DELETE operations", () => {
    it("should delete records with WHERE clause", async () => {
      const deletedUser = createMockUser();
      mockPool.setMockResults([deletedUser]);

      const result = await db.delete("users", { id: 1 });

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("DELETE FROM users");
      expect(mockPool.getLastQuery().text).toContain("WHERE id = $1");
      expect(mockPool.getLastQuery().text).toContain("RETURNING *");
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual(deletedUser);
    });

    it("should delete with complex WHERE clause", async () => {
      const deletedUsers = [createMockUser()];
      mockPool.setMockResults(deletedUsers);

      const result = await db.delete("users", {
        active: false,
        age: 30,
      });

      expect(mockPool).toHaveExecutedQueries(1);
      expect(mockPool.getLastQuery().text).toContain("WHERE active = $");
      expect(mockPool.getLastQuery().text).toContain("AND age = $");
      expect(result).toEqual(deletedUsers);
    });

    it("should return empty array when no records deleted", async () => {
      mockPool.setMockResults([]);

      const result = await db.delete("users", { id: 999 });

      expect(result).toHaveLength(0);
    });
  });

  describe("Batch Operations", () => {
    it("should handle batch with multiple operations", async () => {
      const mockUsers = [
        createMockUser({ id: 1, name: "User 1" }),
        createMockUser({ id: 2, name: "User 2" })
      ];
      mockPool.setMockResults(mockUsers);

      const result = await db.batch({
        insert: [{
          table: "users",
          data: { name: "New User", email: "new@example.com" }
        }],
        update: [{
          table: "users",
          data: { name: "Updated User" },
          where: { id: 1 }
        }],
        delete: [{
          table: "users",
          where: { id: 2 }
        }]
      });

      const queries = mockPool.getQueryLog().map(q => q.text);
      expect(queries).toContain("BEGIN");
      expect(mockPool.getQueriesMatching("INSERT INTO users")).toHaveLength(1);
      expect(mockPool.getQueriesMatching("UPDATE users SET")).toHaveLength(1);
      expect(mockPool.getQueriesMatching("DELETE FROM users WHERE")).toHaveLength(1);
      expect(queries).toContain("COMMIT");

      expect(result.inserted.users).toBeDefined();
      expect(result.updated.users).toBeDefined();
      expect(result.deleted.users).toBeDefined();
    });

    it("should handle batch insert with chunking", async () => {
      const mockResult = Array(5).fill(null).map((_, i) =>
        createMockUser({ id: i + 1, name: `User ${i + 1}` })
      );
      mockPool.setMockResults(mockResult);

      const data = Array(5).fill(null).map((_, i) => ({
        name: `User ${i + 1}`,
        email: `user${i + 1}@example.com`
      }));

      const result = await db.batchInsert("users", data, 2);

      expect(mockPool.getQueryLog()).toHaveLength(1); // Single INSERT with multiple VALUES
      expect(result).toHaveLength(5);
    });

    it("should handle batch update with multiple conditions", async () => {
      // Set up mock responses for each query
      const mockResponses = [
        [], // BEGIN
        [createMockUser({ id: 1, name: "Updated 1" })], // First UPDATE
        [createMockUser({ id: 2, name: "Updated 2" })], // Second UPDATE
        []  // COMMIT
      ];

      let responseIndex = 0;
      const queryLog: Array<{ text: string; values: any[] }> = [];
      mockPool.query = jest.fn().mockImplementation((text, values = []) => {
        queryLog.push({ text, values });
        const result = { rows: mockResponses[responseIndex] };
        responseIndex++;
        return Promise.resolve(result);
      });
      mockPool.getQueryLog = () => queryLog;

      const result = await db.batchUpdate("users", [
        { set: { name: "Updated 1" }, where: { id: 1 } },
        { set: { name: "Updated 2" }, where: { id: 2 } }
      ]);

      const queries = mockPool.getQueryLog().map(q => q.text);
      expect(queries).toContain("BEGIN");
      expect(queries).toContain("COMMIT");
      expect(queries.filter(q => q.includes("UPDATE users SET")).length).toBe(2);
      expect(result).toHaveLength(2);
      expect(result.map(r => r.name).sort()).toEqual(["Updated 1", "Updated 2"]);
    });

    it("should handle batch delete with multiple conditions", async () => {
      // Mock results for each delete
      mockPool.setMockResults([createMockUser({ id: 1 })]);
      mockPool.setMockResults([createMockUser({ id: 2 })]);

      const result = await db.batchDelete("users", [
        { id: 1 },
        { id: 2 }
      ]);

      expect(mockPool).toHaveExecutedQueries(4); // BEGIN + 2 deletes + COMMIT
      expect(result).toHaveLength(2);
    });

    it("should handle empty batch operations", async () => {
      const result = await db.batch({});
      expect(result.inserted).toEqual({});
      expect(result.updated).toEqual({});
      expect(result.deleted).toEqual({});
    });

    it("should handle empty arrays in batch operations", async () => {
      const insertResult = await db.batchInsert("users", []);
      expect(insertResult).toEqual([]);

      const updateResult = await db.batchUpdate("users", []);
      expect(updateResult).toEqual([]);

      const deleteResult = await db.batchDelete("users", []);
      expect(deleteResult).toEqual([]);
    });
  });

  describe("Raw SQL queries", () => {
    it("should execute raw query with parameters", async () => {
      const mockData = [{ user_count: 42, avg_age: 30.5 }];
      mockPool.setMockResults(mockData);

      const result = await db.raw<{ user_count: number; avg_age: number }>(
        "SELECT COUNT(*) as user_count, AVG(age) as avg_age FROM users WHERE active = $1",
        [true]
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT COUNT(*) as user_count, AVG(age) as avg_age FROM users WHERE active = $1",
        [true]
      );
      expect(result).toEqual(mockData);
    });

    it("should execute raw query without parameters", async () => {
      const mockData = [{ total_users: 100 }];
      mockPool.setMockResults(mockData);

      const result = await db.raw<{ total_users: number }>(
        "SELECT COUNT(*) as total_users FROM users"
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT COUNT(*) as total_users FROM users",
        []
      );
      expect(result).toEqual(mockData);
    });
  });

  describe("Table queries", () => {
    it("should create TypedQuery instance", () => {
      const query = db.table("users");

      expect(query).toBeDefined();
      expect(typeof query.where).toBe("function");
      expect(typeof query.select).toBe("function");
      expect(typeof query.execute).toBe("function");
    });

    it("should work with table method chaining", async () => {
      const mockUsers = [createMockUser()];
      mockPool.setMockResults(mockUsers);

      const result = await db
        .table("users")
        .where("active", "=", true)
        .select("id", "name", "email")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT id, name, email FROM users WHERE active = $1",
        [true]
      );
      expect(result).toEqual(mockUsers);
    });
  });

  describe("Error handling", () => {
    it("should propagate query errors", async () => {
      const errorPool = {
        query: jest
          .fn()
          .mockRejectedValue(new Error("Database connection failed")),
      };

      const errorDb = new TypedPg<TestSchema>(errorPool as any);

      await expect(errorDb.table("users").execute()).rejects.toThrow(
        "Database connection failed"
      );
    });

    it("should handle insert errors", async () => {
      const errorPool = {
        query: jest
          .fn()
          .mockRejectedValue(new Error("Unique constraint violation")),
      };

      const errorDb = new TypedPg<TestSchema>(errorPool as any);

      await expect(
        errorDb.insert("users", {
          name: "John",
          email: "john@example.com",
          age: 30,
          active: true,
        })
      ).rejects.toThrow("Unique constraint violation");
    });
  });

  describe("Connection management", () => {
    it("should provide access to underlying pool", () => {
      const pool = db.getPool();
      expect(pool).toBe(mockPool);
    });

    it("should close connections", async () => {
      const endSpy = jest.spyOn(mockPool, "end");

      await db.close();

      expect(endSpy).toHaveBeenCalled();
    });
  });
});
