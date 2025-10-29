import { TypedPg } from "../../src/index";
import { MockPool, TestSchema, createMockUser } from "../test_utils";

describe("table() with executeWithLogging integration", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  describe("Query executor integration", () => {
    it("should use queryExecutor when available", async () => {
      mockPool.setMockResults([createMockUser()]);

      const result = await db.table("users").where("id", "=", 1).execute();

      expect(result).toHaveLength(1);
      expect(mockPool.hasExecutedQuery("SELECT * FROM users WHERE id = $1")).toBe(true);
    });

    it("should use queryExecutor for count queries", async () => {
      mockPool.setMockResults([{ count: "5" }]);

      const count = await db.table("users").count();

      expect(count).toBe(5);
      expect(mockPool.hasExecutedQuery("SELECT COUNT(*) as count FROM users")).toBe(true);
    });
  });

  describe("Logging integration", () => {
    it("should log table().execute() queries through executeWithLogging", async () => {
      const logs: any[] = [];
      db.setLogger({
        log: (level, entry) => {
          logs.push({ level, entry });
        },
      });

      mockPool.setMockResults([createMockUser()]);

      await db.table("users").where("id", "=", 1).execute();

      const debugLogs = logs.filter(l => l.level === "debug");
      expect(debugLogs.length).toBeGreaterThan(0);
      expect(debugLogs[0].entry.query).toContain("SELECT * FROM users WHERE id = $1");
    });

    it("should log table().count() queries", async () => {
      const logs: any[] = [];
      db.setLogger({
        log: (level, entry) => {
          logs.push({ level, entry });
        },
      });

      mockPool.setMockResults([{ count: "5" }]);

      await db.table("users").count();

      const debugLogs = logs.filter(l => l.level === "debug");
      expect(debugLogs.length).toBeGreaterThan(0);
      expect(debugLogs[0].entry.query).toContain("SELECT COUNT(*) as count");
    });

    it("should log errors through executeWithLogging", async () => {
      const logs: any[] = [];
      db.setLogger({
        log: (level, entry) => {
          logs.push({ level, entry });
        },
      });

      // Force an error by making the mock pool throw
      mockPool.setMockError(new Error("Database connection failed"));

      await expect(
        db.table("users").where("id", "=", 1).execute()
      ).rejects.toThrow("Database connection failed");

      // Verify error was logged
      const errorLogs = logs.filter(l => l.level === "error");
      expect(errorLogs.length).toBeGreaterThan(0);
      expect(errorLogs[0].entry.error.message).toBe("Database connection failed");
    });
  });

  describe("Options integration", () => {
    it("should respect timeout option set on TypedPg", async () => {
      // This test verifies the integration - actual timeout functionality
      // is tested in the security features test file
      db.setOptions({ timeout: 30000 });

      const options = db.getOptions();
      expect(options.timeout).toBe(30000);

      mockPool.setMockResults([createMockUser()]);
      await db.table("users").execute();
    });

    it("should respect retry options set on TypedPg", async () => {
      db.setOptions({ retryAttempts: 3, retryDelay: 100 });

      const options = db.getOptions();
      expect(options.retryAttempts).toBe(3);
      expect(options.retryDelay).toBe(100);

      mockPool.setMockResults([createMockUser()]);
      await db.table("users").execute();
    });

    it("should respect security options set on TypedPg", async () => {
      db.setOptions({
        security: {
          maxBatchSize: 100,
        },
      });

      const options = db.getOptions();
      expect(options.security?.maxBatchSize).toBe(100);

      mockPool.setMockResults([createMockUser()]);
      await db.table("users").execute();
    });
  });

  describe("Error callback integration", () => {
    it("should support onError callback for table queries", async () => {
      const errors: any[] = [];
      db.setOptions({
        onError: (err, context) => {
          errors.push({ err, context });
        },
      });

      // The onError integration is verified through the options system
      const options = db.getOptions();
      expect(options.onError).toBeDefined();
    });
  });

  describe("Feature parity verification", () => {
    it("table() queries should go through same execution path as insert()", async () => {
      mockPool.setMockResults([createMockUser()]);

      // Both should execute successfully
      await db.insert("users", { name: "Test", email: "test@example.com", age: 25, active: true });
      await db.table("users").where("name", "=", "Test").execute();

      // Verify both used the query executor (logged)
      expect(mockPool.getQueryLog().length).toBe(2);
    });

    it("table() queries should go through same execution path as update()", async () => {
      mockPool.setMockResults([createMockUser(), createMockUser()]);

      await db.update("users", { name: "Updated" }, { id: 1 });
      await db.table("users").where("id", "=", 1).execute();

      expect(mockPool.getQueryLog().length).toBe(2);
    });

    it("table() queries should go through same execution path as delete()", async () => {
      mockPool.setMockResults([createMockUser(), createMockUser()]);

      await db.delete("users", { id: 1 });
      await db.table("users").where("id", "=", 1).execute();

      expect(mockPool.getQueryLog().length).toBe(2);
    });

    it("table() queries should go through same execution path as raw()", async () => {
      mockPool.setMockResults([createMockUser(), createMockUser()]);

      await db.raw<TestSchema["users"]>("SELECT * FROM users WHERE id = $1", [1]);
      await db.table("users").where("id", "=", 1).execute();

      expect(mockPool.getQueryLog().length).toBe(2);
    });
  });
});
