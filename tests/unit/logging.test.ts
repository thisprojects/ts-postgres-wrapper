import { TypedPg, createTypedPg, QueryLogger, QueryLogEntry, LogLevel, ConsoleLogger, Pool } from "../../src/index";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
  };
}

describe("Query Logging", () => {
  let mockPool: MockPool;
  let logEntries: Array<{ level: LogLevel; entry: QueryLogEntry }>;
  let testLogger: QueryLogger;

  beforeEach(() => {
    mockPool = new MockPool();
    logEntries = [];
    testLogger = {
      log(level: LogLevel, entry: QueryLogEntry): void {
        logEntries.push({ level, entry });
      }
    };
  });

  describe("TypedQuery logging", () => {
    it("should log SELECT queries with duration", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([
        { id: 1, name: "Alice", email: "alice@example.com" }
      ]);

      await db.table("users").where("id", "=", 1).execute();

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].level).toBe("debug");
      expect(logEntries[0].entry.query).toContain("SELECT * FROM users WHERE id = $1");
      expect(logEntries[0].entry.params).toEqual([1]);
      expect(logEntries[0].entry.duration).toBeGreaterThanOrEqual(0);
      expect(logEntries[0].entry.timestamp).toBeInstanceOf(Date);
    });

    it("should log count queries", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([{ count: "5" }]);

      await db.table("users").count();

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].entry.query).toContain("SELECT COUNT(*) as count FROM users");
    });

    it("should log errors with error details", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      const testError = new Error("Database connection failed");

      // Make the mock throw an error
      mockPool.query = async () => {
        throw testError;
      };

      await expect(db.table("users").execute()).rejects.toThrow("Database connection failed");

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].level).toBe("error");
      expect(logEntries[0].entry.error).toBe(testError);
      expect(logEntries[0].entry.duration).toBeGreaterThanOrEqual(0);
    });
  });

  describe("CRUD operation logging", () => {
    it("should log INSERT operations", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([
        { id: 1, name: "Bob", email: "bob@example.com" }
      ]);

      await db.insert("users", { name: "Bob", email: "bob@example.com" });

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].entry.query).toContain("INSERT INTO users");
      expect(logEntries[0].entry.params).toEqual(["Bob", "bob@example.com"]);
    });

    it("should log UPDATE operations", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([
        { id: 1, name: "Bob Updated", email: "bob@example.com" }
      ]);

      await db.update("users", { name: "Bob Updated" }, { id: 1 });

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].entry.query).toContain("UPDATE users");
      expect(logEntries[0].entry.params).toEqual(["Bob Updated", 1]);
    });

    it("should log DELETE operations", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([
        { id: 1, name: "Bob", email: "bob@example.com" }
      ]);

      await db.delete("users", { id: 1 });

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].entry.query).toContain("DELETE FROM users");
      expect(logEntries[0].entry.params).toEqual([1]);
    });

    it("should log raw queries", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([
        { id: 1, name: "Custom", email: "custom@example.com" }
      ]);

      await db.raw("SELECT * FROM users WHERE custom_field = $1", ["value"]);

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].entry.query).toBe("SELECT * FROM users WHERE custom_field = $1");
      expect(logEntries[0].entry.params).toEqual(["value"]);
    });
  });

  describe("Logger management", () => {
    it("should allow setting logger after creation", () => {
      const db = new TypedPg<TestSchema>(mockPool as any);
      expect(db.getLogger()).toBeUndefined();

      db.setLogger(testLogger);
      expect(db.getLogger()).toBe(testLogger);
    });

    it("should allow disabling logger", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);

      // Query with logger
      await db.table("users").execute();
      expect(logEntries).toHaveLength(1);

      // Disable logger
      db.setLogger(undefined);
      await db.table("users").execute();

      // Should still only have 1 entry
      expect(logEntries).toHaveLength(1);
    });

    it("should pass logger to query builders", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);
      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);

      // The logger should be passed to the TypedQuery created by table()
      await db.table("users").execute();

      expect(logEntries).toHaveLength(1);
      expect(db.getLogger()).toBe(testLogger);
    });
  });

  describe("ConsoleLogger", () => {
    let consoleSpy: jest.SpyInstance;

    beforeEach(() => {
      consoleSpy = jest.spyOn(console, "log").mockImplementation();
    });

    afterEach(() => {
      consoleSpy.mockRestore();
    });

    it("should log to console at debug level", () => {
      const logger = new ConsoleLogger("debug");
      const entry: QueryLogEntry = {
        query: "SELECT * FROM users",
        params: [1],
        duration: 10,
        timestamp: new Date()
      };

      logger.log("debug", entry);

      expect(consoleSpy).toHaveBeenCalled();
      const logMessage = consoleSpy.mock.calls[0][0];
      expect(logMessage).toContain("DEBUG");
      expect(logMessage).toContain("SELECT * FROM users");
      expect(logMessage).toContain("10ms");
    });

    it("should respect minimum log level", () => {
      const warnSpy = jest.spyOn(console, "warn").mockImplementation();
      const logger = new ConsoleLogger("warn");
      const entry: QueryLogEntry = {
        query: "SELECT * FROM users",
        params: [],
        duration: 5,
        timestamp: new Date()
      };

      logger.log("debug", entry);
      expect(consoleSpy).not.toHaveBeenCalled();
      expect(warnSpy).not.toHaveBeenCalled();

      logger.log("warn", entry);
      expect(warnSpy).toHaveBeenCalled();

      warnSpy.mockRestore();
    });

    it("should log errors to console.error", () => {
      const errorSpy = jest.spyOn(console, "error").mockImplementation();
      const logger = new ConsoleLogger("debug");
      const entry: QueryLogEntry = {
        query: "SELECT * FROM users",
        params: [],
        duration: 5,
        timestamp: new Date(),
        error: new Error("Test error")
      };

      logger.log("error", entry);

      expect(errorSpy).toHaveBeenCalled();
      errorSpy.mockRestore();
    });
  });

  describe("Performance metrics", () => {
    it("should track query duration", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, testLogger);

      // Add a small delay to the mock
      const originalQuery = mockPool.query.bind(mockPool);
      mockPool.query = async (text: string, values: any[]) => {
        await new Promise(resolve => setTimeout(resolve, 10));
        return originalQuery(text, values);
      };

      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);
      await db.table("users").execute();

      expect(logEntries).toHaveLength(1);
      expect(logEntries[0].entry.duration).toBeGreaterThanOrEqual(10);
    });
  });
});
