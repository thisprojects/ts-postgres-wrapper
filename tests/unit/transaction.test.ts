import { TypedPg } from "../../src/index";
import { MockPool, TestSchema, createMockUser } from "../test_utils";

describe("TypedPg Transaction Tests", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  it("should successfully commit transaction", async () => {
    const mockUser = createMockUser();
    mockPool.setMockResults([mockUser]);

    const result = await db.transaction(async (trx) => {
      const user = await trx.insert("users", {
        name: "John",
        email: "john@example.com",
        age: 30,
        active: true,
      });
      return user;
    });

    expect(mockPool).toHaveExecutedQueries(3); // BEGIN + INSERT + COMMIT
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(1);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(1);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual(mockUser);

    // Verify INSERT query structure
    const insertQueries = mockPool.getQueriesMatching("INSERT INTO");
    expect(insertQueries).toHaveLength(1);
    expect(insertQueries[0].text).toMatch(/INSERT INTO users \(name, email, age, active\) VALUES \(\$1, \$2, \$3, \$4\) RETURNING \*/);
    expect(insertQueries[0].values).toEqual(["John", "john@example.com", 30, true]);
  });

  it("should rollback transaction on error", async () => {
    const error = new Error("Test error");

    await expect(db.transaction(async (trx) => {
      throw error;
    })).rejects.toThrow(error);

    expect(mockPool).toHaveExecutedQueries(2); // BEGIN + ROLLBACK
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(1);
    expect(mockPool.getQueriesMatching("ROLLBACK")).toHaveLength(1);
  });

  it("should release client after transaction", async () => {
    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
    };
    jest.spyOn(mockPool, "connect").mockResolvedValue(mockClient);

    await db.transaction(async (trx) => {
      return true;
    });

    // Verify client was released after transaction completed
    expect(mockClient.release).toHaveBeenCalledTimes(1);
    expect(mockClient.release).toHaveBeenCalledWith();

    // Verify order of operations
    const beginQueries = mockPool.getQueriesMatching("BEGIN");
    const commitQueries = mockPool.getQueriesMatching("COMMIT");
    expect(beginQueries).toHaveLength(1);
    expect(commitQueries).toHaveLength(1);
  });

  it("should release client even after error", async () => {
    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
    };
    jest.spyOn(mockPool, "connect").mockResolvedValue(mockClient);

    await expect(db.transaction(async (trx) => {
      throw new Error("Test error");
    })).rejects.toThrow();

    expect(mockClient.release).toHaveBeenCalledTimes(1);
    expect(mockClient.release).toHaveBeenCalledWith();

    // Verify order of operations
    const beginQueries = mockPool.getQueriesMatching("BEGIN");
    const rollbackQueries = mockPool.getQueriesMatching("ROLLBACK");
    expect(beginQueries).toHaveLength(1);
    expect(rollbackQueries).toHaveLength(1);
  });

  it("should support nested transactions", async () => {
    const mockUser = createMockUser();
    mockPool.setMockResults([mockUser, mockUser]); // For two inserts

    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };
    mockPool.connect = jest.fn().mockResolvedValue(mockClient);

    await db.transaction(async (outerTx) => {
      await outerTx.insert("users", { name: "Outer", email: "outer@example.com", age: 30, active: true });

      await outerTx.transaction(async (innerTx) => {
        await innerTx.insert("users", { name: "Inner", email: "inner@example.com", age: 25, active: true });
      });
    });

    expect(mockPool).toHaveExecutedQueries(6); // BEGIN + INSERT + BEGIN + INSERT + COMMIT + COMMIT
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(2);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(2);

    // Verify INSERT queries
    const insertQueries = mockPool.getQueriesMatching("INSERT INTO");
    expect(insertQueries).toHaveLength(2);
    expect(insertQueries[0].text).toMatch(/INSERT INTO users \(name, email, age, active\) VALUES \(\$1, \$2, \$3, \$4\) RETURNING \*/);
    expect(insertQueries[0].values).toEqual(["Outer", "outer@example.com", 30, true]);
    expect(insertQueries[1].text).toMatch(/INSERT INTO users \(name, email, age, active\) VALUES \(\$1, \$2, \$3, \$4\) RETURNING \*/);
    expect(insertQueries[1].values).toEqual(["Inner", "inner@example.com", 25, true]);
  });

  it("should rollback entire transaction chain on inner error", async () => {
    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };
    mockPool.connect = jest.fn().mockResolvedValue(mockClient);
    await expect(db.transaction(async (outerTx) => {
      await outerTx.insert("users", { name: "Outer", email: "outer@example.com", age: 30, active: true });

      await outerTx.transaction(async (innerTx) => {
        throw new Error("Inner transaction error");
      });
    })).rejects.toThrow("Inner transaction error");

    expect(mockPool).toHaveExecutedQueries(5); // BEGIN + INSERT + BEGIN + ROLLBACK + ROLLBACK
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(2);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(0);
    expect(mockPool.getQueriesMatching("ROLLBACK")).toHaveLength(2);
  });

  it("should handle undefined return value from transaction", async () => {
    const result = await db.transaction(async (trx) => {
      await trx.insert("users", {
        name: "Test",
        email: "test@example.com",
        age: 30,
        active: true,
      });
      // Don't return anything
    });

    expect(result).toBeUndefined();
    expect(mockPool).toHaveExecutedQueries(3); // BEGIN + INSERT + COMMIT
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(1);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(1);
  });

  it("should handle invalid client object in transaction", async () => {
    const invalidClient = {
      query: () => { throw new Error("Cannot read properties of undefined"); },
      release: jest.fn()
    };
    mockPool.connect = jest.fn().mockResolvedValue(invalidClient);

    await expect(db.transaction(async (trx) => {
      await trx.insert("users", {
        name: "Test",
        email: "test@example.com",
        age: 30,
        active: true
      });
    })).rejects.toThrow("Cannot read properties of undefined");

    expect(invalidClient.release).toHaveBeenCalled();
  });

  it("should handle database connection errors", async () => {
    const errorPool = {
      connect: jest.fn().mockRejectedValue(new Error("Connection failed")),
      query: jest.fn(),
      release: jest.fn(),
    };

    const errorDb = new TypedPg<TestSchema>(errorPool as any);

    await expect(errorDb.transaction(async (trx) => {
      return true;
    })).rejects.toThrow("Connection failed");

    expect(errorPool.release).not.toHaveBeenCalled();
  });

  it("should preserve options/logger in transaction context", async () => {
    const logEntries: any[] = [];
    const testLogger = {
      log: (level: string, entry: any) => {
        logEntries.push({ level, entry });
      }
    };

    const options = {
      logger: testLogger,
      timeout: 5000,
      retryAttempts: 2,
    };

    const dbWithOptions = new TypedPg<TestSchema>(mockPool as any, undefined, options);
    const mockUser = createMockUser();
    mockPool.setMockResults([mockUser]);

    await dbWithOptions.transaction(async (trx) => {
      // Verify options are preserved in transaction
      expect(trx.getOptions()).toMatchObject({
        logger: testLogger,
        timeout: 5000,
        retryAttempts: 2,
      });
      expect(trx.getLogger()).toBe(testLogger);

      await trx.insert("users", {
        name: "Test",
        email: "test@example.com",
        age: 30,
        active: true,
      });
    });

    // Verify logger was used in transaction
    expect(logEntries.length).toBeGreaterThan(0);
    const insertLog = logEntries.find(e => e.entry.query.includes("INSERT INTO"));
    expect(insertLog).toBeDefined();
    expect(insertLog.level).toBe("debug");
  });
});