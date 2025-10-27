import { TypedPg, DatabaseError, isTransientError, TypedPgOptions, ErrorContext } from "../../src";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
  };
}

describe("Error Handling", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
  });

  describe("DatabaseError class", () => {
    it("should create DatabaseError with all properties", () => {
      const context: ErrorContext = {
        query: "SELECT * FROM users",
        params: [1],
        attempt: 1,
        operation: "query"
      };

      const originalError = new Error("Connection failed");
      const dbError = new DatabaseError(
        "Database connection failed",
        "ECONNREFUSED",
        context,
        originalError
      );

      expect(dbError).toBeInstanceOf(Error);
      expect(dbError).toBeInstanceOf(DatabaseError);
      expect(dbError.message).toBe("Database connection failed");
      expect(dbError.code).toBe("ECONNREFUSED");
      expect(dbError.context).toEqual(context);
      expect(dbError.originalError).toBe(originalError);
      expect(dbError.name).toBe("DatabaseError");
    });

    it("should create DatabaseError without optional fields", () => {
      const dbError = new DatabaseError("Something went wrong");

      expect(dbError.message).toBe("Something went wrong");
      expect(dbError.code).toBeUndefined();
      expect(dbError.context).toBeUndefined();
      expect(dbError.originalError).toBeUndefined();
    });
  });

  describe("isTransientError function", () => {
    it("should identify serialization failures as transient", () => {
      const error = { code: '40001' }; // serialization_failure
      expect(isTransientError(error)).toBe(true);
    });

    it("should identify deadlock as transient", () => {
      const error = { code: '40P01' }; // deadlock_detected
      expect(isTransientError(error)).toBe(true);
    });

    it("should identify connection errors as transient", () => {
      expect(isTransientError({ code: 'ECONNRESET' })).toBe(true);
      expect(isTransientError({ code: 'ETIMEDOUT' })).toBe(true);
      expect(isTransientError({ code: 'ECONNREFUSED' })).toBe(true);
      expect(isTransientError({ code: 'ENOTFOUND' })).toBe(true);
    });

    it("should identify resource errors as transient", () => {
      expect(isTransientError({ code: '53000' })).toBe(true); // insufficient_resources
      expect(isTransientError({ code: '53100' })).toBe(true); // disk_full
      expect(isTransientError({ code: '53200' })).toBe(true); // out_of_memory
      expect(isTransientError({ code: '53300' })).toBe(true); // too_many_connections
    });

    it("should not identify non-transient errors", () => {
      expect(isTransientError({ code: '23505' })).toBe(false); // unique_violation
      expect(isTransientError({ code: '23503' })).toBe(false); // foreign_key_violation
      expect(isTransientError({ code: '42P01' })).toBe(false); // undefined_table
      expect(isTransientError({ code: 'UNKNOWN' })).toBe(false);
    });

    it("should handle null/undefined errors", () => {
      expect(isTransientError(null)).toBe(false);
      expect(isTransientError(undefined)).toBe(false);
      expect(isTransientError({})).toBe(false);
    });
  });

  describe("TypedPgOptions configuration", () => {
    it("should accept timeout configuration", () => {
      const options: TypedPgOptions = {
        timeout: 5000
      };

      const db = new TypedPg<TestSchema>(mockPool as any, undefined, options);
      expect(db.getOptions().timeout).toBe(5000);
    });

    it("should accept retry configuration", () => {
      const options: TypedPgOptions = {
        retryAttempts: 3,
        retryDelay: 500
      };

      const db = new TypedPg<TestSchema>(mockPool as any, undefined, options);
      expect(db.getOptions().retryAttempts).toBe(3);
      expect(db.getOptions().retryDelay).toBe(500);
    });

    it("should accept custom error handler", () => {
      const errorHandler = jest.fn();
      const options: TypedPgOptions = {
        onError: errorHandler
      };

      const db = new TypedPg<TestSchema>(mockPool as any, undefined, options);
      expect(db.getOptions().onError).toBe(errorHandler);
    });

    it("should support backward compatibility with logger parameter", () => {
      const mockLogger = {
        log: jest.fn()
      };

      const db = new TypedPg<TestSchema>(mockPool as any, undefined, mockLogger);
      expect(db.getLogger()).toBe(mockLogger);
    });

    it("should allow updating options after creation", () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      db.setOptions({
        timeout: 10000,
        retryAttempts: 5
      });

      const options = db.getOptions();
      expect(options.timeout).toBe(10000);
      expect(options.retryAttempts).toBe(5);
    });
  });

  describe("Query timeout handling", () => {
    it("should timeout long-running queries", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        timeout: 100 // 100ms timeout
      });

      // Mock a slow query
      mockPool.query = jest.fn().mockImplementation(() =>
        new Promise(resolve => setTimeout(() => resolve({ rows: [] }), 500))
      );

      // Use TypedPg methods directly which go through executeWithLogging
      await expect(async () => {
        await db.raw("SELECT * FROM users");
      }).rejects.toThrow(DatabaseError);

      await expect(async () => {
        await db.raw("SELECT * FROM users");
      }).rejects.toThrow("Query timeout exceeded");
    }, 10000);

    it("should not timeout fast queries", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        timeout: 1000 // 1 second timeout
      });

      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);

      const result = await db.raw<TestSchema["users"]>("SELECT * FROM users");
      expect(result).toHaveLength(1);
    });

    it("should work without timeout configured", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        timeout: 0 // Disabled
      });

      mockPool.setMockResults([{ id: 1, name: "Test", email: "test@example.com" }]);

      const result = await db.raw<TestSchema["users"]>("SELECT * FROM users");
      expect(result).toHaveLength(1);
    });
  });

  describe("Retry logic for transient errors", () => {
    it("should retry on transient errors", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        retryAttempts: 2,
        retryDelay: 10
      });

      let attemptCount = 0;
      mockPool.query = jest.fn().mockImplementation(() => {
        attemptCount++;
        if (attemptCount < 2) {
          const error: any = new Error("Deadlock detected");
          error.code = '40P01'; // deadlock_detected
          throw error;
        }
        return Promise.resolve({ rows: [{ id: 1, name: "Test", email: "test@example.com" }] });
      });

      const result = await db.raw<TestSchema["users"]>("SELECT * FROM users");

      expect(attemptCount).toBe(2);
      expect(result).toHaveLength(1);
      expect(mockPool.query).toHaveBeenCalledTimes(2);
    }, 10000);

    it("should not retry non-transient errors", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        retryAttempts: 3,
        retryDelay: 10
      });

      let attemptCount = 0;
      mockPool.query = jest.fn().mockImplementation(() => {
        attemptCount++;
        const error: any = new Error("Unique constraint violation");
        error.code = '23505'; // unique_violation (not transient)
        throw error;
      });

      await expect(async () => {
        await db.raw("SELECT * FROM users");
      }).rejects.toThrow("Unique constraint violation");

      expect(attemptCount).toBe(1); // Only tried once
      expect(mockPool.query).toHaveBeenCalledTimes(1);
    });

    it("should exhaust all retry attempts before failing", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        retryAttempts: 3,
        retryDelay: 10
      });

      let attemptCount = 0;
      mockPool.query = jest.fn().mockImplementation(() => {
        attemptCount++;
        const error: any = new Error("Connection timeout");
        error.code = 'ETIMEDOUT'; // transient error
        throw error;
      });

      await expect(async () => {
        await db.raw("SELECT * FROM users");
      }).rejects.toThrow("Connection timeout");

      expect(attemptCount).toBe(4); // 1 initial + 3 retries
      expect(mockPool.query).toHaveBeenCalledTimes(4);
    }, 10000);

    it("should work without retry configured", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        retryAttempts: 0 // No retries
      });

      let attemptCount = 0;
      mockPool.query = jest.fn().mockImplementation(() => {
        attemptCount++;
        const error: any = new Error("Transient error");
        error.code = 'ECONNRESET';
        throw error;
      });

      await expect(async () => {
        await db.raw("SELECT * FROM users");
      }).rejects.toThrow("Transient error");

      expect(attemptCount).toBe(1); // No retries
    });
  });

  describe("Custom error handler", () => {
    it("should call custom error handler on query failure", async () => {
      const errorHandler = jest.fn();
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        onError: errorHandler
      });

      mockPool.query = jest.fn().mockRejectedValue(new Error("Query failed"));

      await expect(async () => {
        await db.raw("SELECT * FROM users WHERE id = $1", [1]);
      }).rejects.toThrow("Query failed");

      expect(errorHandler).toHaveBeenCalledTimes(1);
      expect(errorHandler).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          query: expect.stringContaining("SELECT * FROM users"),
          params: [1],
          operation: "query"
        })
      );
    });

    it("should call error handler with retry context", async () => {
      const errorHandler = jest.fn();
      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        onError: errorHandler,
        retryAttempts: 2,
        retryDelay: 10
      });

      let attemptCount = 0;
      mockPool.query = jest.fn().mockImplementation(() => {
        attemptCount++;
        const error: any = new Error("Transient error");
        error.code = 'ECONNRESET';
        throw error;
      });

      await expect(async () => {
        await db.raw("SELECT * FROM users");
      }).rejects.toThrow();

      // Error handler called only once (on final failure, not on retries)
      expect(errorHandler).toHaveBeenCalledTimes(1);
      expect(errorHandler).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({
          attempt: 3 // Final attempt number
        })
      );
    }, 10000);
  });

  describe("Pool error handling", () => {
    it("should handle pool errors", (done) => {
      const errorHandler = jest.fn();
      const mockLogger = {
        log: jest.fn()
      };

      const db = new TypedPg<TestSchema>(mockPool as any, undefined, {
        logger: mockLogger,
        onError: errorHandler
      });

      // Simulate pool error
      const poolError = new Error("Pool connection error");
      (mockPool as any).emit('error', poolError);

      // Give event handler time to execute
      setTimeout(() => {
        expect(mockLogger.log).toHaveBeenCalledWith(
          'error',
          expect.objectContaining({
            query: 'Pool error',
            error: poolError
          })
        );

        expect(errorHandler).toHaveBeenCalledWith(
          poolError,
          expect.objectContaining({
            operation: 'connection'
          })
        );

        done();
      }, 100);
    });
  });

  describe("DatabaseError wrapping", () => {
    it("should wrap errors in DatabaseError", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      const originalError = new Error("Original error");
      (originalError as any).code = 'SOME_CODE';
      mockPool.query = jest.fn().mockRejectedValue(originalError);

      try {
        await db.raw("SELECT * FROM users");
        fail("Should have thrown an error");
      } catch (error) {
        expect(error).toBeInstanceOf(DatabaseError);
        const dbError = error as DatabaseError;
        expect(dbError.message).toBe("Original error");
        expect(dbError.code).toBe('SOME_CODE');
        expect(dbError.originalError).toBe(originalError);
        expect(dbError.context).toMatchObject({
          query: expect.stringContaining("SELECT * FROM users"),
          params: []
        });
      }
    });

    it("should not double-wrap DatabaseError", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      const dbError = new DatabaseError("Already wrapped", "CODE");
      mockPool.query = jest.fn().mockRejectedValue(dbError);

      try {
        await db.raw("SELECT * FROM users");
        fail("Should have thrown an error");
      } catch (error) {
        expect(error).toBe(dbError); // Same instance
      }
    });
  });
});
