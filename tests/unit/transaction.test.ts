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

    expect(mockClient.release).toHaveBeenCalled();
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

    expect(mockClient.release).toHaveBeenCalled();
  });

  it("should support nested transactions", async () => {
    const mockUser = createMockUser();
    mockPool.setMockResults([mockUser, mockUser]); // For two inserts

    await db.transaction(async (outerTx) => {
      await outerTx.insert("users", { name: "Outer", email: "outer@example.com", age: 30, active: true });

      await outerTx.transaction(async (innerTx) => {
        await innerTx.insert("users", { name: "Inner", email: "inner@example.com", age: 25, active: true });
      });
    });

    expect(mockPool).toHaveExecutedQueries(6); // 2x(BEGIN + INSERT + COMMIT)
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(2);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(2);
  });

  it("should rollback entire transaction chain on inner error", async () => {
    await expect(db.transaction(async (outerTx) => {
      await outerTx.insert("users", { name: "Outer", email: "outer@example.com", age: 30, active: true });

      await outerTx.transaction(async (innerTx) => {
        throw new Error("Inner transaction error");
      });
    })).rejects.toThrow("Inner transaction error");

    expect(mockPool).toHaveExecutedQueries(4); // BEGIN + INSERT + BEGIN + ROLLBACK
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(0);
    expect(mockPool.getQueriesMatching("ROLLBACK")).toHaveLength(1);
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
});