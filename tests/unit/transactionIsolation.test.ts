import { TypedPg } from "../../src/index";
import { MockPool, TestSchema, createMockUser } from "../test_utils";

describe("Transaction Isolation", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    mockPool.setMockResults([]);
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  it("should handle concurrent transactions", async () => {
    const mockClient1 = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };

    const mockClient2 = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };

    // Mock first connect() call to return first client, second to return second client
    mockPool.connect = jest.fn()
      .mockResolvedValueOnce(mockClient1)
      .mockResolvedValueOnce(mockClient2);

    // Run two transactions concurrently
    await Promise.all([
      db.transaction(async (tx1) => {
        await tx1.insert("users", {
          name: "User 1",
          email: "user1@example.com",
          age: 30,
          active: true,
          created_at: new Date()
        });
      }),
      db.transaction(async (tx2) => {
        await tx2.insert("users", {
          name: "User 2",
          email: "user2@example.com",
          age: 25,
          active: true,
          created_at: new Date()
        });
      })
    ]);

    // Each transaction should have its own BEGIN and COMMIT
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(2);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(2);

    // Each client should be released
    expect(mockClient1.release).toHaveBeenCalledTimes(1);
    expect(mockClient2.release).toHaveBeenCalledTimes(1);

    // Verify both inserts happened
    const insertQueries = mockPool.getQueriesMatching("INSERT INTO");
    expect(insertQueries).toHaveLength(2);
    const names = insertQueries.map(q => q.values.find(v => typeof v === 'string' && v.startsWith('User')));
    expect(names.sort()).toEqual(["User 1", "User 2"]);
  });

  it("should support nested transactions", async () => {
    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };
    mockPool.connect = jest.fn().mockResolvedValue(mockClient);

    await db.transaction(async (tx1) => {
      await tx1.insert("users", {
        name: "Outer User",
        email: "outer@example.com",
        age: 30,
        active: true,
        created_at: new Date()
      });

      await tx1.transaction(async (tx2) => {
        await tx2.insert("users", {
          name: "Inner User",
          email: "inner@example.com",
          age: 25,
          active: true,
          created_at: new Date()
        });
      });
    });

    // BEGIN + INSERT + BEGIN + INSERT + COMMIT + COMMIT
    expect(mockPool).toHaveExecutedQueries(6);
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(2);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(2);
    expect(mockClient.release).toHaveBeenCalled();

    // Verify inserts
    const insertQueries = mockPool.getQueriesMatching("INSERT INTO");
    expect(insertQueries).toHaveLength(2);
    const names = insertQueries.map(q => q.values.find(v => typeof v === 'string' && (v.includes('Outer') || v.includes('Inner'))));
    expect(names.sort()).toEqual(["Inner User", "Outer User"]);
  });

  it("should rollback nested transactions on error", async () => {
    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };
    mockPool.connect = jest.fn().mockResolvedValue(mockClient);

    await expect(db.transaction(async (tx1) => {
      await tx1.insert("users", {
        name: "Outer User",
        email: "outer@example.com",
        age: 30,
        active: true,
        created_at: new Date()
      });

      await tx1.transaction(async (tx2) => {
        await tx2.insert("users", {
          name: "Inner User",
          email: "inner@example.com",
          age: 25,
          active: true,
          created_at: new Date()
        });

        throw new Error("Inner transaction error");
      });
    })).rejects.toThrow("Inner transaction error");

    // BEGIN + INSERT + BEGIN + INSERT + ROLLBACK + ROLLBACK
    expect(mockPool).toHaveExecutedQueries(6);
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(2);
    expect(mockPool.getQueriesMatching("ROLLBACK")).toHaveLength(2);
    expect(mockClient.release).toHaveBeenCalled();
  });

  it("should handle multiple operations in a transaction", async () => {
    const mockClient = {
      query: mockPool.query.bind(mockPool),
      release: jest.fn(),
      connect: jest.fn(function() { return Promise.resolve(this); })
    };
    mockPool.connect = jest.fn().mockResolvedValue(mockClient);

    await db.transaction(async (tx) => {
      await tx.raw("SET statement_timeout = 5000");

      // Insert
      await tx.insert("users", {
        id: 1,
        name: "Test User",
        email: "test@example.com",
        age: 30,
        active: true,
        created_at: new Date()
      });

      // Update
      await tx.update(
        "users",
        { name: "Updated User" },
        { id: 1 }
      );

      // Query
      await tx.table("users")
        .where("id", "=", 1)
        .execute();

      // Delete
      await tx.delete("users", { id: 1 });
    });

    // BEGIN + SET + INSERT + UPDATE + SELECT + DELETE + COMMIT
    expect(mockPool).toHaveExecutedQueries(7);
    expect(mockPool.getQueriesMatching("BEGIN")).toHaveLength(1);
    expect(mockPool.getQueriesMatching("COMMIT")).toHaveLength(1);
    expect(mockClient.release).toHaveBeenCalled();

    // Verify operations in order
    const queries = mockPool.getQueryLog().map(q => q.text);
    expect(queries).toEqual([
      "BEGIN",
      "SET statement_timeout = 5000",
      expect.stringMatching(/^INSERT INTO users/),
      expect.stringMatching(/^UPDATE users/),
      expect.stringMatching(/^SELECT .* FROM users/),
      expect.stringMatching(/^DELETE FROM users/),
      "COMMIT"
    ]);
  });
});