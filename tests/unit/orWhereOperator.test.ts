import { TypedPg } from "../../src/index";
import { MockPool, TestSchema, createMockUser } from "../test_utils";

describe("TypedQuery orWhere with IN operator", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  it("should handle first orWhere with IN operator", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("id", "IN", [1, 2, 3])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE id IN ($1, $2, $3)",
      [1, 2, 3]
    );
  });

  it("should combine multiple orWhere conditions with IN operator", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("age", "=", 25)
      .orWhere("id", "IN", [1, 2])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE age = $1 OR id IN ($2, $3)",
      [25, 1, 2]
    );
  });

  it("should handle empty array with IN operator", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("id", "IN", [])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE id IN ()",
      []
    );
  });

  it("should handle mixed where and orWhere with IN", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .where("active", "=", true)
      .orWhere("id", "IN", [1, 2])
      .orWhere("age", "IN", [25, 30])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE active = $1 OR id IN ($2, $3) OR age IN ($4, $5)",
      [true, 1, 2, 25, 30]
    );
  });

  it("should handle multiple IN conditions with different array sizes", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("id", "IN", [1])
      .orWhere("age", "IN", [25, 30, 35])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE id IN ($1) OR age IN ($2, $3, $4)",
      [1, 25, 30, 35]
    );
  });
});