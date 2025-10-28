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

  it("should handle BETWEEN operator with orWhere", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("age", "BETWEEN", [25, 35])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE age BETWEEN $1 AND $2",
      [25, 35]
    );
  });

  it("should combine BETWEEN with other orWhere conditions", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("age", "BETWEEN", [25, 35])
      .orWhere("id", "=", 100)
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE age BETWEEN $1 AND $2 OR id = $3",
      [25, 35, 100]
    );
  });

  it("should handle IS NULL operator with orWhere", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("email", "IS NULL", null)
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE email IS NULL",
      []
    );
  });

  it("should handle IS NOT NULL operator with orWhere", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("email", "IS NOT NULL", null)
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE email IS NOT NULL",
      []
    );
  });

  it("should combine IS NULL/IS NOT NULL with other conditions", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .where("active", "=", true)
      .orWhere("email", "IS NULL", null)
      .orWhere("age", "BETWEEN", [25, 35])
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE active = $1 OR email IS NULL OR age BETWEEN $2 AND $3",
      [true, 25, 35]
    );
  });

  it("should handle all new operators together", async () => {
    const mockUsers = [createMockUser()];
    mockPool.setMockResults(mockUsers);

    await db.table("users")
      .orWhere("id", "IN", [1, 2, 3])
      .orWhere("age", "BETWEEN", [18, 65])
      .orWhere("email", "IS NOT NULL", null)
      .execute();

    expect(mockPool).toHaveExecutedQueryWithParams(
      "SELECT * FROM users WHERE id IN ($1, $2, $3) OR age BETWEEN $4 AND $5 OR email IS NOT NULL",
      [1, 2, 3, 18, 65]
    );
  });
});