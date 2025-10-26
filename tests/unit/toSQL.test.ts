import { TypedPg } from "../../src/index";
import { MockPool, TestSchema } from "../test_utils";

describe("TypedQuery toSQL() Method", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  it("should return basic SELECT query", () => {
    const { query, params } = db.table("users").toSQL();

    expect(query).toBe("SELECT * FROM users");
    expect(params).toEqual([]);
  });

  it("should return query with selected columns", () => {
    const { query, params } = db
      .table("users")
      .select("id", "name", "email")
      .toSQL();

    expect(query).toBe("SELECT id, name, email FROM users");
    expect(params).toEqual([]);
  });

  it("should return query with WHERE clause", () => {
    const { query, params } = db
      .table("users")
      .where("active", "=", true)
      .where("age", ">", 21)
      .toSQL();

    expect(query).toBe("SELECT * FROM users WHERE active = $1 AND age > $2");
    expect(params).toEqual([true, 21]);
  });

  it("should return query with complex conditions", () => {
    const { query, params } = db
      .table("users")
      .select("id", "name")
      .where("active", "=", true)
      .orWhere("age", "IN", [25, 30])
      .orderBy("name", "ASC")
      .limit(10)
      .offset(20)
      .toSQL();

    expect(query).toBe(
      "SELECT id, name FROM users WHERE active = $1 OR age IN ($2, $3) ORDER BY name ASC LIMIT 10 OFFSET 20"
    );
    expect(params).toEqual([true, 25, 30]);
  });

  it("should return query with JOINs", () => {
    const { query, params } = db
      .table("users")
      .select("users.id", "posts.title")
      .innerJoin("posts", "users.id", "posts.user_id")
      .where("users.active", "=", true)
      .toSQL();

    expect(query).toBe(
      "SELECT users.id, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id WHERE users.active = $1"
    );
    expect(params).toEqual([true]);
  });

  it("should return query with table alias", () => {
    const { query, params } = db
      .table("users", "u")
      .select("u.id", "u.name")
      .where("u.active", "=", true)
      .toSQL();

    expect(query).toBe(
      "SELECT u.id, u.name FROM users AS u WHERE u.active = $1"
    );
    expect(params).toEqual([true]);
  });

  it("should return query with multiple JOINs and aliases", () => {
    const { query, params } = db
      .table("users", "u")
      .select("u.id", "p.title", "c.content")
      .innerJoin("posts", "u.id", "posts.user_id", "p")
      .leftJoin("comments", "p.id", "comments.post_id", "c")
      .where("u.active", "=", true)
      .toSQL();

    expect(query).toBe(
      "SELECT u.id, p.title, c.content FROM users AS u " +
      "INNER JOIN posts AS p ON u.id = posts.user_id " +
      "LEFT JOIN comments AS c ON p.id = comments.post_id " +
      "WHERE u.active = $1"
    );
    expect(params).toEqual([true]);
  });

  it("should return query with LIKE operator", () => {
    const { query, params } = db
      .table("users")
      .where("name", "LIKE", "%John%")
      .toSQL();

    expect(query).toBe("SELECT * FROM users WHERE name LIKE $1");
    expect(params).toEqual(["%John%"]);
  });

  it("should return query with multiple ORDER BY", () => {
    const { query, params } = db
      .table("users")
      .orderBy("name", "ASC")
      .orderBy("age", "DESC")
      .toSQL();

    expect(query).toBe("SELECT * FROM users ORDER BY name ASC, age DESC");
    expect(params).toEqual([]);
  });
});