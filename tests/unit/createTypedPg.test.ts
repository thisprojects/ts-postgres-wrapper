import { Pool } from "pg";
import { createTypedPg, TypedPg } from "../../src/index";
import { TestSchema } from "../test_utils";

jest.mock("pg");

describe("createTypedPg Factory Function", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should accept an existing Pool instance", () => {
    const PoolMock = Pool as jest.MockedClass<typeof Pool>;
    const pool = new Pool();

    // Reset the mock count after creating our pool
    PoolMock.mockClear();

    const db = createTypedPg<TestSchema>(pool);

    expect(db).toBeInstanceOf(TypedPg);
    expect(db.getPool()).toBe(pool);
    expect(PoolMock).not.toHaveBeenCalled(); // Should not create new Pool after our initial pool creation
  });

  it("should create Pool from connection string", () => {
    const connectionString = "postgresql://user:pass@localhost:5432/db";
    const db = createTypedPg<TestSchema>(connectionString);

    expect(db).toBeInstanceOf(TypedPg);
    expect(Pool).toHaveBeenCalledWith({ connectionString });
  });

  it("should create Pool from config object", () => {
    const config = {
      host: "localhost",
      port: 5432,
      database: "testdb",
      user: "testuser",
      password: "testpass"
    };
    const db = createTypedPg<TestSchema>(config);

    expect(db).toBeInstanceOf(TypedPg);
    expect(Pool).toHaveBeenCalledWith(config);
  });

  it("should pass schema to TypedPg instance", () => {
    const pool = new Pool();
    const schema = {
      users: {
        id: 0,
        name: "",
        email: "",
      }
    };
    const db = createTypedPg(pool, schema);

    expect(db).toBeInstanceOf(TypedPg);
    // Test schema by attempting a typed query
    const query = db.table("users").select("id", "name");
    expect(query).toBeDefined();
  });

  it("should handle Pool creation errors", () => {
    const error = new Error("Invalid config");
    const PoolMock = Pool as jest.MockedClass<typeof Pool>;
    PoolMock.mockImplementation(() => {
      throw error;
    });

    expect(() => {
      createTypedPg({ host: "invalid" });
    }).toThrow("Failed to create database pool: Invalid config");
  });
});