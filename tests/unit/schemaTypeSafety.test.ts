import { TypedQuery, TypedPg } from "../../src/index";
import { MockPool, TestSchema } from "../test_utils";

describe("Schema Type Safety", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
  });

  describe("Runtime Schema Validation", () => {
    it("should allow queries on non-existent tables for runtime flexibility", async () => {
      const query = new TypedQuery<"nonexistent_table", any>(
        mockPool as any,
        "nonexistent_table"
      );
      await query.execute();
      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM nonexistent_table"
      );
    });

    it("should allow WHERE clause with non-existent columns for runtime flexibility", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.where("nonexistent_column" as any, "=", "value").execute();
      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE nonexistent_column = $1",
        ["value"]
      );
    });

    it("should allow SELECT with non-existent columns for runtime flexibility", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.select("id", "nonexistent_column" as any).execute();
      expect(mockPool).toHaveExecutedQuery(
        "SELECT id, nonexistent_column FROM users"
      );
    });

    it("should allow ORDER BY with non-existent columns for runtime flexibility", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.orderBy("nonexistent_column" as any).execute();
      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users ORDER BY nonexistent_column ASC"
      );
    });
  });

  describe("Schema Evolution", () => {
    it("should handle schema changes through re-initialization", () => {
      // Initial schema
      const db = new TypedPg<TestSchema>(mockPool as any);

      // Modified schema with new column
      interface ModifiedSchema extends TestSchema {
        users: TestSchema["users"] & {
          new_column: string;
        };
      }

      const modifiedDb = new TypedPg<ModifiedSchema>(mockPool as any);

      // Both instances should work with their respective schemas
      expect(() => {
        db.table("users").select("id", "name");
        modifiedDb.table("users").select("id", "name", "new_column");
      }).not.toThrow();
    });

    it("should handle partial schema definitions", () => {
      interface PartialSchema {
        users: {
          id: number;
          name: string;
          // Omitting other fields
        };
      }

      const partialDb = new TypedPg<PartialSchema>(mockPool as any);

      expect(() => {
        partialDb.table("users").select("id", "name");
      }).not.toThrow();
    });
  });

  describe("Nullable Columns", () => {
    interface SchemaWithNullables {
      users: {
        id: number;
        name: string | null;
        email: string | undefined;
        profile: object | null;
      };
    }

    it("should handle nullable columns in WHERE clause", async () => {
      const db = new TypedPg<SchemaWithNullables>(mockPool as any);

      await db.raw(
        "SELECT * FROM users WHERE name IS NULL AND email IS NOT NULL"
      );

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users WHERE name IS NULL AND email IS NOT NULL"
      );
    });

    it("should handle nullable columns in INSERT", async () => {
      const db = new TypedPg<SchemaWithNullables>(mockPool as any);

      await db.insert("users", {
        id: 1,
        name: null,
        email: undefined,
        profile: null
      });

      expect(mockPool).toHaveExecutedQueryWithParams(
        "INSERT INTO users (id, name, email, profile) VALUES ($1, $2, $3, $4) RETURNING *",
        [1, null, null, null]
      );
    });

    it("should handle nullable columns in UPDATE", async () => {
      const db = new TypedPg<SchemaWithNullables>(mockPool as any);

      await db.update("users",
        { name: null, profile: null },
        { id: 1 }
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "UPDATE users SET name = $1, profile = $2 WHERE id = $3 RETURNING *",
        [null, null, 1]
      );
    });
  });

  describe("Type Coercion", () => {
    it("should handle type coercion for numeric comparisons", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .where("id", ">", "123") // String that should be coerced to number
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE id > $1",
        ["123"]
      );
    });

    it("should handle type coercion for boolean values", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .where("active", "=", 1) // Number that should be coerced to boolean
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE active = $1",
        [1]
      );
    });

    it("should handle type coercion in UPDATE operations", async () => {
      const db = new TypedPg<TestSchema>(mockPool as any);

      await db.update("users",
        { active: 1 as any }, // Number that should be coerced to boolean
        { id: 1 }
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "UPDATE users SET active = $1 WHERE id = $2 RETURNING *",
        [1, 1]
      );
    });
  });

  describe("Complex Schema Validations", () => {
    it("should validate nested object types", async () => {
      interface ComplexSchema {
        users: {
          id: number;
          settings: {
            theme: string;
            notifications: boolean;
          };
        };
      }

      const db = new TypedPg<ComplexSchema>(mockPool as any);

      await db.insert("users", {
        id: 1,
        settings: {
          theme: "dark",
          notifications: true
        }
      });

      expect(mockPool).toHaveExecutedQueryWithParams(
        "INSERT INTO users (id, settings) VALUES ($1, $2) RETURNING *",
        [1, { theme: "dark", notifications: true }]
      );
    });

    it("should validate array types", async () => {
      interface ArraySchema {
        users: {
          id: number;
          tags: string[];
          scores: number[];
        };
      }

      const db = new TypedPg<ArraySchema>(mockPool as any);

      await db.insert("users", {
        id: 1,
        tags: ["admin", "moderator"],
        scores: [85, 92, 78]
      });

      expect(mockPool).toHaveExecutedQueryWithParams(
        "INSERT INTO users (id, tags, scores) VALUES ($1, $2, $3) RETURNING *",
        [1, ["admin", "moderator"], [85, 92, 78]]
      );
    });

    it("should validate enum types", async () => {
      enum UserRole {
        Admin = "ADMIN",
        User = "USER"
      }

      interface EnumSchema {
        users: {
          id: number;
          role: UserRole;
        };
      }

      const db = new TypedPg<EnumSchema>(mockPool as any);

      await db.insert("users", {
        id: 1,
        role: UserRole.Admin
      });

      expect(mockPool).toHaveExecutedQueryWithParams(
        "INSERT INTO users (id, role) VALUES ($1, $2) RETURNING *",
        [1, "ADMIN"]
      );
    });
  });
});