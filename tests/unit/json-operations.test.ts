import { TypedQuery, expr } from "../../src";
import { MockPool } from "../test_utils/MockPool";

interface TestRow {
  id: number;
  data: Record<string, any>;
  jsonbData: Record<string, any>;
  jsonArray: any[];
}

interface TestSchema {
  test_table: TestRow;
}

describe("JSON Operations", () => {
  let pool: MockPool;
  let query: TypedQuery<"test_table", TestRow, TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery<"test_table", TestRow, TestSchema>(
      pool as any,
      "test_table"
    );
  });

  describe("JSON Field Access", () => {
    it("should get JSON object field", async () => {
      await query.where("data->>'name'", "=", "John").execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data->>'name' = $1",
        ["John"]
      );
    });

    it("should get nested JSON field", async () => {
      await query.where("data->'address'->>'city'", "=", "New York").execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data->'address'->>'city' = $1",
        ["New York"]
      );
    });

    it("should use jsonField helper method", () => {
      const field = query.jsonField<{ name: string }>("data", "name");
      expect(field).toBe("data->'name'");
    });

    it("should use jsonFieldAsText helper method", () => {
      const field = query.jsonFieldAsText<{ name: string }>("data", "name");
      expect(field).toBe("data->>'name'");
    });
  });

  describe("JSON Path Operations", () => {
    it("should get JSON value by path", async () => {
      const path = ["address", "city"];
      await query
        .where(query.jsonPathAsText("data", path), "=", "New York")
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data#>>ARRAY['address','city'] = $1",
        ["New York"]
      );
    });

    it("should check if JSON path exists", async () => {
      await query.hasJsonPath("data", ["address", "city"]).execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT * FROM test_table WHERE data#>ARRAY['address','city'] IS NOT NULL"
      );
    });

    it("should use jsonPath helper method", () => {
      const path = query.jsonPath("data", ["address", "city"]);
      expect(path).toBe("data#>ARRAY['address','city']");
    });

    it("should use jsonPathAsText helper method", () => {
      const path = query.jsonPathAsText("data", ["address", "city"]);
      expect(path).toBe("data#>>ARRAY['address','city']");
    });
  });

  describe("JSONB Containment", () => {
    it("should check if JSONB contains value", async () => {
      const searchDoc = { name: "John", age: 30 };
      await query.containsJson("jsonbData", searchDoc).execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData @> $1::jsonb",
        [JSON.stringify(searchDoc)]
      );
    });

    it("should check if JSONB is contained in value", async () => {
      const searchDoc = { name: "John", age: 30, email: "john@example.com" };
      await query.containedInJson("jsonbData", searchDoc).execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData <@ $1::jsonb",
        [JSON.stringify(searchDoc)]
      );
    });
  });

  describe("Key Existence", () => {
    it("should check if key exists", async () => {
      await query.hasJsonKey("jsonbData", "email").execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData ? $1",
        ["email"]
      );
    });

    it("should check if any key exists", async () => {
      await query.hasAnyJsonKey("jsonbData", ["email", "phone"]).execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData ?| $1::text[]",
        [["email", "phone"]]
      );
    });

    it("should check if all keys exist", async () => {
      await query.hasAllJsonKeys("jsonbData", ["name", "email"]).execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData ?& $1::text[]",
        [["name", "email"]]
      );
    });
  });

  describe("JSONPath Queries", () => {
    it("should query with JSONPath", async () => {
      await query
        .jsonPathQuery("jsonbData", "$.store.book[*].author")
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData @? $1",
        ["$.store.book[*].author"]
      );
    });

    it("should match with JSONPath predicate", async () => {
      await query
        .jsonPathMatch("jsonbData", "$.store.book[*].price < 10")
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonbData @@ $1",
        ["$.store.book[*].price < 10"]
      );
    });
  });

  describe("Complex JSON Operations", () => {
    it("should combine multiple JSON conditions", async () => {
      await query
        .where("data->>'type'", "=", "user")
        .where("data->'age'", ">", "21")
        .hasJsonKey("data", "email")
        .containsJson("data", { verified: true })
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data->>'type' = $1 AND data->'age' > $2 AND data ? $3 AND data @> $4::jsonb",
        ["user", "21", "email", JSON.stringify({ verified: true })]
      );
    });

    it("should handle JSON array operations", async () => {
      await query
        .where("jsonArray", "@>", ["tag1", "tag2"])
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE jsonArray @> $1::jsonb",
        [JSON.stringify(["tag1", "tag2"])]
      );
    });

    it("should support OR conditions with JSON", async () => {
      await query
        .where("data->>'status'", "=", "active")
        .orWhere("data->>'status'", "=", "pending")
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data->>'status' = $1 OR data->>'status' = $2",
        ["active", "pending"]
      );
    });
  });

  describe("Aggregate Functions with JSON", () => {
    it("should aggregate JSON values", async () => {
      await query
        .select(
          expr("COUNT(*) FILTER (WHERE data->>'active' = 'true')", "active_count"),
          expr("COUNT(*) FILTER (WHERE data->>'active' = 'false')", "inactive_count"),
          expr("AVG((data->>'age')::int)", "avg_age")
        )
        .execute();

      expect(pool).toHaveExecutedQuery(
        "SELECT COUNT(*) FILTER (WHERE data->>'active' = 'true') AS active_count, " +
        "COUNT(*) FILTER (WHERE data->>'active' = 'false') AS inactive_count, " +
        "AVG((data->>'age')::int) AS avg_age FROM test_table"
      );
    });
  });

  describe("JSON Type Safety", () => {
    interface UserData {
      name: string;
      age: number;
      address: {
        city: string;
        country: string;
      };
      tags: string[];
    }

    it("should provide type safety for JSON field access", () => {
      const field = query.jsonField<UserData>("data", "name");
      const nestedField = query.jsonField<UserData>("data", "address");

      expect(field).toBe("data->'name'");
      expect(nestedField).toBe("data->'address'");
    });

    it("should provide type safety for JSON containment", async () => {
      const partial: Partial<UserData> = {
        name: "John",
        address: { city: "New York", country: "USA" }
      };

      await query.containsJson("data", partial).execute();

      expect(pool).toHaveExecutedQueryWithParams(
        "SELECT * FROM test_table WHERE data @> $1::jsonb",
        [JSON.stringify(partial)]
      );
    });
  });
});