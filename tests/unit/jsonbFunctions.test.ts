import { TypedQuery } from "../../src";
import { MockPool } from "../test_utils/MockPool";

interface UserProfile {
  name: string;
  age: number;
  address: {
    city: string;
    country: string;
  };
  tags: string[];
}

interface TestRow {
  id: number;
  profile: UserProfile;
  metadata: Record<string, any>;
  settings: Record<string, any>;
}

interface TestSchema {
  users: TestRow;
}

describe("JSONB Functions", () => {
  let pool: MockPool;
  let query: TypedQuery<"users", TestRow, TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery<"users", TestRow, TestSchema>(
      pool as any,
      "users"
    );
  });

  describe("jsonb_set - Update values at path", () => {
    it("should update a nested value with jsonb_set", async () => {
      const updateExpr = query.jsonbSet("profile", ["address", "city"], "San Francisco");

      await query
        .select("id", { column: updateExpr, as: "updated_profile" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_set(profile, '{address,city}', '\"San Francisco\"'::jsonb, true)");
    });

    it("should update with createMissing=false", async () => {
      const updateExpr = query.jsonbSet("profile", ["newField"], "value", false);

      await query
        .select({ column: updateExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_set(profile, '{newField}', '\"value\"'::jsonb, false)");
    });

    it("should update with object values", async () => {
      const newAddress = { city: "Boston", country: "USA" };
      const updateExpr = query.jsonbSet("profile", ["address"], newAddress);

      await query
        .select({ column: updateExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain(`jsonb_set(profile, '{address}', '${JSON.stringify(newAddress)}'::jsonb, true)`);
    });
  });

  describe("jsonb_insert - Insert values", () => {
    it("should insert a value at path", async () => {
      const insertExpr = query.jsonbInsert("profile", ["tags", "0"], "new-tag");

      await query
        .select({ column: insertExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_insert(profile, '{tags,0}', '\"new-tag\"'::jsonb, false)");
    });

    it("should insert after with insertAfter=true", async () => {
      const insertExpr = query.jsonbInsert("profile", ["tags", "1"], "tag", true);

      await query
        .select({ column: insertExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_insert(profile, '{tags,1}', '\"tag\"'::jsonb, true)");
    });
  });

  describe("jsonb delete operations", () => {
    it("should delete a path with jsonbDeletePath", async () => {
      const deleteExpr = query.jsonbDeletePath("profile", ["address", "city"]);

      await query
        .select({ column: deleteExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("profile #- '{address,city}'");
    });

    it("should delete a top-level key with jsonbDeleteKey", async () => {
      const deleteExpr = query.jsonbDeleteKey("metadata", "oldField");

      await query
        .select({ column: deleteExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("metadata - 'oldField'");
    });

    it("should handle keys with special characters", async () => {
      const deleteExpr = query.jsonbDeleteKey("metadata", "field'with'quotes");

      await query
        .select({ column: deleteExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("metadata - 'field''with''quotes'");
    });
  });

  describe("jsonb concatenation and building", () => {
    it("should concatenate JSONB objects", async () => {
      const concatExpr = query.jsonbConcat("profile", { premium: true });

      await query
        .select({ column: concatExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain(`profile || '${JSON.stringify({ premium: true })}'::jsonb`);
    });

    it("should build JSONB object", async () => {
      const buildExpr = query.jsonbBuildObject({ name: "John", age: 30 });

      await query
        .select({ column: buildExpr, as: "newObject" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_build_object('name', 'John', 'age', 30)");
    });

    it("should build JSONB array", async () => {
      const buildExpr = query.jsonbBuildArray([1, 2, 3]);

      await query
        .select({ column: buildExpr, as: "newArray" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_build_array(1, 2, 3)");
    });

    it("should build array with strings", async () => {
      const buildExpr = query.jsonbBuildArray(["a", "b", "c"]);

      await query
        .select({ column: buildExpr, as: "newArray" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_build_array('a', 'b', 'c')");
    });
  });

  describe("jsonb introspection functions", () => {
    it("should get JSONB object keys", async () => {
      const keysExpr = query.jsonbObjectKeys("profile");

      await query
        .select({ column: keysExpr, as: "keys" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_object_keys(profile)");
    });

    it("should get JSONB typeof at root", async () => {
      const typeExpr = query.jsonbTypeof("profile");

      await query
        .select({ column: typeExpr, as: "type" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_typeof(profile)");
    });

    it("should get JSONB typeof at path", async () => {
      const typeExpr = query.jsonbTypeof("profile", ["address"]);

      await query
        .select({ column: typeExpr, as: "type" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_typeof(profile#>'{address}')");
    });

    it("should get JSONB array length", async () => {
      const lengthExpr = query.jsonbArrayLength("profile", ["tags"]);

      await query
        .select({ column: lengthExpr, as: "count" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_array_length(profile#>'{tags}')");
    });

    it("should get array length at root", async () => {
      const lengthExpr = query.jsonbArrayLength("metadata");

      await query
        .select({ column: lengthExpr, as: "count" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_array_length(metadata)");
    });
  });

  describe("Type-safe JSON field access", () => {
    it("should provide type-safe field access with jsonField", async () => {
      // Type-safe: "name" is a valid field in UserProfile
      const fieldExpr = query.jsonField<"profile", UserProfile>("profile", "name");

      await query
        .select({ column: fieldExpr, as: "userName" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("profile->'name'");
    });

    it("should provide type-safe field access as text", async () => {
      const fieldExpr = query.jsonFieldAsText<"profile", UserProfile>("profile", "name");

      await query
        .select({ column: fieldExpr, as: "userName" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("profile->>'name'");
    });

    it("should handle nested path access", async () => {
      const pathExpr = query.jsonPathAsText<"profile", UserProfile, readonly ["address", "city"]>(
        "profile",
        ["address", "city"] as const
      );

      await query
        .select({ column: pathExpr, as: "city" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("profile#>>ARRAY['address','city']");
    });
  });

  describe("Complex JSONB update scenarios", () => {
    it("should chain multiple JSONB operations", async () => {
      // Update city, then add premium flag
      const updated = query.jsonbSet("profile", ["address", "city"], "NYC");
      const withPremium = query.jsonbConcat("profile", { premium: true });

      await query
        .select(
          { column: updated, as: "updated_profile" },
          { column: withPremium, as: "with_premium" }
        )
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("jsonb_set(profile");
      expect(executedQuery.text).toContain("profile || ");
    });

    it("should combine JSONB functions with WHERE clauses", async () => {
      await query
        .where(query.jsonFieldAsText("profile", "name"), "=", "John")
        .select({ column: query.jsonbObjectKeys("profile"), as: "keys" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("WHERE profile->>'name' = $1");
      expect(executedQuery.text).toContain("jsonb_object_keys(profile)");
      expect(executedQuery.values).toEqual(["John"]);
    });

    it("should work with JSON containment operators", async () => {
      const searchData = { name: "John" };

      await query
        .containsJson("profile", searchData)
        .select({ column: query.jsonbTypeof("profile", ["address"]), as: "addrType" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("profile @> $1::jsonb");
      expect(executedQuery.text).toContain("jsonb_typeof(profile#>'{address}')");
      expect(executedQuery.values).toEqual([JSON.stringify(searchData)]);
    });
  });

  describe("Edge cases and special characters", () => {
    it("should handle string values with quotes in jsonbSet", async () => {
      const updateExpr = query.jsonbSet("profile", ["note"], 'Text with "quotes"');

      await query
        .select({ column: updateExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Quotes should be escaped
      expect(executedQuery.text).toContain('Text with \\"quotes\\"');
    });

    it("should handle empty path arrays", async () => {
      const typeExpr = query.jsonbTypeof("profile", []);

      await query
        .select({ column: typeExpr, as: "type" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Empty path should use root level
      expect(executedQuery.text).toContain("jsonb_typeof(profile)");
    });

    it("should handle numeric values in paths", async () => {
      const pathExpr = query.jsonPath("profile", ["tags", "0"]);

      await query
        .select({ column: pathExpr, as: "firstTag" })
        .execute();

      const executedQuery = pool.getLastQuery();
      expect(executedQuery.text).toContain("profile#>ARRAY['tags','0']");
    });

    it("should escape single quotes in jsonbSet to prevent SQL injection", async () => {
      const maliciousValue = "value'; DROP TABLE users; --";
      const updateExpr = query.jsonbSet("profile", ["note"], maliciousValue);

      await query
        .select({ column: updateExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Single quotes should be escaped with double single quotes
      expect(executedQuery.text).toContain("value''; DROP TABLE users; --");
      // The escaped quotes make it safe - verify the full escaped sequence
      expect(executedQuery.text).toContain("'\"value''; DROP TABLE users; --\"'::jsonb");
    });

    it("should escape single quotes in jsonbInsert to prevent SQL injection", async () => {
      const maliciousValue = "tag'; DELETE FROM posts WHERE 'x'='x";
      const insertExpr = query.jsonbInsert("profile", ["tags", "0"], maliciousValue);

      await query
        .select({ column: insertExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Single quotes should be escaped
      expect(executedQuery.text).toContain("tag''; DELETE FROM");
      expect(executedQuery.text).toContain("WHERE ''x''=''x");
    });

    it("should escape single quotes in jsonbConcat to prevent SQL injection", async () => {
      const maliciousObj = { field: "value'; DROP TABLE logs; --" };
      const concatExpr = query.jsonbConcat("profile", maliciousObj);

      await query
        .select({ column: concatExpr, as: "updated" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Single quotes in the JSON should be escaped
      expect(executedQuery.text).toContain("value''; DROP TABLE logs; --");
      // Verify the full escaped JSON string with ::jsonb cast
      expect(executedQuery.text).toContain("'{\"field\":\"value''; DROP TABLE logs; --\"}'::jsonb");
    });

    it("should escape single quotes in jsonbBuildObject to prevent SQL injection", async () => {
      const maliciousData = { key: "value'; TRUNCATE TABLE audit; --" };
      const buildExpr = query.jsonbBuildObject(maliciousData);

      await query
        .select({ column: buildExpr, as: "built" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Single quotes in string values should be escaped
      expect(executedQuery.text).toContain("value''; TRUNCATE TABLE audit; --");
    });

    it("should escape single quotes in jsonbBuildArray to prevent SQL injection", async () => {
      const maliciousArray = ["item1", "item2'; DELETE FROM users; --", "item3"];
      const buildExpr = query.jsonbBuildArray(maliciousArray);

      await query
        .select({ column: buildExpr, as: "built" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Single quotes should be escaped
      expect(executedQuery.text).toContain("item2''; DELETE FROM users; --");
    });

    it("should handle nested objects with single quotes in jsonbBuildObject", async () => {
      const nestedData = {
        user: { name: "O'Brien", note: "It's a test" },
        comment: "Don't panic"
      };
      const buildExpr = query.jsonbBuildObject(nestedData);

      await query
        .select({ column: buildExpr, as: "built" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // All single quotes should be properly escaped
      expect(executedQuery.text).toContain("O''Brien");
      expect(executedQuery.text).toContain("It''s");
      expect(executedQuery.text).toContain("Don''t");
    });

    it("should handle nested objects in jsonbBuildArray", async () => {
      const nestedArray = [
        { name: "Test" },
        { desc: "It's working" },
        42
      ];
      const buildExpr = query.jsonbBuildArray(nestedArray);

      await query
        .select({ column: buildExpr, as: "built" })
        .execute();

      const executedQuery = pool.getLastQuery();
      // Objects should be JSON-stringified and single quotes escaped
      expect(executedQuery.text).toContain("It''s working");
      expect(executedQuery.text).toContain("::jsonb");
    });
  });
});
