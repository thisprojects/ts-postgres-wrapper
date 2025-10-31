import { TypedQuery } from "../../src";
import { MockPool } from "../test_utils/MockPool";

interface TestRow {
  id: number;
  data: Record<string, any>;
}

interface TestSchema {
  test_table: TestRow;
}

describe("JSON Path Escaping", () => {
  let pool: MockPool;
  let query: TypedQuery<"test_table", TestRow, TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery<"test_table", TestRow, TestSchema>(
      pool as any,
      "test_table"
    );
  });

  describe("Critical: Single quotes in path literals must be escaped", () => {
    it("should properly escape single quotes in jsonbSet paths", () => {
      // Field name "o'clock" contains a single quote
      // CRITICAL: Must escape to o''clock when embedded in '{...}' literal
      const expr = query.jsonbSet("data", ["time", "o'clock"], "noon");

      // The path literal should have doubled quotes: '{time,o''clock}'
      expect(expr).toContain("'{time,o''clock}'");
      expect(expr).toMatch(/jsonb_set\(data, '\{time,o''clock\}', '"noon"'::jsonb, true\)/);
    });

    it("should handle multiple single quotes in path", () => {
      const expr = query.jsonbSet("data", ["user's", "friend's", "name"], "value");
      expect(expr).toContain("'{user''s,friend''s,name}'");
    });

    it("should properly escape in jsonbInsert", () => {
      const expr = query.jsonbInsert("data", ["field's", "name"], "value");
      expect(expr).toContain("'{field''s,name}'");
      expect(expr).toMatch(/jsonb_insert\(data, '\{field''s,name\}', '"value"'::jsonb, false\)/);
    });

    it("should properly escape in jsonbDeletePath", () => {
      const expr = query.jsonbDeletePath("data", ["user's", "data"]);
      expect(expr).toContain("'{user''s,data}'");
      expect(expr).toMatch(/data #- '\{user''s,data\}'/);
    });

    it("should properly escape in jsonbTypeof with path", () => {
      const expr = query.jsonbTypeof("data", ["field's", "type"]);
      expect(expr).toContain("'{field''s,type}'");
      expect(expr).toMatch(/jsonb_typeof\(data#>'\{field''s,type\}'\)/);
    });

    it("should properly escape in jsonbArrayLength with path", () => {
      const expr = query.jsonbArrayLength("data", ["user's", "items"]);
      expect(expr).toContain("'{user''s,items}'");
      expect(expr).toMatch(/jsonb_array_length\(data#>'\{user''s,items\}'\)/);
    });
  });

  describe("Validation AND escaping work together", () => {
    it("should allow valid field names with single quotes", () => {
      // Single quotes are allowed (common in JSON) but must be escaped
      const expr = query.jsonbSet("data", ["user's_settings"], "value");
      expect(expr).toContain("user''s_settings");
    });

    it("should still block SQL injection even with escaping", () => {
      // Semicolons are blocked by validation
      expect(() => {
        query.jsonbSet("data", ["field; DROP TABLE users"], "value");
      }).toThrow(/dangerous characters/i);
    });

    it("should block SQL keywords", () => {
      expect(() => {
        query.jsonbSet("data", ["field UNION SELECT"], "value");
      }).toThrow(/SQL keywords/i);
    });

    it("should block double quotes", () => {
      expect(() => {
        query.jsonbSet("data", ['field"name'], "value");
      }).toThrow(/dangerous characters/i);
    });
  });

  describe("Edge cases", () => {
    it("should handle field names without special characters", () => {
      const expr = query.jsonbSet("data", ["simple", "field"], "value");
      expect(expr).toContain("'{simple,field}'");
      // No escaping needed - should be same as input
    });

    it("should handle numeric field names", () => {
      const expr = query.jsonbSet("data", ["0", "1", "2"], "value");
      expect(expr).toContain("'{0,1,2}'");
    });

    it("should handle hyphens and underscores", () => {
      const expr = query.jsonbSet("data", ["field-name", "field_name"], "value");
      expect(expr).toContain("'{field-name,field_name}'");
    });
  });

  describe("Integration with jsonField (no path literal, different escaping)", () => {
    it("should escape single quotes in jsonField", () => {
      // jsonField uses -> operator with quoted field name
      const expr = query.jsonField("data", "user's_name");
      // In the -> operator context: data->'user''s_name'
      expect(expr).toBe("data->'user''s_name'");
    });

    it("should escape single quotes in jsonFieldAsText", () => {
      const expr = query.jsonFieldAsText("data", "friend's_email");
      expect(expr).toBe("data->>'friend''s_email'");
    });
  });
});
