import { TypedPg } from "../../src/index";
import { MockPool } from "../test_utils";

interface AdvancedSchema {
  advanced_types: {
    id: number;
    uuid_col: string;  // UUID stored as string
    date_col: Date;
    timestamp_col: Date;
    json_col: Record<string, any>;
    jsonb_col: Record<string, any>;
    array_text: string[];
    array_int: number[];
    array_json: Record<string, any>[];
    nullable_col: string | null;
  };
}

describe("TypedPg Data Type Handling", () => {
  let mockPool: MockPool;
  let db: TypedPg<AdvancedSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<AdvancedSchema>(mockPool as any);
  });

  describe("UUID handling", () => {
    it("should handle UUID in INSERT", async () => {
      const uuid = "123e4567-e89b-12d3-a456-426614174000";
      mockPool.setMockResults([{ id: 1, uuid_col: uuid }]);

      await db.insert("advanced_types", {
        uuid_col: uuid
      });

      expect(mockPool.getLastQuery().values).toContain(uuid);
    });

    it("should handle UUID in WHERE clause", async () => {
      const uuid = "123e4567-e89b-12d3-a456-426614174000";
      mockPool.setMockResults([{ id: 1, uuid_col: uuid }]);

      await db.table("advanced_types")
        .where("uuid_col", "=", uuid)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE uuid_col = $1",
        [uuid]
      );
    });
  });

  describe("Date handling", () => {
    it("should handle Date objects in INSERT", async () => {
      const date = new Date("2023-01-01");
      mockPool.setMockResults([{ id: 1, date_col: date }]);

      await db.insert("advanced_types", {
        date_col: date
      });

      expect(mockPool.getLastQuery().values).toContain(date);
    });

    it("should handle timestamp comparison in WHERE", async () => {
      const timestamp = new Date("2023-01-01T12:00:00Z");
      mockPool.setMockResults([]);

      await db.table("advanced_types")
        .where("timestamp_col", ">", timestamp)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE timestamp_col > $1",
        [timestamp]
      );
    });
  });

  describe("JSON handling", () => {
    it("should handle JSON object in INSERT", async () => {
      const jsonData = { key: "value", nested: { foo: "bar" } };
      mockPool.setMockResults([{ id: 1, json_col: jsonData }]);

      await db.insert("advanced_types", {
        json_col: jsonData
      });

      expect(mockPool.getLastQuery().values).toContainEqual(jsonData);
    });

    it("should handle JSONB object in WHERE clause", async () => {
      const jsonData = { status: "active" };
      mockPool.setMockResults([]);

      await db.table("advanced_types")
        .where("jsonb_col", "=", jsonData)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE jsonb_col = $1",
        [jsonData]
      );
    });
  });

  describe("Array handling", () => {
    it("should handle text array in INSERT", async () => {
      const textArray = ["one", "two", "three"];
      mockPool.setMockResults([{ id: 1, array_text: textArray }]);

      await db.insert("advanced_types", {
        array_text: textArray
      });

      expect(mockPool.getLastQuery().values).toContainEqual(textArray);
    });

    it("should handle integer array in WHERE with IN operator", async () => {
      const intArray = [1, 2, 3];
      mockPool.setMockResults([]);

      await db.table("advanced_types")
        .where("array_int", "=", intArray)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE array_int = $1",
        [intArray]
      );
    });

    it("should handle array of JSON objects", async () => {
      const jsonArray = [{ id: 1 }, { id: 2 }];
      mockPool.setMockResults([{ id: 1, array_json: jsonArray }]);

      await db.insert("advanced_types", {
        array_json: jsonArray
      });

      expect(mockPool.getLastQuery().values).toContainEqual(jsonArray);
    });
  });

  describe("NULL handling", () => {
    it("should handle NULL in INSERT", async () => {
      mockPool.setMockResults([{ id: 1, nullable_col: null }]);

      await db.insert("advanced_types", {
        nullable_col: null
      });

      expect(mockPool.getLastQuery().values).toContain(null);
    });

    it("should handle NULL in WHERE clause", async () => {
      mockPool.setMockResults([]);

      await db.table("advanced_types")
        .where("nullable_col", "=", null)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE nullable_col = $1",
        [null]
      );
    });

    it("should handle IS NULL in raw query", async () => {
      mockPool.setMockResults([]);

      await db.raw(
        "SELECT * FROM advanced_types WHERE nullable_col IS NULL"
      );

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE nullable_col IS NULL",
        []
      );
    });
  });

  describe("Multiple data types in single query", () => {
    it("should handle mixed data types in complex query", async () => {
      const date = new Date("2023-01-01");
      const uuid = "123e4567-e89b-12d3-a456-426614174000";
      const jsonData = { status: "active" };
      const textArray = ["one", "two"];

      mockPool.setMockResults([]);

      await db.table("advanced_types")
        .where("date_col", ">", date)
        .where("uuid_col", "=", uuid)
        .where("jsonb_col", "=", jsonData)
        .where("array_text", "=", textArray)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM advanced_types WHERE date_col > $1 AND uuid_col = $2 AND jsonb_col = $3 AND array_text = $4",
        [date, uuid, jsonData, textArray]
      );
    });
  });
});