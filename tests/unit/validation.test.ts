import { TypedPg } from "../../src/index";
import { MockPool, TestSchema } from "../test_utils";

describe("WHERE Clause Validation", () => {
  let mockPool: MockPool;
  let db: TypedPg<TestSchema>;

  beforeEach(() => {
    mockPool = new MockPool();
    db = new TypedPg<TestSchema>(mockPool as any);
  });

  describe("UPDATE validation", () => {
    it("should block update with empty WHERE clause", async () => {
      await expect(db.update("users", { active: false }, {})).rejects.toThrow(
        "Update operation requires at least one WHERE condition"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should block update with null WHERE value", async () => {
      await expect(
        db.update("users", { active: false }, { id: null as any })
      ).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should block update with undefined WHERE value", async () => {
      await expect(
        db.update("users", { active: false }, { id: undefined as any })
      ).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should block update with empty data object", async () => {
      await expect(db.update("users", {}, { id: 1 })).rejects.toThrow(
        "Update operation requires at least one column to update"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should allow valid update operations", async () => {
      mockPool.setMockResults([{ id: 1, active: false }]);

      await expect(
        db.update("users", { active: false }, { id: 1 })
      ).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should allow update with multiple WHERE conditions", async () => {
      mockPool.setMockResults([{ id: 1, active: false }]);

      await expect(
        db.update(
          "users",
          { active: false },
          { id: 1, email: "test@example.com" }
        )
      ).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should detect mixed valid and invalid WHERE conditions", async () => {
      await expect(
        db.update(
          "users",
          { active: false },
          { id: 1, email: null as any, name: undefined as any }
        )
      ).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values. Invalid columns: email, name"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });
  });

  describe("DELETE validation", () => {
    it("should block delete with empty WHERE clause", async () => {
      await expect(db.delete("users", {})).rejects.toThrow(
        "Delete operation requires at least one WHERE condition"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should block delete with null WHERE value", async () => {
      await expect(db.delete("users", { id: null as any })).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should block delete with undefined WHERE value", async () => {
      await expect(
        db.delete("users", { id: undefined as any })
      ).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should allow valid delete operations", async () => {
      mockPool.setMockResults([{ id: 1 }]);

      await expect(db.delete("users", { id: 1 })).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should allow delete with multiple WHERE conditions", async () => {
      mockPool.setMockResults([{ id: 1 }]);

      await expect(
        db.delete("users", { id: 1, active: false })
      ).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should detect invalid WHERE conditions in delete", async () => {
      await expect(
        db.delete("users", { id: null as any, email: undefined as any })
      ).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values. Invalid columns: id, email"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });
  });

  describe("WHERE clause validation benefits", () => {
    it("should prevent accidental mass updates", async () => {
      // This test demonstrates the safety benefit
      await expect(db.update("users", { active: false }, {})).rejects.toThrow(
        "Update operation requires at least one WHERE condition"
      );

      // Without validation, this would update ALL users
      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should prevent accidental mass deletes", async () => {
      // This test demonstrates the safety benefit
      await expect(db.delete("users", {})).rejects.toThrow(
        "Delete operation requires at least one WHERE condition"
      );

      // Without validation, this would delete ALL users
      expect(mockPool).toHaveExecutedQueries(0);
    });

    it("should provide clear error messages for developers", async () => {
      try {
        await db.update(
          "users",
          { active: false },
          { id: null as any, email: undefined as any }
        );
        fail("Should have thrown an error");
      } catch (error) {
        expect((error as Error).message).toContain(
          "WHERE conditions cannot have null or undefined values"
        );
        expect((error as Error).message).toContain(
          "Invalid columns: id, email"
        );
      }
    });

    it("should validate before hitting the database for performance", async () => {
      // Validation should happen instantly without database query
      const startTime = Date.now();

      await expect(db.update("users", { active: false }, {})).rejects.toThrow();

      const endTime = Date.now();

      // Validation should be instant (< 10ms)
      expect(endTime - startTime).toBeLessThan(10);
      expect(mockPool).toHaveExecutedQueries(0);
    });
  });

  describe("Edge cases", () => {
    it("should allow WHERE values of 0 (falsy but valid)", async () => {
      mockPool.setMockResults([{ id: 1 }]);

      await expect(
        db.update("users", { active: false }, { age: 0 })
      ).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should allow WHERE values of false (falsy but valid)", async () => {
      mockPool.setMockResults([{ id: 1 }]);

      await expect(
        db.update("users", { age: 25 }, { active: false })
      ).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should allow WHERE values of empty string (falsy but valid)", async () => {
      mockPool.setMockResults([{ id: 1 }]);

      await expect(
        db.update("users", { age: 25 }, { name: "" })
      ).resolves.not.toThrow();

      expect(mockPool).toHaveExecutedQueries(1);
    });

    it("should reject mixed valid falsy and invalid null values", async () => {
      await expect(
        db.update("users", { active: false }, { age: 0, id: null as any })
      ).rejects.toThrow(
        "WHERE conditions cannot have null or undefined values. Invalid columns: id"
      );

      expect(mockPool).toHaveExecutedQueries(0);
    });
  });
});
