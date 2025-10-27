import { TypedPg, TypedQuery } from "../../src/index";
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

  describe("GROUP BY validation", () => {
    let mockPool: MockPool;
    let schema: TestSchema;

    beforeEach(() => {
      mockPool = new MockPool();
      schema = {
        users: {
          id: 1,
          name: "",
          email: "",
          age: 0,
          active: true,
          department: "",
          created_at: new Date()
        },
        posts: {
          id: 1,
          user_id: 1,
          title: "",
          content: "",
          published: false,
          tags: [],
          created_at: new Date()
        },
        comments: {
          id: 1,
          post_id: 1,
          user_id: 1,
          content: "",
          created_at: new Date()
        },
        products: {
          id: 1,
          name: "",
          price: 0,
          category_id: 1,
          in_stock: true
        }
      };
    });

    it("should throw error for empty GROUP BY", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await expect(async () => {
        await query.groupBy().execute();
      }).rejects.toThrow("GROUP BY clause requires at least one column");
    });

    it("should throw error for invalid GROUP BY column", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await expect(async () => {
        await query.groupBy("invalid_column").execute();
      }).rejects.toThrow("Invalid column names in GROUP BY: invalid_column");
    });
  });

  describe("HAVING validation", () => {
    let mockPool: MockPool;
    let schema: TestSchema;

    beforeEach(() => {
      mockPool = new MockPool();
      schema = {
        users: {
          id: 1,
          name: "",
          email: "",
          age: 0,
          active: true,
          department: "",
          created_at: new Date()
        },
        posts: {
          id: 1,
          user_id: 1,
          title: "",
          content: "",
          published: false,
          tags: [],
          created_at: new Date()
        },
        comments: {
          id: 1,
          post_id: 1,
          user_id: 1,
          content: "",
          created_at: new Date()
        },
        products: {
          id: 1,
          name: "",
          price: 0,
          category_id: 1,
          in_stock: true
        }
      };
    });

    it("should throw error for HAVING without GROUP BY", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await expect(async () => {
        await query.having("COUNT(*)", ">", 5).execute();
      }).rejects.toThrow("HAVING clause requires GROUP BY");
    });

    it("should throw error for non-aggregate, non-grouped column in HAVING", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await expect(async () => {
        await query
          .groupBy("department")
          .having("email", "=", "test@example.com")
          .execute();
      }).rejects.toThrow("HAVING clause must reference either an aggregate function or a GROUP BY column: email");
    });

    it("should allow aggregate functions in HAVING", async () => {
      mockPool.setMockResults([]);
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await query
        .groupBy("department")
        .having("COUNT(*)", ">", 5)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users GROUP BY department HAVING COUNT(*) > $1",
        [5]
      );
    });

    it("should allow GROUP BY columns in HAVING", async () => {
      mockPool.setMockResults([]);
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await query
        .groupBy("department")
        .having("department", "=", "Engineering")
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users GROUP BY department HAVING department = $1",
        ["Engineering"]
      );
    });
  });

  describe("ORDER BY validation", () => {
    let mockPool: MockPool;
    let schema: TestSchema;

    beforeEach(() => {
      mockPool = new MockPool();
      schema = {
        users: {
          id: 1,
          name: "",
          email: "",
          age: 0,
          active: true,
          department: "",
          created_at: new Date()
        },
        posts: {
          id: 1,
          user_id: 1,
          title: "",
          content: "",
          published: false,
          tags: [],
          created_at: new Date()
        },
        comments: {
          id: 1,
          post_id: 1,
          user_id: 1,
          content: "",
          created_at: new Date()
        },
        products: {
          id: 1,
          name: "",
          price: 0,
          category_id: 1,
          in_stock: true
        }
      };
    });

    it("should throw error for invalid ORDER BY column", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await expect(async () => {
        await query.orderBy("invalid_column").execute();
      }).rejects.toThrow("Invalid column name in ORDER BY: invalid_column");
    });

    it("should allow aggregate functions in ORDER BY", async () => {
      mockPool.setMockResults([]);
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await query
        .groupBy("department")
        .orderBy("COUNT(*)")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users GROUP BY department ORDER BY COUNT(*) ASC"
      );
    });

    it("should allow window functions in ORDER BY", async () => {
      mockPool.setMockResults([]);
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await query
        .orderBy("ROW_NUMBER() OVER (PARTITION BY department)")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users ORDER BY ROW_NUMBER() OVER (PARTITION BY department) ASC"
      );
    });

    it("should allow GROUP BY columns in ORDER BY", async () => {
      mockPool.setMockResults([]);
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users",
        schema
      );

      await query
        .groupBy("department")
        .orderBy("department")
        .execute();

      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users GROUP BY department ORDER BY department ASC"
      );
    });
  });
});
