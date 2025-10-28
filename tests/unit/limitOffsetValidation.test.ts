import { TypedQuery } from "../../src/index";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
  };
}

describe("LIMIT and OFFSET Validation", () => {
  let mockPool: MockPool;

  beforeEach(() => {
    mockPool = new MockPool();
    mockPool.setMockResults([]);
  });

  describe("LIMIT validation", () => {
    it("should accept valid positive integers", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.limit(1).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users LIMIT 1");

      mockPool.reset();
      await query.limit(100).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users LIMIT 100");

      mockPool.reset();
      await query.limit(1000000).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users LIMIT 1000000");
    });

    it("should reject zero", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(0);
      }).toThrow("LIMIT value must be positive. Got: 0");
    });

    it("should reject negative numbers", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(-1);
      }).toThrow("LIMIT value must be positive. Got: -1");

      expect(() => {
        query.limit(-100);
      }).toThrow("LIMIT value must be positive. Got: -100");
    });

    it("should reject floats/decimals", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(10.5);
      }).toThrow("LIMIT value must be an integer. Got: 10.5");

      expect(() => {
        query.limit(3.14);
      }).toThrow("LIMIT value must be an integer. Got: 3.14");

      expect(() => {
        query.limit(1.1);
      }).toThrow("LIMIT value must be an integer. Got: 1.1");
    });

    it("should reject NaN", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(NaN);
      }).toThrow("LIMIT value must be a finite number. Got: NaN");
    });

    it("should reject Infinity", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(Infinity);
      }).toThrow("LIMIT value must be a finite number. Got: Infinity");

      expect(() => {
        query.limit(-Infinity);
      }).toThrow("LIMIT value must be a finite number. Got: -Infinity");
    });

    it("should reject excessively large values to prevent DoS", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      const MAX_LIMIT = 10000000;

      // Just at the limit should work
      expect(() => {
        query.limit(MAX_LIMIT);
      }).not.toThrow();

      // One over should fail
      expect(() => {
        query.limit(MAX_LIMIT + 1);
      }).toThrow(/exceeds maximum allowed value of 10000000/);

      expect(() => {
        query.limit(100000000);
      }).toThrow(/exceeds maximum allowed value of 10000000/);
    });

    it("should provide helpful error message for large values", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(50000000);
      }).toThrow(/Large result sets should be paginated using LIMIT and OFFSET/);
    });
  });

  describe("OFFSET validation", () => {
    it("should accept valid non-negative integers including zero", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // Zero is valid for OFFSET
      await query.offset(0).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users OFFSET 0");

      mockPool.reset();
      await query.offset(1).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users OFFSET 1");

      mockPool.reset();
      await query.offset(100).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users OFFSET 100");

      mockPool.reset();
      await query.offset(1000000).execute();
      expect(mockPool).toHaveExecutedQuery("SELECT * FROM users OFFSET 1000000");
    });

    it("should reject negative numbers", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.offset(-1);
      }).toThrow("OFFSET value must be non-negative. Got: -1");

      expect(() => {
        query.offset(-100);
      }).toThrow("OFFSET value must be non-negative. Got: -100");
    });

    it("should reject floats/decimals", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.offset(10.5);
      }).toThrow("OFFSET value must be an integer. Got: 10.5");

      expect(() => {
        query.offset(3.14);
      }).toThrow("OFFSET value must be an integer. Got: 3.14");

      expect(() => {
        query.offset(0.1);
      }).toThrow("OFFSET value must be an integer. Got: 0.1");
    });

    it("should reject NaN", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.offset(NaN);
      }).toThrow("OFFSET value must be a finite number. Got: NaN");
    });

    it("should reject Infinity", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.offset(Infinity);
      }).toThrow("OFFSET value must be a finite number. Got: Infinity");

      expect(() => {
        query.offset(-Infinity);
      }).toThrow("OFFSET value must be a finite number. Got: -Infinity");
    });

    it("should reject excessively large values to prevent DoS", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      const MAX_OFFSET = 100000000;

      // Just at the limit should work
      expect(() => {
        query.offset(MAX_OFFSET);
      }).not.toThrow();

      // One over should fail
      expect(() => {
        query.offset(MAX_OFFSET + 1);
      }).toThrow(/exceeds maximum allowed value of 100000000/);

      expect(() => {
        query.offset(200000000);
      }).toThrow(/exceeds maximum allowed value of 100000000/);
    });

    it("should provide helpful error message for large values", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.offset(150000000);
      }).toThrow(/consider using cursor-based pagination instead/);
    });
  });

  describe("Combined LIMIT and OFFSET", () => {
    it("should validate both when used together", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query.limit(10).offset(20).execute();
      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users LIMIT 10 OFFSET 20"
      );
    });

    it("should reject invalid LIMIT even with valid OFFSET", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(-5).offset(10);
      }).toThrow("LIMIT value must be positive");
    });

    it("should reject invalid OFFSET even with valid LIMIT", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(10).offset(-5);
      }).toThrow("OFFSET value must be non-negative");
    });

    it("should work with complex queries", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      await query
        .where("name", "=", "John")
        .orderBy("id", "DESC")
        .limit(5)
        .offset(10)
        .execute();

      expect(mockPool).toHaveExecutedQueryWithParams(
        "SELECT * FROM users WHERE name = $1 ORDER BY id DESC LIMIT 5 OFFSET 10",
        ["John"]
      );
    });
  });

  describe("Edge cases and type coercion", () => {
    it("should handle string numbers passed as any (runtime safety)", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // In JavaScript, "10" as any could be passed
      // Number.isInteger and Number.isFinite will handle this correctly
      // Note: Number.isFinite checks first, so string will fail that check
      expect(() => {
        query.limit("10" as any);
      }).toThrow("LIMIT value must be a finite number");
    });

    it("should handle very small decimals", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      expect(() => {
        query.limit(10.000001);
      }).toThrow("LIMIT value must be an integer");
    });

    it("should handle MAX_SAFE_INTEGER boundary", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // MAX_SAFE_INTEGER is larger than our MAX_LIMIT, so should fail on MAX_LIMIT check
      expect(() => {
        query.limit(Number.MAX_SAFE_INTEGER);
      }).toThrow(/exceeds maximum allowed value/);
    });

    it("should allow OFFSET of zero (common pattern for first page)", async () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      // This is a common pattern: LIMIT 10 OFFSET 0 for page 1
      await query.limit(10).offset(0).execute();
      expect(mockPool).toHaveExecutedQuery(
        "SELECT * FROM users LIMIT 10 OFFSET 0"
      );
    });
  });

  describe("Immutability", () => {
    it("should not modify original query on limit validation error", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      const baseQuery = query.where("id", "=", 1);

      expect(() => {
        baseQuery.limit(-5);
      }).toThrow();

      // Original query should still work
      expect(() => {
        baseQuery.limit(10);
      }).not.toThrow();
    });

    it("should not modify original query on offset validation error", () => {
      const query = new TypedQuery<"users", TestSchema["users"]>(
        mockPool as any,
        "users"
      );

      const baseQuery = query.where("id", "=", 1);

      expect(() => {
        baseQuery.offset(-5);
      }).toThrow();

      // Original query should still work
      expect(() => {
        baseQuery.offset(10);
      }).not.toThrow();
    });
  });
});
