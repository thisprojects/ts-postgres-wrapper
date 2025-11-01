import { TypedQuery } from "../../src/TypedQuery";
import { WindowFunctionBuilder } from "../../src/builders/WindowFunctionBuilder";
import { SetOperationsBuilder } from "../../src/builders/SetOperations";
import { CteBuilder } from "../../src/builders/CteBuilder";
import { DatabaseError } from "../../src/errors";
import { sanitizeSqlIdentifier } from "../../src/utils";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
    data: any;
  };
}

describe("Input Validation DoS Prevention", () => {
  const createQuery = () => {
    return new TypedQuery<"users", TestSchema["users"]>(
      {} as any, // pool not needed for toSQL()
      "users", // table name
      undefined, // schema
      undefined, // table alias
      undefined, // logger
      undefined // query executor
    );
  };

  describe("String Length Limits", () => {
    describe("Column names", () => {
      it("should reject excessively long column names (>63 chars)", () => {
        const longName = "a".repeat(64);

        expect(() => {
          sanitizeSqlIdentifier(longName);
        }).toThrow(/identifier.*exceeds.*63/i);
      });

      it("should accept column names at PostgreSQL limit (63 chars)", () => {
        const maxName = "a".repeat(63);
        expect(() => {
          sanitizeSqlIdentifier(maxName);
        }).not.toThrow(/exceeds/);
      });
    });

    describe("Table names", () => {
      it("should reject excessively long table names (>63 chars)", () => {
        const longTable = "t".repeat(64);

        expect(() => {
          sanitizeSqlIdentifier(longTable);
        }).toThrow(/identifier.*exceeds.*63/i);
      });
    });

    describe("String values in WHERE", () => {
      it("should reject excessively long string values (>10MB)", () => {
        const hugeString = "x".repeat(10 * 1024 * 1024 + 1);

        expect(() => {
          createQuery().where("name", "=", hugeString).toSQL();
        }).toThrow(DatabaseError);

        expect(() => {
          createQuery().where("name", "=", hugeString).toSQL();
        }).toThrow(/parameter.*exceeds maximum.*10.*MB/i);
      });

      it("should accept string values at reasonable limit (10MB)", () => {
        const largeString = "x".repeat(10 * 1024 * 1024);
        const query = createQuery().where("name", "=", largeString);

        expect(() => query.toSQL()).not.toThrow();
      });
    });
  });

  describe("Operator Validation", () => {
    describe("WHERE operators", () => {
      it("should reject invalid comparison operators at runtime", () => {
        const invalidOp = "INVALID" as any;

        expect(() => {
          createQuery().where("id", invalidOp, 1).toSQL();
        }).toThrow(DatabaseError);

        expect(() => {
          createQuery().where("id", invalidOp, 1).toSQL();
        }).toThrow(/invalid operator/i);
      });

      it("should reject SQL injection via operator", () => {
        const maliciousOp = "= 1 OR 1=1 --" as any;

        expect(() => {
          createQuery().where("id", maliciousOp, 1).toSQL();
        }).toThrow(/invalid operator/i);
      });

      it("should accept valid operators", () => {
        const validOps = ["=", "!=", "<>", "<", ">", "<=", ">=", "LIKE", "ILIKE"];

        validOps.forEach((op) => {
          const query = createQuery().where("name", op as any, "test");
          expect(() => query.toSQL()).not.toThrow();
        });
      });
    });

    describe("ORDER BY direction", () => {
      it("should reject invalid sort directions", () => {
        const invalidDir = "INVALID" as any;

        expect(() => {
          createQuery().orderBy("id", invalidDir).toSQL();
        }).toThrow(DatabaseError);

        expect(() => {
          createQuery().orderBy("id", invalidDir).toSQL();
        }).toThrow(/invalid.*direction/i);
      });

      it("should reject SQL injection via sort direction", () => {
        const maliciousDir = "ASC; DROP TABLE users; --" as any;

        expect(() => {
          createQuery().orderBy("id", maliciousDir).toSQL();
        }).toThrow(/invalid.*direction/i);
      });

      it("should accept valid sort directions", () => {
        const query1 = createQuery().orderBy("id", "ASC");
        const query2 = createQuery().orderBy("id", "DESC");

        expect(() => query1.toSQL()).not.toThrow();
        expect(() => query2.toSQL()).not.toThrow();
      });
    });
  });

  describe("Array Length Limits", () => {
    describe("Window function arrays", () => {
      it("should reject too many partition columns (50+)", () => {
        const builder = new WindowFunctionBuilder(
          (col) => sanitizeSqlIdentifier(col),
          () => {}
        );

        const largePartition = Array.from({ length: 51 }, (_, i) => `col_${i}`);

        expect(() => {
          builder.addRowNumber(largePartition);
        }).toThrow(DatabaseError);

        expect(() => {
          builder.addRowNumber(largePartition);
        }).toThrow(/partition.*exceeds maximum.*50/i);
      });

      it("should reject too many order by columns in window functions (50+)", () => {
        const builder = new WindowFunctionBuilder(
          (col) => sanitizeSqlIdentifier(col),
          () => {}
        );

        const largeOrder = Array.from({ length: 51 }, (_, i) => [`col_${i}`, "ASC"] as [string, "ASC" | "DESC"]);

        expect(() => {
          builder.addRowNumber(undefined, largeOrder);
        }).toThrow(/order.*exceeds maximum.*50/i);
      });

      it("should accept partition/order at the limit (50)", () => {
        const builder = new WindowFunctionBuilder(
          (col) => sanitizeSqlIdentifier(col),
          () => {}
        );

        const partition = Array.from({ length: 50 }, (_, i) => `col_${i}`);
        const order = Array.from({ length: 50 }, (_, i) => [`col_${i}`, "ASC"] as [string, "ASC" | "DESC"]);

        expect(() => {
          builder.addRowNumber(partition, order);
        }).not.toThrow();
      });
    });

    describe("Set operations", () => {
      it("should reject too many set operations (100+)", () => {
        const builder = new SetOperationsBuilder();

        // Add 101 operations
        for (let i = 0; i < 101; i++) {
          builder.addOperation("UNION", "SELECT 1", []);
        }

        expect(() => {
          builder.buildQuery("SELECT 1", []);
        }).toThrow(DatabaseError);

        expect(() => {
          builder.buildQuery("SELECT 1", []);
        }).toThrow(/operations.*exceeds maximum.*100/i);
      });

      it("should accept set operations at the limit (100)", () => {
        const builder = new SetOperationsBuilder();

        for (let i = 0; i < 100; i++) {
          builder.addOperation("UNION", "SELECT 1", []);
        }

        expect(() => {
          builder.buildQuery("SELECT 1", []);
        }).not.toThrow();
      });
    });

    describe("CTEs", () => {
      it("should reject too many CTEs (50+)", () => {
        const builder = new CteBuilder(sanitizeSqlIdentifier);

        // Add 51 CTEs
        for (let i = 0; i < 51; i++) {
          builder.addCte(`cte_${i}`, "SELECT 1", []);
        }

        expect(() => {
          builder.buildWithClause();
        }).toThrow(DatabaseError);

        expect(() => {
          builder.buildWithClause();
        }).toThrow(/cte.*exceeds maximum.*50/i);
      });

      it("should accept CTEs at the limit (50)", () => {
        const builder = new CteBuilder(sanitizeSqlIdentifier);

        for (let i = 0; i < 50; i++) {
          builder.addCte(`cte_${i}`, "SELECT 1", []);
        }

        expect(() => {
          builder.buildWithClause();
        }).not.toThrow();
      });
    });
  });

  describe("Nested Structure Limits", () => {
    describe("Deeply nested JSON paths", () => {
      it("should reject excessively deep JSON paths (>20 levels)", () => {
        const deepPath = Array.from({ length: 21 }, (_, i) => `level_${i}`);
        const query = createQuery();

        expect(() => {
          query.jsonPath("data", deepPath);
        }).toThrow(DatabaseError);

        expect(() => {
          query.jsonPath("data", deepPath);
        }).toThrow(/path.*exceeds maximum depth.*20/i);
      });

      it("should accept JSON paths at the limit (20 levels)", () => {
        const deepPath = Array.from({ length: 20 }, (_, i) => `level_${i}`);
        const query = createQuery();

        expect(() => {
          query.jsonPath("data", deepPath);
        }).not.toThrow();
      });
    });

    describe("Chained WHERE clauses", () => {
      it("should reject too many WHERE conditions (500+)", () => {
        let query = createQuery();

        // Add 500 WHERE conditions (limit is >= 500, so 500th will trigger)
        expect(() => {
          for (let i = 0; i < 501; i++) {
            query = query.where("id", "=", i);
          }
        }).toThrow(DatabaseError);

        expect(() => {
          let query2 = createQuery();
          for (let i = 0; i < 501; i++) {
            query2 = query2.where("id", "=", i);
          }
        }).toThrow(/where.*exceeds maximum.*500/i);
      });

      it("should accept WHERE conditions just below the limit (499)", () => {
        let query = createQuery();

        for (let i = 0; i < 499; i++) {
          query = query.where("id", "=", i);
        }

        expect(() => query.toSQL()).not.toThrow();
      });
    });

    describe("ORDER BY clauses", () => {
      it("should reject too many ORDER BY clauses (100+)", () => {
        let query = createQuery();

        // Add 100 orderBy clauses (limit is >= 100, so 100th will trigger)
        expect(() => {
          for (let i = 0; i < 101; i++) {
            query = query.orderBy("id", "ASC");
          }
        }).toThrow(DatabaseError);

        expect(() => {
          let query2 = createQuery();
          for (let i = 0; i < 101; i++) {
            query2 = query2.orderBy("id", "ASC");
          }
        }).toThrow(/order by.*exceeds maximum.*100/i);
      });

      it("should accept ORDER BY just below the limit (99)", () => {
        let query = createQuery();

        for (let i = 0; i < 99; i++) {
          query = query.orderBy("id", "ASC");
        }

        expect(() => query.toSQL()).not.toThrow();
      });
    });
  });
});
