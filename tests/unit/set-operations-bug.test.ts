import { SetOperationsBuilder } from "../../src/builders";

describe("SetOperations Parameter Renumbering Bug", () => {
  describe("Double-replacement edge case", () => {
    it("should handle renumbering when base has 10+ params", () => {
      const builder = new SetOperationsBuilder();

      // Add operation with $1, $2 that needs to become $11, $12
      builder.addOperation(
        "UNION",
        "SELECT id FROM admins WHERE role = $1 AND status = $2",
        ["admin", "active"]
      );

      // Base query has 10 parameters
      const result = builder.buildQuery(
        "SELECT id FROM users WHERE p1 = $1 AND p2 = $2 AND p3 = $3 AND p4 = $4 AND p5 = $5 AND p6 = $6 AND p7 = $7 AND p8 = $8 AND p9 = $9 AND p10 = $10",
        ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"]
      );

      // The operation's $1 should become $11 (not get confused with existing $1)
      expect(result.query).toContain("role = $11");
      expect(result.query).toContain("status = $12");

      // Original $1 should stay $1
      expect(result.query).toContain("p1 = $1");

      // Params should be in correct order
      expect(result.params).toEqual([
        "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
        "admin", "active"
      ]);
    });

    it("should handle renumbering when result would have 100+ params", () => {
      const builder = new SetOperationsBuilder();

      // Create base query with 99 parameters
      const baseParams = Array.from({ length: 99 }, (_, i) => `v${i + 1}`);
      const basePlaceholders = baseParams.map((_, i) => `p${i + 1} = $${i + 1}`).join(" AND ");

      // Add operation with $1 that should become $100
      builder.addOperation(
        "UNION",
        "SELECT id FROM admins WHERE role = $1",
        ["admin"]
      );

      const result = builder.buildQuery(
        `SELECT id FROM users WHERE ${basePlaceholders}`,
        baseParams
      );

      // $1 should become $100 (not get confused)
      expect(result.query).toContain("role = $100");

      // Original $1 should stay $1
      expect(result.query).toContain("p1 = $1");

      expect(result.params.length).toBe(100);
      expect(result.params[99]).toBe("admin");
    });

    it("should handle multiple operations with overlapping parameter numbers", () => {
      const builder = new SetOperationsBuilder();

      // First operation: $1, $2 become $11, $12
      builder.addOperation(
        "UNION",
        "SELECT id FROM admins WHERE role = $1 AND dept = $2",
        ["admin", "IT"]
      );

      // Second operation: $1, $2 become $13, $14
      builder.addOperation(
        "UNION",
        "SELECT id FROM managers WHERE role = $1 AND dept = $2",
        ["manager", "Sales"]
      );

      const result = builder.buildQuery(
        "SELECT id FROM users WHERE a = $1 AND b = $2 AND c = $3 AND d = $4 AND e = $5 AND f = $6 AND g = $7 AND h = $8 AND i = $9 AND j = $10",
        ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"]
      );

      // First operation
      expect(result.query).toContain("admins WHERE role = $11 AND dept = $12");

      // Second operation
      expect(result.query).toContain("managers WHERE role = $13 AND dept = $14");

      // Original params should be unchanged
      expect(result.query).toContain("a = $1");
      expect(result.query).toContain("j = $10");

      expect(result.params).toEqual([
        "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
        "admin", "IT",
        "manager", "Sales"
      ]);
    });

    it("should handle operation with many parameters", () => {
      const builder = new SetOperationsBuilder();

      // Operation with 15 parameters
      const opParams = Array.from({ length: 15 }, (_, i) => `op${i + 1}`);
      const opPlaceholders = opParams.map((_, i) => `col${i + 1} = $${i + 1}`).join(" AND ");

      builder.addOperation(
        "UNION",
        `SELECT id FROM admins WHERE ${opPlaceholders}`,
        opParams
      );

      const result = builder.buildQuery(
        "SELECT id FROM users WHERE status = $1",
        ["active"]
      );

      // Operation's $1 should become $2, $15 should become $16
      expect(result.query).toContain("col1 = $2");
      expect(result.query).toContain("col15 = $16");

      // Original $1 should stay $1
      expect(result.query).toContain("status = $1");

      expect(result.params.length).toBe(16);
    });
  });

  describe("Edge cases for parameter renumbering", () => {
    it("should not match $1 inside $10", () => {
      const builder = new SetOperationsBuilder();

      // Operation with 10 parameters where both $1 and $10 appear
      const opParams = Array.from({ length: 10 }, (_, i) => `val${i + 1}`);

      builder.addOperation(
        "UNION",
        "SELECT id FROM admins WHERE a = $1 AND b = $10",
        opParams
      );

      const result = builder.buildQuery(
        "SELECT id FROM users WHERE x = $1",
        ["x1"]
      );

      // Original operation had $1 and $10
      // After renumbering: $1 -> $2, $10 -> $11
      expect(result.query).toContain("a = $2");
      expect(result.query).toContain("b = $11");

      // Should not have corrupted $10 when replacing $1
      expect(result.query).not.toContain("$101");
      expect(result.query).not.toContain("$20");
      expect(result.query).not.toContain("$110");
    });

    it("should handle $1 appearing multiple times", () => {
      const builder = new SetOperationsBuilder();

      builder.addOperation(
        "UNION",
        "SELECT id FROM admins WHERE role = $1 OR backup_role = $1",
        ["admin"]
      );

      const result = builder.buildQuery(
        "SELECT id FROM users WHERE x = $1 AND y = $2 AND z = $3",
        ["v1", "v2", "v3"]
      );

      // Both instances of $1 should become $4
      const matches = result.query.match(/\$4/g);
      expect(matches?.length).toBe(2);
      expect(result.query).toContain("role = $4 OR backup_role = $4");
    });
  });
});
