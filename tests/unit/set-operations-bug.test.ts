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

      // Operation with parameters at $1 and $10
      // op.params has 10 values, but query only uses 2 unique param numbers
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

      // The query has 2 unique parameters: $1, $10
      // They get renumbered to $2, $3 (sequential from base+1)
      expect(result.query).toContain("a = $2");
      expect(result.query).toContain("b = $3");

      // Should not have corrupted $10 when replacing $1
      expect(result.query).not.toContain("$10");
      expect(result.query).not.toContain("$101");
      expect(result.query).not.toContain("$20");
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

    it("should handle collision scenario where new param matches existing pattern", () => {
      const builder = new SetOperationsBuilder();

      // This is the key edge case: if we naively replace $1 -> $10
      // and the query already has $10, we could get double-replacement
      builder.addOperation(
        "UNION",
        "SELECT id FROM data WHERE a = $1 AND b = $10",
        ["val1", "val10"]
      );

      // Base has 9 params
      const result = builder.buildQuery(
        "SELECT id FROM users WHERE p1=$1 AND p2=$2 AND p3=$3 AND p4=$4 AND p5=$5 AND p6=$6 AND p7=$7 AND p8=$8 AND p9=$9",
        ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9"]
      );

      // With placeholder approach:
      // Query has unique params $1, $10 which get mapped to $10, $11 (sequential)
      // $1 -> __PARAM_10__, $10 -> __PARAM_11__
      // Then both get converted to final form
      expect(result.query).toContain("a = $10");
      expect(result.query).toContain("b = $11");

      // Should not have any corrupted parameter numbers
      expect(result.query).not.toContain("$100");
      expect(result.query).not.toContain("$109");
      expect(result.query).not.toContain("$110");

      expect(result.params).toEqual([
        "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9",
        "val1", "val10"
      ]);
    });

    it("should handle complex collision with $1, $11, $111", () => {
      const builder = new SetOperationsBuilder();

      // Create query with $1, $11, $111 to test word boundary handling
      builder.addOperation(
        "UNION",
        "SELECT id FROM data WHERE a = $1 AND b = $11 AND c = $111",
        ["v1", "v11", "v111"]
      );

      // Base has 5 params
      const result = builder.buildQuery(
        "SELECT id FROM users WHERE x1=$1 AND x2=$2 AND x3=$3 AND x4=$4 AND x5=$5",
        ["base1", "base2", "base3", "base4", "base5"]
      );

      // Query has unique params $1, $11, $111 which get sequentially renumbered
      // $1 -> $6, $11 -> $7, $111 -> $8
      expect(result.query).toContain("a = $6");
      expect(result.query).toContain("b = $7");
      expect(result.query).toContain("c = $8");

      // Verify no partial replacements occurred
      expect(result.query).not.toContain("$61");
      expect(result.query).not.toContain("$11");
      expect(result.query).not.toContain("$111");

      expect(result.params).toEqual([
        "base1", "base2", "base3", "base4", "base5",
        "v1", "v11", "v111"
      ]);
    });

    it("should handle renumbering when target range overlaps source range", () => {
      const builder = new SetOperationsBuilder();

      // Operation has $5, $6, $7 which need to become $1, $2, $3
      // This tests backward renumbering
      builder.addOperation(
        "UNION",
        "SELECT id FROM admins WHERE a = $5 AND b = $6 AND c = $7",
        ["val5", "val6", "val7"]
      );

      const result = builder.buildQuery(
        "SELECT id FROM users",
        []
      );

      // $5 -> $1, $6 -> $2, $7 -> $3
      expect(result.query).toContain("a = $1");
      expect(result.query).toContain("b = $2");
      expect(result.query).toContain("c = $3");

      // Should not have leftover $5, $6, $7
      expect(result.query).not.toContain("$5");
      expect(result.query).not.toContain("$6");
      expect(result.query).not.toContain("$7");

      expect(result.params).toEqual(["val5", "val6", "val7"]);
    });

    it("should handle sparse parameter numbers", () => {
      const builder = new SetOperationsBuilder();

      // Operation uses $1, $5, $10 (sparse - not all numbers in between)
      builder.addOperation(
        "UNION",
        "SELECT id FROM data WHERE a = $1 AND b = $5 AND c = $10",
        ["v1", "v5", "v10"]
      );

      const result = builder.buildQuery(
        "SELECT id FROM users WHERE x = $1",
        ["base"]
      );

      // All three parameters should be renumbered correctly
      expect(result.query).toContain("a = $2");
      expect(result.query).toContain("b = $3");
      expect(result.query).toContain("c = $4");

      expect(result.params).toEqual(["base", "v1", "v5", "v10"]);
    });
  });
});
