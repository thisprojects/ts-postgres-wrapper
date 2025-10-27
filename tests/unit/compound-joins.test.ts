import { TypedQuery } from "../../src";
import { MockPool } from "../test_utils/MockPool";

describe("Compound Key Joins", () => {
  let pool: MockPool;
  let query: TypedQuery;

  beforeEach(() => {
    pool = new MockPool();
    query = new TypedQuery(pool as any, "shipments");
  });

  describe("INNER JOIN with compound keys", () => {
    it("should create an INNER JOIN with multiple conditions", async () => {
      await query
        .innerJoin("tracking", [
          { leftColumn: "shipments.carrier_id", rightColumn: "tracking.carrier_id" },
          { leftColumn: "shipments.tracking_number", rightColumn: "tracking.tracking_number" }
        ])
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments INNER JOIN tracking ON shipments.carrier_id = tracking.carrier_id AND shipments.tracking_number = tracking.tracking_number`
      );
    });

    it("should support table alias with compound keys", async () => {
      await query
        .innerJoin("tracking", [
          { leftColumn: "shipments.carrier_id", rightColumn: "t.carrier_id" },
          { leftColumn: "shipments.tracking_number", rightColumn: "t.tracking_number" }
        ], "t")
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments INNER JOIN tracking AS t ON shipments.carrier_id = t.carrier_id AND shipments.tracking_number = t.tracking_number`
      );
    });

    it("should maintain backward compatibility for single-column joins", async () => {
      await query
        .innerJoin("orders", "shipments.order_id", "orders.id")
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments INNER JOIN orders ON shipments.order_id = orders.id`
      );
    });
  });

  describe("LEFT JOIN with compound keys", () => {
    it("should create a LEFT JOIN with multiple conditions", async () => {
      await query
        .leftJoin("inventory", [
          { leftColumn: "shipments.warehouse_id", rightColumn: "inventory.warehouse_id" },
          { leftColumn: "shipments.product_id", rightColumn: "inventory.product_id" }
        ])
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments LEFT JOIN inventory ON shipments.warehouse_id = inventory.warehouse_id AND shipments.product_id = inventory.product_id`
      );
    });
  });

  describe("RIGHT JOIN with compound keys", () => {
    it("should create a RIGHT JOIN with multiple conditions", async () => {
      await query
        .rightJoin("orders", [
          { leftColumn: "shipments.order_id", rightColumn: "orders.id" },
          { leftColumn: "shipments.customer_id", rightColumn: "orders.customer_id" }
        ])
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments RIGHT JOIN orders ON shipments.order_id = orders.id AND shipments.customer_id = orders.customer_id`
      );
    });
  });

  describe("FULL JOIN with compound keys", () => {
    it("should create a FULL JOIN with multiple conditions", async () => {
      await query
        .fullJoin("inventory", [
          { leftColumn: "shipments.warehouse_id", rightColumn: "inventory.warehouse_id" },
          { leftColumn: "shipments.product_id", rightColumn: "inventory.product_id" }
        ])
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments FULL JOIN inventory ON shipments.warehouse_id = inventory.warehouse_id AND shipments.product_id = inventory.product_id`
      );
    });
  });

  describe("Complex scenarios", () => {
    it("should support multiple compound key joins", async () => {
      await query
        .innerJoin("tracking", [
          { leftColumn: "shipments.carrier_id", rightColumn: "t.carrier_id" },
          { leftColumn: "shipments.tracking_number", rightColumn: "t.tracking_number" }
        ], "t")
        .leftJoin("inventory", [
          { leftColumn: "shipments.warehouse_id", rightColumn: "i.warehouse_id" },
          { leftColumn: "shipments.product_id", rightColumn: "i.product_id" }
        ], "i")
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments ` +
        `INNER JOIN tracking AS t ON shipments.carrier_id = t.carrier_id AND shipments.tracking_number = t.tracking_number ` +
        `LEFT JOIN inventory AS i ON shipments.warehouse_id = i.warehouse_id AND shipments.product_id = i.product_id`
      );
    });

    it("should support mixing single-column and compound key joins", async () => {
      await query
        .innerJoin("orders", "shipments.order_id", "orders.id")
        .leftJoin("inventory", [
          { leftColumn: "shipments.warehouse_id", rightColumn: "i.warehouse_id" },
          { leftColumn: "shipments.product_id", rightColumn: "i.product_id" }
        ], "i")
        .execute();

      expect(pool).toHaveExecutedQuery(
        `SELECT * FROM shipments ` +
        `INNER JOIN orders ON shipments.order_id = orders.id ` +
        `LEFT JOIN inventory AS i ON shipments.warehouse_id = i.warehouse_id AND shipments.product_id = i.product_id`
      );
    });

    it("should support compound key joins with WHERE conditions", async () => {
      await query
        .innerJoin("tracking", [
          { leftColumn: "shipments.carrier_id", rightColumn: "t.carrier_id" },
          { leftColumn: "shipments.tracking_number", rightColumn: "t.tracking_number" }
        ], "t")
        .where("t.status", "=", "delivered")
        .execute();

      expect(pool).toHaveExecutedQueryWithParams(
        `SELECT * FROM shipments ` +
        `INNER JOIN tracking AS t ON shipments.carrier_id = t.carrier_id AND shipments.tracking_number = t.tracking_number ` +
        `WHERE t.status = $1`,
        ["delivered"]
      );
    });
  });
});