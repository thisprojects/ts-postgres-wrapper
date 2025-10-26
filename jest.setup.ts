// Global test setup for Jest
import { MockPool } from "./tests/test_utils/MockPool";

// Extend Jest matchers with custom assertions for database testing
declare global {
  namespace jest {
    interface Matchers<R> {
      toHaveExecutedQuery(expectedSql: string): R;
      toHaveExecutedQueryWithParams(
        expectedSql: string,
        expectedParams: any[]
      ): R;
      toHaveExecutedQueries(expectedCount: number): R;
    }
  }
}

// Custom matcher to check if a query was executed
expect.extend({
  toHaveExecutedQuery(mockPool: MockPool, expectedSql: string) {
    const queries = mockPool.getQueryLog();
    const found = queries.some((query) => query.text === expectedSql);

    if (found) {
      return {
        message: () =>
          `Expected query "${expectedSql}" not to have been executed`,
        pass: true,
      };
    } else {
      return {
        message: () =>
          `Expected query "${expectedSql}" to have been executed.\nActual queries:\n${queries
            .map((q) => `- ${q.text}`)
            .join("\n")}`,
        pass: false,
      };
    }
  },

  toHaveExecutedQueryWithParams(
    mockPool: MockPool,
    expectedSql: string,
    expectedParams: any[]
  ) {
    const queries = mockPool.getQueryLog();
    const found = queries.some(
      (query) =>
        query.text === expectedSql &&
        JSON.stringify(query.values) === JSON.stringify(expectedParams)
    );

    if (found) {
      return {
        message: () =>
          `Expected query "${expectedSql}" with params ${JSON.stringify(
            expectedParams
          )} not to have been executed`,
        pass: true,
      };
    } else {
      return {
        message: () =>
          `Expected query "${expectedSql}" with params ${JSON.stringify(
            expectedParams
          )} to have been executed.\nActual queries:\n${queries
            .map((q) => `- ${q.text} with params ${JSON.stringify(q.values)}`)
            .join("\n")}`,
        pass: false,
      };
    }
  },

  toHaveExecutedQueries(mockPool: MockPool, expectedCount: number) {
    const actualCount = mockPool.getQueryLog().length;

    if (actualCount === expectedCount) {
      return {
        message: () =>
          `Expected not to have executed exactly ${expectedCount} queries`,
        pass: true,
      };
    } else {
      return {
        message: () =>
          `Expected to have executed ${expectedCount} queries, but executed ${actualCount}`,
        pass: false,
      };
    }
  },
});

// Global test timeout
jest.setTimeout(30000);
