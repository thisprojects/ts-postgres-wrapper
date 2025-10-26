// Type declarations for custom Jest matchers
import { MockPool } from "../tests/test_utils/MockPool";

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

// Augment expect interface
declare module "expect" {
  interface Matchers<R> {
    toHaveExecutedQuery(expectedSql: string): R;
    toHaveExecutedQueryWithParams(
      expectedSql: string,
      expectedParams: any[]
    ): R;
    toHaveExecutedQueries(expectedCount: number): R;
  }
}

export {};
