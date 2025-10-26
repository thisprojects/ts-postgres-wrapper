/**
 * Mock pg Pool for testing database queries without a real database connection
 */
export class MockPool {
  private mockResults: any[] = [];
  public queryLog: Array<{ text: string; values: any[] }> = [];

  constructor() {}

  /**
   * Set the mock results that will be returned by query() calls
   */
  setMockResults(results: any[]) {
    this.mockResults = results;
  }

  /**
   * Get the current mock results
   */
  getMockResults() {
    return this.mockResults;
  }

  /**
   * Get all executed queries for assertion purposes
   */
  getQueryLog() {
    return this.queryLog;
  }

  /**
   * Clear the query log (useful in beforeEach hooks)
   */
  clearQueryLog() {
    this.queryLog = [];
  }

  /**
   * Reset both query log and mock results
   */
  reset() {
    this.queryLog = [];
    this.mockResults = [];
  }

  /**
   * Mock implementation of pg Pool.query()
   */
  async query<T>(text: string, values: any[] = []): Promise<{ rows: T[] }> {
    this.queryLog.push({ text, values });
    return { rows: this.mockResults as T[] };
  }

  /**
   * Mock implementation of pg Pool.connect()
   */
  async connect() {
    return {
      query: this.query.bind(this),
      release: () => {},
    };
  }

  /**
   * Mock implementation of pg Pool.end()
   */
  async end() {}

  /**
   * Helper method to get the last executed query
   */
  getLastQuery() {
    return this.queryLog[this.queryLog.length - 1];
  }

  /**
   * Helper method to check if a specific query was executed
   */
  hasExecutedQuery(sql: string): boolean {
    return this.queryLog.some((query) => query.text === sql);
  }

  /**
   * Helper method to get all queries matching a pattern
   */
  getQueriesMatching(
    pattern: string | RegExp
  ): Array<{ text: string; values: any[] }> {
    if (typeof pattern === "string") {
      return this.queryLog.filter((query) => query.text.includes(pattern));
    }
    return this.queryLog.filter((query) => pattern.test(query.text));
  }
}
