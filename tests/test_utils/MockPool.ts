/**
 * Mock pg Pool for testing database queries without a real database connection
 */
export class MockPool {
  private mockResults: any[] = [];
  private mockResultsQueue: any[][] = [];
  private queryIndex: number = 0;
  public queryLog: Array<{ text: string; values: any[] }> = [];
  private eventListeners: Map<string, Function[]> = new Map();

  constructor() {}

  /**
   * Mock implementation of EventEmitter.on()
   */
  on(event: string, listener: Function): this {
    if (!this.eventListeners.has(event)) {
      this.eventListeners.set(event, []);
    }
    this.eventListeners.get(event)!.push(listener);
    return this;
  }

  /**
   * Mock implementation of EventEmitter.emit()
   */
  emit(event: string, ...args: any[]): boolean {
    const listeners = this.eventListeners.get(event);
    if (!listeners || listeners.length === 0) {
      return false;
    }
    listeners.forEach(listener => listener(...args));
    return true;
  }

  /**
   * Mock implementation of EventEmitter.removeListener()
   */
  removeListener(event: string, listener: Function): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      const index = listeners.indexOf(listener);
      if (index > -1) {
        listeners.splice(index, 1);
      }
    }
    return this;
  }

  /**
   * Set the mock results that will be returned by query() calls
   */
  setMockResults(results: any[] | any[][]) {
    // Check if this is a queue of results (array of arrays) or single result set
    if (results.length > 0 && Array.isArray(results[0]) && results.every((r: any) => Array.isArray(r))) {
      this.mockResultsQueue = results as any[][];
      this.mockResults = [];
      this.queryIndex = 0;
    } else {
      this.mockResults = results;
      this.mockResultsQueue = [];
      this.queryIndex = 0;
    }
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
    this.queryIndex = 0;
  }

  /**
   * Reset both query log and mock results
   */
  reset() {
    this.queryLog = [];
    this.mockResults = [];
    this.mockResultsQueue = [];
    this.queryIndex = 0;
  }

  /**
   * Mock implementation of pg Pool.query()
   */
  async query<T>(text: string, values: any[] = []): Promise<{ rows: T[] }> {
    // Normalize Date objects to strings
    const normalizedValues = values.map(value =>
      value instanceof Date ? value.toISOString() : value
    );
    this.queryLog.push({ text, values: normalizedValues });

    // If we have a queue of results, return them sequentially
    if (this.mockResultsQueue.length > 0) {
      const result = this.mockResultsQueue[this.queryIndex] || [];
      this.queryIndex++;
      return { rows: result as T[] };
    }

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
