/**
 * Query logging functionality
 */

import { QueryLogger, LogLevel, QueryLogEntry } from './types';

/**
 * Default console logger implementation
 */
export class ConsoleLogger implements QueryLogger {
  constructor(private minLevel: LogLevel = "info") {}

  log(level: LogLevel, entry: QueryLogEntry): void {
    const levels: LogLevel[] = ["debug", "info", "warn", "error"];
    if (levels.indexOf(level) < levels.indexOf(this.minLevel)) {
      return;
    }

    const timestamp = entry.timestamp.toISOString();
    const duration = entry.duration ? `${entry.duration}ms` : "N/A";
    const message = `[${timestamp}] [${level.toUpperCase()}] Query: ${entry.query} | Params: ${JSON.stringify(entry.params)} | Duration: ${duration}`;

    if (entry.error) {
      console.error(message, entry.error);
    } else if (level === "error") {
      console.error(message);
    } else if (level === "warn") {
      console.warn(message);
    } else {
      console.log(message);
    }
  }
}
