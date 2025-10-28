// Re-export main classes
export { TypedQuery } from "./TypedQuery";
export { TypedPg, createTypedPg } from "./TypedPg";

// Re-export Pool type for convenience
export { Pool } from "pg";

// Re-export types from types module
export type {
  QueryLogEntry,
  LogLevel,
  QueryLogger,
  SecurityOptions,
  TypedPgOptions,
  ErrorContext,
  JSONOperator,
  JSONPath,
  JSONValue,
  JSONPathType,
  JSONField,
  ColumnNames,
  ColumnAlias,
  ExpressionAlias,
  ColumnSpec,
  IsTypedAlias,
  ResultColumns,
  SelectColumns,
  JoinCondition,
  JoinConfig,
} from "./types";

// Re-export helper functions from types
export { col, expr } from "./types";

// Re-export utilities
export {
  stripSqlComments,
  sanitizeSqlIdentifier,
  validateQueryComplexity,
} from "./utils";

// Re-export error handling
export { DatabaseError, isTransientError } from "./errors";

// Re-export logger
export { ConsoleLogger } from "./logger";
