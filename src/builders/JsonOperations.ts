/**
 * JSON/JSONB operations for TypedQuery
 * Handles JSON field access, path operations, and JSONB functions
 */

import type { ColumnNames, JSONValue } from "../types";
import { DatabaseError } from "../errors";

export class JsonOperations<Row extends Record<string, any>> {
  constructor(
    private qualifyColumn: (column: string) => string,
    private escapeSingleQuotes: (value: string) => string
  ) {}

  /**
   * Validate JSON field/path component to prevent SQL injection
   */
  private validateJsonIdentifier(value: string, context: string = "JSON identifier"): string {
    const strValue = String(value).trim();

    if (strValue.length === 0) {
      throw new DatabaseError(
        `Invalid ${context}: cannot be empty`,
        'INVALID_JSON_IDENTIFIER',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    if (/[;"`\\]|--|\*\/|\/\*/.test(strValue)) {
      throw new DatabaseError(
        `Invalid ${context}: contains dangerous characters`,
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    if (/\b(UNION|SELECT|DROP|INSERT|UPDATE|DELETE|TRUNCATE|ALTER|EXEC|EXECUTE)\b/i.test(strValue)) {
      throw new DatabaseError(
        `Invalid ${context}: contains SQL keywords`,
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    if (/(^|\s)(OR|AND)(\s|$)/i.test(strValue) || /'.*?(OR|AND).*?'/i.test(strValue)) {
      throw new DatabaseError(
        `Invalid ${context}: contains SQL operators in suspicious context`,
        'SQL_INJECTION_ATTEMPT',
        { query: '', params: [], detail: `value: ${strValue}` }
      );
    }

    if (strValue.length > 255) {
      throw new DatabaseError(
        `Invalid ${context}: exceeds maximum length of 255 characters`,
        'INVALID_JSON_IDENTIFIER',
        { query: '', params: [], detail: `value: ${strValue.substring(0, 50)}...` }
      );
    }

    return strValue;
  }

  /**
   * Get JSON object field (using -> operator)
   * Returns JSON
   */
  jsonField<K extends ColumnNames<Row>, T = Row[K]>(
    column: K,
    field: T extends Record<string, any> ? keyof T : string
  ): string;
  jsonField<T extends Record<string, any>>(
    column: string,
    field: keyof T
  ): string;
  jsonField(column: string, field: string): string {
    const validatedField = this.validateJsonIdentifier(field, "JSON field name");
    return `${this.qualifyColumn(column)}->'${this.escapeSingleQuotes(validatedField)}'`;
  }

  /**
   * Get JSON object field as text (using ->> operator)
   * Returns TEXT
   */
  jsonFieldAsText<K extends ColumnNames<Row>, T = Row[K]>(
    column: K,
    field: T extends Record<string, any> ? keyof T : string
  ): string;
  jsonFieldAsText<T extends Record<string, any>>(
    column: string,
    field: keyof T
  ): string;
  jsonFieldAsText(column: string, field: string): string {
    const validatedField = this.validateJsonIdentifier(field, "JSON field name");
    return `${this.qualifyColumn(column)}->>'${this.escapeSingleQuotes(validatedField)}'`;
  }

  /**
   * Get JSON value by path (using #> operator)
   * Returns JSON
   */
  jsonPath(column: string, path: string[]): string {
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const arrayStr = validatedPath
      .map((p) => `'${this.escapeSingleQuotes(p)}'`)
      .join(",");
    return `${this.qualifyColumn(column)}#>ARRAY[${arrayStr}]`;
  }

  /**
   * Get JSON value by path as text (using #>> operator)
   * Returns TEXT
   */
  jsonPathAsText(column: string, path: string[]): string {
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const arrayStr = validatedPath
      .map((p) => `'${this.escapeSingleQuotes(p)}'`)
      .join(",");
    return `${this.qualifyColumn(column)}#>>ARRAY[${arrayStr}]`;
  }

  /**
   * Create a JSONB set expression for updating a value at a path
   * Usage: jsonbSet("data", ["address", "city"], "New York")
   * Returns: jsonb_set(data, '{address,city}', '"New York"'::jsonb)
   */
  jsonbSet(
    column: string,
    path: string[],
    value: JSONValue,
    createMissing: boolean = true
  ): string {
    const qualifiedColumn = this.qualifyColumn(column);
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
    const pathStr = `{${escapedPath.join(",")}}`;
    const jsonValueStr = this.escapeSingleQuotes(JSON.stringify(value));
    return `jsonb_set(${qualifiedColumn}, '${pathStr}', '${jsonValueStr}'::jsonb, ${createMissing})`;
  }

  /**
   * Create a JSONB insert expression for inserting a value at a path
   */
  jsonbInsert(
    column: string,
    path: string[],
    value: JSONValue,
    insertAfter: boolean = false
  ): string {
    const qualifiedColumn = this.qualifyColumn(column);
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
    const pathStr = `{${escapedPath.join(",")}}`;
    const jsonValueStr = this.escapeSingleQuotes(JSON.stringify(value));
    return `jsonb_insert(${qualifiedColumn}, '${pathStr}', '${jsonValueStr}'::jsonb, ${insertAfter})`;
  }

  /**
   * Concatenate JSONB values
   */
  jsonbConcat(column: string, value: Record<string, any>): string {
    const qualifiedColumn = this.qualifyColumn(column);
    const jsonValueStr = this.escapeSingleQuotes(JSON.stringify(value));
    return `${qualifiedColumn} || '${jsonValueStr}'::jsonb`;
  }

  /**
   * Delete a key from JSONB object
   */
  jsonbDeleteKey(column: string, key: string): string {
    const qualifiedColumn = this.qualifyColumn(column);
    const validatedKey = this.validateJsonIdentifier(key, "JSON key");
    const escapedKey = this.escapeSingleQuotes(validatedKey);
    return `${qualifiedColumn} - '${escapedKey}'`;
  }

  /**
   * Delete a path from JSONB object
   */
  jsonbDeletePath(column: string, path: string[]): string {
    const qualifiedColumn = this.qualifyColumn(column);
    const validatedPath = path.map((p) => this.validateJsonIdentifier(p, "JSON path component"));
    const escapedPath = validatedPath.map((p) => this.escapeSingleQuotes(p));
    const pathStr = `{${escapedPath.join(",")}}`;
    return `${qualifiedColumn} #- '${pathStr}'`;
  }

  /**
   * Build a JSONB object from key-value pairs
   */
  jsonbBuildObject(...pairs: [string, any][]): string {
    const args = pairs
      .flatMap(([key, value]) => {
        const validatedKey = this.validateJsonIdentifier(key, "JSON object key");
        return [
          `'${this.escapeSingleQuotes(validatedKey)}'`,
          typeof value === "string"
            ? `'${this.escapeSingleQuotes(value)}'`
            : String(value),
        ];
      })
      .join(", ");
    return `jsonb_build_object(${args})`;
  }

  /**
   * Build a JSONB array from values
   */
  jsonbBuildArray(...values: any[]): string {
    const args = values
      .map((v) =>
        typeof v === "string"
          ? `'${this.escapeSingleQuotes(v)}'`
          : String(v)
      )
      .join(", ");
    return `jsonb_build_array(${args})`;
  }

  /**
   * Get all keys from a JSONB object at top level
   */
  jsonbObjectKeys(column: string): string {
    return `jsonb_object_keys(${this.qualifyColumn(column)})`;
  }

  /**
   * Get the type of a JSONB value
   */
  jsonbTypeof(column: string): string {
    return `jsonb_typeof(${this.qualifyColumn(column)})`;
  }

  /**
   * Get the length of a JSONB array
   */
  jsonbArrayLength(column: string): string {
    return `jsonb_array_length(${this.qualifyColumn(column)})`;
  }
}
