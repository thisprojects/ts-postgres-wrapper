/**
 * Utility functions for SQL query processing and validation
 */

import { SecurityOptions } from './types';
import { DatabaseError } from './errors';

/**
 * Strip SQL comments from a query string
 * Handles both line comments and block comments
 * Preserves comments inside strings and PostgreSQL dollar-quoted strings
 */
export function stripSqlComments(sql: string): string {
  let result = '';
  let inString = false;
  let stringChar = '';
  let inBlockComment = false;
  let inDollarQuote = false;
  let dollarQuoteTag = '';

  for (let i = 0; i < sql.length; i++) {
    const char = sql[i];
    const next = sql[i + 1];

    // Handle dollar-quoted strings $tag$...$tag$
    // Dollar quotes take precedence over everything else when active
    if (inDollarQuote) {
      result += char;

      // Check if we're at the closing dollar quote
      if (char === '$') {
        // Try to match the closing tag
        let potentialTag = '$';
        let j = i + 1;

        // Extract potential closing tag
        while (j < sql.length && sql[j] !== '$') {
          potentialTag += sql[j];
          j++;
        }

        if (j < sql.length && sql[j] === '$') {
          potentialTag += '$';

          // Check if this matches our opening tag
          if (potentialTag === dollarQuoteTag) {
            // Found closing tag - add remaining characters and exit dollar quote
            for (let k = i + 1; k < j + 1; k++) {
              result += sql[k];
            }
            i = j; // Move past the closing tag
            inDollarQuote = false;
            dollarQuoteTag = '';
          }
        }
      }
      continue;
    }

    // Check for dollar quote start (only outside strings and comments)
    if (!inString && !inBlockComment && char === '$') {
      // Try to extract dollar quote tag
      let tag = '$';
      let j = i + 1;

      // Tag can contain letters, digits, and underscores (or be empty)
      while (j < sql.length && /[a-zA-Z0-9_]/.test(sql[j])) {
        tag += sql[j];
        j++;
      }

      // Must end with another $
      if (j < sql.length && sql[j] === '$') {
        tag += '$';

        // This is a valid dollar quote
        inDollarQuote = true;
        dollarQuoteTag = tag;

        // Add the opening tag to result
        for (let k = i; k <= j; k++) {
          result += sql[k];
        }
        i = j; // Move past the opening tag
        continue;
      }
    }

    // Handle block comments /* ... */
    if (!inString && !inBlockComment && char === '/' && next === '*') {
      inBlockComment = true;
      i++; // Skip the *
      continue;
    }

    if (inBlockComment) {
      if (char === '*' && next === '/') {
        inBlockComment = false;
        i++; // Skip the /
      }
      continue; // Skip all characters inside block comment
    }

    // Handle string literals (single and double quotes)
    if (char === "'" || char === '"') {
      if (!inString) {
        // Starting a string literal
        inString = true;
        stringChar = char;
        result += char;
      } else if (char === stringChar) {
        // Could be end of string or escaped quote
        if (next === stringChar) {
          // PostgreSQL escaped quote: '' or ""
          result += char;
          result += next;
          i++; // Skip the next quote
        } else {
          // End of string literal
          inString = false;
          stringChar = '';
          result += char;
        }
      } else {
        // Different quote character inside string
        result += char;
      }
      continue;
    }

    // Handle line comments -- ...
    if (!inString && char === '-' && next === '-') {
      // Skip until end of line
      while (i < sql.length && sql[i] !== '\n') {
        i++;
      }
      // Include the newline if present
      if (i < sql.length && sql[i] === '\n') {
        result += '\n';
      }
      continue;
    }

    // Regular character
    if (!inBlockComment) {
      result += char;
    }
  }

  return result.trim();
}

/**
 * Validate query complexity to prevent DoS attacks
 */
export function validateQueryComplexity(
  query: string,
  security: SecurityOptions
): void {
  // Check query length
  const maxLength = (security as any).maxQueryLength || 50000;
  if (query.length > maxLength) {
    throw new DatabaseError(
      `Query exceeds maximum length of ${maxLength} characters`,
      'QUERY_TOO_LONG',
      { query: query.substring(0, 200) + '...', params: [] }
    );
  }

  // Strip comments if not allowed
  let processedQuery = query;
  if (!(security as any).allowComments) {
    const stripped = stripSqlComments(query);
    if (stripped !== query) {
      throw new DatabaseError(
        'SQL comments are not allowed',
        'COMMENTS_NOT_ALLOWED',
        { query: query.substring(0, 200), params: [] }
      );
    }
    processedQuery = stripped;
  }

  // Count JOINs
  const maxJoins = security.maxJoins || 10;
  const joinMatches = processedQuery.match(/\b(INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN\b/gi);
  const joinCount = joinMatches ? joinMatches.length : 0;
  if (joinCount > maxJoins) {
    throw new DatabaseError(
      `Query has ${joinCount} JOINs, maximum allowed is ${maxJoins}`,
      'TOO_MANY_JOINS',
      { query: processedQuery.substring(0, 200), params: [] }
    );
  }

  // Count WHERE conditions (approximate by counting AND/OR)
  const maxConditions = security.maxWhereConditions || 50;
  const whereMatch = processedQuery.match(/\bWHERE\b/i);
  if (whereMatch) {
    const whereSection = processedQuery.substring(whereMatch.index!);
    const andOrMatches = whereSection.match(/\b(AND|OR)\b/gi);
    const conditionCount = (andOrMatches ? andOrMatches.length : 0) + 1;
    if (conditionCount > maxConditions) {
      throw new DatabaseError(
        `Query has ${conditionCount} WHERE conditions, maximum allowed is ${maxConditions}`,
        'TOO_MANY_CONDITIONS',
        { query: processedQuery.substring(0, 200), params: [] }
      );
    }
  }
}

/**
 * PostgreSQL reserved keywords that must be quoted when used as identifiers
 * Source: PostgreSQL 16 official documentation
 * This list includes:
 * 1. Strictly reserved keywords (cannot be used as identifiers without quoting)
 * 2. Commonly problematic keywords that often cause issues
 *
 * Note: This is a conservative list focused on keywords that MUST be quoted.
 * Many PostgreSQL keywords are non-reserved and can be used unquoted, but
 * we include the most commonly used SQL keywords for safety.
 */
const RESERVED_KEYWORDS = new Set([
  // Strictly reserved keywords in PostgreSQL (cannot be used as identifiers without quoting)
  // Based on PostgreSQL official documentation
  'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric',
  'both', 'case', 'cast', 'check', 'collate', 'constraint', 'create',
  'current_catalog', 'current_date', 'current_role', 'current_time',
  'current_timestamp', 'current_user', 'default', 'deferrable', 'desc',
  'distinct', 'do', 'else', 'end', 'except', 'false', 'fetch', 'for',
  'foreign', 'from', 'grant', 'group', 'having', 'in', 'initially',
  'intersect', 'into', 'lateral', 'leading', 'limit', 'localtime',
  'localtimestamp', 'not', 'null', 'offset', 'on', 'only', 'or',
  'order', 'placing', 'primary', 'references', 'returning', 'select',
  'session_user', 'some', 'symmetric', 'table', 'then', 'to', 'trailing',
  'true', 'union', 'unique', 'user', 'using', 'variadic', 'when', 'where',
  'window', 'with',

  // Common SQL keywords that should be quoted (DML/DDL)
  'alter', 'begin', 'commit', 'delete', 'drop', 'insert', 'merge', 'rollback',
  'truncate', 'update',

  // JOIN keywords
  'cross', 'full', 'inner', 'join', 'left', 'natural', 'outer', 'right'
]);

/**
 * Sanitize SQL identifier to prevent injection
 * Used for table names, column names, and other identifiers in SQL queries
 */
export function sanitizeSqlIdentifier(identifier: string): string {
  // PostgreSQL identifier length limit (NAMEDATALEN - 1 = 63 bytes)
  // This prevents DoS attacks via extremely long identifiers
  if (identifier.length > 63) {
    throw new Error(`SQL identifier exceeds maximum length of 63 characters: ${identifier.substring(0, 20)}...`);
  }

  // If identifier is already quoted, validate and return it
  if (identifier.startsWith('"') && identifier.endsWith('"')) {
    const unquoted = identifier.slice(1, -1);
    // Check for SQL injection in quoted identifiers
    if (/[;'\\]|--|\*\/|\/\*/.test(unquoted)) {
      throw new Error(`Invalid SQL identifier: ${identifier}`);
    }
    // Check for unescaped double quotes
    const withoutEscapedQuotes = unquoted.replace(/""/g, '');
    if (withoutEscapedQuotes.includes('"')) {
      throw new Error(`Invalid SQL identifier: ${identifier}`);
    }
    return identifier;
  }

  // Block obvious SQL injection attempts (semicolons, comments, quotes, backslashes)
  if (/[;'"\\]|--|\*\/|\/\*/.test(identifier)) {
    throw new Error(`Invalid SQL identifier: ${identifier}`);
  }

  // Standard SQL identifiers: alphanumeric and underscore, starting with letter or underscore
  // This pattern also allows dots for qualified names (table.column)
  if (/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/.test(identifier)) {
    // Check if any part is a reserved keyword
    const parts = identifier.split('.');
    const needsQuoting = parts.some(part => RESERVED_KEYWORDS.has(part.toLowerCase()));

    if (needsQuoting) {
      // Quote each part that needs it
      const quotedParts = parts.map(part =>
        RESERVED_KEYWORDS.has(part.toLowerCase()) ? `"${part}"` : part
      );
      return quotedParts.join('.');
    }

    // Return unquoted for backward compatibility (valid identifiers are safe)
    return identifier;
  }

  // For special characters (like hyphens, Unicode), quote the identifier
  // The identifier has already been checked for SQL injection above
  return `"${identifier.replace(/"/g, '""')}"`;
}
