import { stripSqlComments } from "../../src/index";

describe("stripSqlComments - PostgreSQL Edge Cases", () => {
  describe("PostgreSQL escaped quotes", () => {
    it("should preserve escaped single quotes in strings", () => {
      const sql = "SELECT * FROM users WHERE name = 'O''Brien'";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users WHERE name = 'O''Brien'");
    });

    it("should preserve escaped double quotes in strings", () => {
      const sql = 'SELECT * FROM users WHERE name = "He said ""hello"""';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT * FROM users WHERE name = "He said ""hello"""');
    });

    it("should handle multiple escaped quotes in one string", () => {
      const sql = "SELECT 'It''s a test''s result' FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'It''s a test''s result' FROM table");
    });

    it("should handle escaped quotes at the end of string", () => {
      const sql = "SELECT 'test''', 'data' FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'test''', 'data' FROM table");
    });

    it("should handle mix of single and double quoted strings", () => {
      const sql = `SELECT 'It''s OK', "He said ""yes""" FROM table`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(`SELECT 'It''s OK', "He said ""yes""" FROM table`);
    });
  });

  describe("Comments inside strings", () => {
    it("should not strip -- inside single-quoted strings", () => {
      const sql = "SELECT '-- not a comment' FROM users";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT '-- not a comment' FROM users");
    });

    it("should not strip /* */ inside single-quoted strings", () => {
      const sql = "SELECT '/* not a comment */' FROM users";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT '/* not a comment */' FROM users");
    });

    it("should not strip -- inside double-quoted strings", () => {
      const sql = 'SELECT "-- not a comment" FROM users';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT "-- not a comment" FROM users');
    });

    it("should not strip /* */ inside double-quoted strings", () => {
      const sql = 'SELECT "/* not a comment */" FROM users';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT "/* not a comment */" FROM users');
    });

    it("should handle escaped quotes with comment syntax inside", () => {
      const sql = "SELECT 'It''s -- a test' FROM users -- real comment";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'It''s -- a test' FROM users");
    });
  });

  describe("Complex mixed scenarios", () => {
    it("should handle comments and escaped quotes together", () => {
      const sql = `
        SELECT * FROM users /* block comment */
        WHERE name = 'O''Brien' -- line comment
        AND status = 'active'
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("WHERE name = 'O''Brien'");
      expect(stripped).not.toContain("/* block comment */");
      expect(stripped).not.toContain("-- line comment");
      expect(stripped).toContain("AND status = 'active'");
    });

    it("should handle nested comment-like patterns", () => {
      const sql = "SELECT '/* nested */' FROM t WHERE c = '-- test' /* comment */";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT '/* nested */' FROM t WHERE c = '-- test'");
    });

    it("should handle the complex edge case from Issue 1", () => {
      // Edge case: String containing comment-like patterns
      const sql = `SELECT '/* not a comment */' AS value,
  name FROM users WHERE description LIKE '-- not a comment'`;
      const stripped = stripSqlComments(sql);

      // Verify comment-like patterns inside strings are preserved
      expect(stripped).toContain("'/* not a comment */'");
      expect(stripped).toContain("'-- not a comment'");
      expect(stripped).toContain("AS value");
      expect(stripped).toContain("name FROM users");
      expect(stripped).toContain("WHERE description LIKE");
    });

    it("should handle edge case with both string comments and real comments", () => {
      // String contains comment patterns, followed by real comment
      const sql = `SELECT '/* not a comment */' AS value, -- actual comment
  name FROM users WHERE description LIKE '-- not a comment'`;
      const stripped = stripSqlComments(sql);

      // Verify comment-like patterns inside strings are preserved
      expect(stripped).toContain("'/* not a comment */'");
      expect(stripped).toContain("'-- not a comment'");
      expect(stripped).toContain("AS value,");

      // Verify actual comment is stripped
      expect(stripped).not.toContain("-- actual comment");

      // Verify query structure is intact
      expect(stripped).toContain("name FROM users");
      expect(stripped).toContain("WHERE description LIKE");
    });

    it("should handle URLs with // inside strings", () => {
      const sql = "SELECT 'https://example.com' FROM urls -- comment";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'https://example.com' FROM urls");
    });

    it("should preserve email addresses with @ inside strings", () => {
      const sql = "SELECT 'user@example.com' FROM users /* comment */";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'user@example.com' FROM users");
    });
  });

  describe("Multiline queries with escaped quotes", () => {
    it("should handle multiline strings with escaped quotes", () => {
      const sql = `
        INSERT INTO posts (title, content)
        VALUES ('Today''s News', 'It''s a beautiful day')
        -- This is a comment
        RETURNING *
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("VALUES ('Today''s News', 'It''s a beautiful day')");
      expect(stripped).not.toContain("-- This is a comment");
      expect(stripped).toContain("RETURNING *");
    });

    it("should handle block comments spanning multiple lines with strings", () => {
      const sql = `
        SELECT 'test''s value'
        /* This is a
           multiline
           comment */
        FROM table WHERE status = 'it''s active'
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("SELECT 'test''s value'");
      expect(stripped).not.toContain("multiline");
      expect(stripped).toContain("FROM table WHERE status = 'it''s active'");
    });
  });

  describe("Edge cases with empty strings", () => {
    it("should handle empty single-quoted strings", () => {
      const sql = "SELECT '', 'data' FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT '', 'data' FROM table");
    });

    it("should handle empty double-quoted strings", () => {
      const sql = 'SELECT "", "data" FROM table';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT "", "data" FROM table');
    });

    it("should handle consecutive empty strings", () => {
      const sql = "SELECT '', '', 'data' FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT '', '', 'data' FROM table");
    });
  });

  describe("Comments at various positions", () => {
    it("should strip comment at start of query", () => {
      const sql = "-- Initial comment\nSELECT * FROM users";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users");
    });

    it("should strip comment at end of query", () => {
      const sql = "SELECT * FROM users\n-- Final comment";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users");
    });

    it("should handle multiple consecutive line comments", () => {
      const sql = `
        SELECT * FROM users
        -- comment 1
        -- comment 2
        -- comment 3
        WHERE id = 1
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("SELECT * FROM users");
      expect(stripped).toContain("WHERE id = 1");
      expect(stripped).not.toContain("-- comment");
    });

    it("should handle block comment at start", () => {
      const sql = "/* Initial comment */ SELECT * FROM users";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users");
    });

    it("should handle block comment at end", () => {
      const sql = "SELECT * FROM users /* Final comment */";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT * FROM users");
    });
  });

  describe("Special PostgreSQL cases", () => {
    it("should handle dollar-quoted strings (fully supported)", () => {
      const sql = "SELECT $$It's -- a test$$";
      const stripped = stripSqlComments(sql);
      // Dollar quotes are now fully supported!
      expect(stripped).toBe(sql);
    });

    it("should handle dollar-quoted strings with comment-like patterns", () => {
      const sql = "SELECT $$/* not a comment */ -- also not$$, 'test' FROM t";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });

    it("should handle tagged dollar quotes with comments inside", () => {
      const sql = "SELECT $tag$It's -- not a /* comment */ either$tag$ FROM t";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });

    it("should handle dollar quotes with actual comments after", () => {
      const sql = "SELECT $$string with -- inside$$ FROM t -- real comment";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$string with -- inside$$ FROM t");
    });

    it("should handle nested dollar quotes with different tags", () => {
      const sql = "SELECT $outer$text $inner$nested$inner$ text$outer$ FROM t";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });

    it("should handle identifier quotes (double quotes for column names)", () => {
      const sql = 'SELECT "user_name" FROM "table_name" -- comment';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT "user_name" FROM "table_name"');
    });

    it("should handle mix of identifiers and string literals", () => {
      const sql = `SELECT "column_name", 'string''s value' FROM "table"`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(`SELECT "column_name", 'string''s value' FROM "table"`);
    });

    it("should handle comment markers in double-quoted identifiers", () => {
      const sql = 'SELECT "col--name" FROM "table/*test*/" -- real comment';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT "col--name" FROM "table/*test*/"');
    });
  });

  describe("Stress tests", () => {
    it("should handle very long strings with escaped quotes", () => {
      const longString = "a''".repeat(100);
      const sql = `SELECT '${longString}' FROM table`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });

    it("should handle queries with many strings", () => {
      const sql = "SELECT 'a', 'b', 'c', 'd', 'e' /* comment */ FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'a', 'b', 'c', 'd', 'e'  FROM table");
    });

    it("should handle alternating strings and comments", () => {
      const sql = "SELECT 'a' /* c1 */ , 'b' /* c2 */ , 'c' /* c3 */ FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'a'  , 'b'  , 'c'  FROM table");
    });

    it("should handle complex real-world query with all edge cases", () => {
      const sql = `
        -- Query header comment
        SELECT
          '/* not a comment */' AS block_comment_pattern,
          '-- not a comment' AS line_comment_pattern,
          "col--name" AS identifier, -- real comment here
          $$dollar /* -- quoted */ string$$ AS dollar_str,
          'O''Brien''s test' AS escaped_quotes /* another comment */
        FROM users
        WHERE description LIKE '%--test%' -- Filter with comment pattern
          AND status = 'active' /* Block
                                  comment
                                  multiline */
          AND data @> '{"key": "value"}' -- JSON
      `;
      const stripped = stripSqlComments(sql);

      // Verify all string contents are preserved
      expect(stripped).toContain("'/* not a comment */'");
      expect(stripped).toContain("'-- not a comment'");
      expect(stripped).toContain('"col--name"');
      expect(stripped).toContain("$$dollar /* -- quoted */ string$$");
      expect(stripped).toContain("'O''Brien''s test'");
      expect(stripped).toContain("'%--test%'");
      expect(stripped).toContain("'active'");
      expect(stripped).toContain('\'{"key": "value"}\'');

      // Verify all actual comments are stripped
      expect(stripped).not.toContain("-- Query header comment");
      expect(stripped).not.toContain("-- real comment here");
      expect(stripped).not.toContain("-- Filter with comment pattern");
      expect(stripped).not.toContain("-- JSON");
      expect(stripped).not.toContain("/* another comment */");
      expect(stripped).not.toContain("/* Block");
      expect(stripped).not.toContain("multiline */");

      // Verify query structure is intact
      expect(stripped).toContain("SELECT");
      expect(stripped).toContain("FROM users");
      expect(stripped).toContain("WHERE description LIKE");
      expect(stripped).toContain("AND status =");
      expect(stripped).toContain("AND data @>");
    });

    it("should handle pathological case with adjacent quotes and comments", () => {
      const sql = `SELECT ''/* comment */, '--'-- comment
, "col"FROM t`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("''");
      expect(stripped).toContain("'--'");
      expect(stripped).toContain('"col"');
      expect(stripped).not.toContain("/* comment */");
      expect(stripped).not.toContain("-- comment");
    });
  });

  describe("Error conditions", () => {
    it("should handle unclosed string gracefully", () => {
      const sql = "SELECT 'unclosed string FROM table";
      const stripped = stripSqlComments(sql);
      // Should preserve everything after the opening quote
      expect(stripped).toBe("SELECT 'unclosed string FROM table");
    });

    it("should handle unclosed block comment gracefully", () => {
      const sql = "SELECT * /* unclosed comment";
      const stripped = stripSqlComments(sql);
      // Should remove everything after /*
      expect(stripped).toBe("SELECT *");
    });

    it("should handle */ without opening /*", () => {
      const sql = "SELECT * FROM table */ WHERE id = 1";
      const stripped = stripSqlComments(sql);
      // */ without /* should be treated as regular text
      expect(stripped).toBe("SELECT * FROM table */ WHERE id = 1");
    });
  });
});
