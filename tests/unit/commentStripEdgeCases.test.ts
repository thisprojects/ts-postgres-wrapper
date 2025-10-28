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
