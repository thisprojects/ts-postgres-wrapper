import { stripSqlComments } from "../../src/index";

describe("Dollar-Quoted Strings Support", () => {
  describe("Basic dollar quotes", () => {
    it("should preserve simple dollar-quoted strings", () => {
      const sql = "SELECT $$Hello World$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$Hello World$$");
    });

    it("should preserve dollar quotes with empty tag", () => {
      const sql = "SELECT $$text$$, $$more text$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$text$$, $$more text$$");
    });

    it("should preserve dollar quotes with alphanumeric tags", () => {
      const sql = "SELECT $tag$content$tag$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $tag$content$tag$");
    });

    it("should preserve dollar quotes with complex tags", () => {
      const sql = "SELECT $func_123$content$func_123$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $func_123$content$func_123$");
    });

    it("should preserve dollar quotes with underscore tags", () => {
      const sql = "SELECT $my_tag_1$content$my_tag_1$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $my_tag_1$content$my_tag_1$");
    });
  });

  describe("Dollar quotes with comment-like content", () => {
    it("should preserve -- inside dollar quotes", () => {
      const sql = "SELECT $$It's -- not a comment$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$It's -- not a comment$$");
    });

    it("should preserve /* */ inside dollar quotes", () => {
      const sql = "SELECT $$/* not a comment */$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$/* not a comment */$$");
    });

    it("should preserve both comment types inside dollar quotes", () => {
      const sql = "SELECT $$-- line\n/* block */$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$-- line\n/* block */$$");
    });

    it("should preserve single quotes inside dollar quotes", () => {
      const sql = "SELECT $$It's O'Brien's$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$It's O'Brien's$$");
    });

    it("should preserve double quotes inside dollar quotes", () => {
      const sql = 'SELECT $$"quoted" text$$';
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe('SELECT $$"quoted" text$$');
    });
  });

  describe("PostgreSQL function definitions", () => {
    it("should handle CREATE FUNCTION with dollar quotes", () => {
      const sql = `
        CREATE FUNCTION test_func() RETURNS text AS $$
          BEGIN
            -- This is a comment inside function
            RETURN 'Hello World';
          END;
        $$ LANGUAGE plpgsql;
      `;
      const stripped = stripSqlComments(sql);
      // Comment inside function body should be preserved
      expect(stripped).toContain("-- This is a comment inside function");
      expect(stripped).toContain("$$");
      expect(stripped).toContain("RETURN 'Hello World'");
    });

    it("should handle multiple dollar-quoted blocks", () => {
      const sql = `
        SELECT $a$first$a$, $b$second$b$, $c$third$c$
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("$a$first$a$");
      expect(stripped).toContain("$b$second$b$");
      expect(stripped).toContain("$c$third$c$");
    });

    it("should handle nested dollar quotes with different tags", () => {
      const sql = "SELECT $outer$Content with $inner$nested$inner$ text$outer$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $outer$Content with $inner$nested$inner$ text$outer$");
    });

    it("should handle function with comments outside dollar quotes", () => {
      const sql = `
        -- Function definition
        CREATE FUNCTION /* inline comment */ add(a int, b int) RETURNS int AS $$
          BEGIN
            RETURN a + b; -- Add two numbers
          END;
        $$ LANGUAGE plpgsql; -- End function
      `;
      const stripped = stripSqlComments(sql);
      // Comments outside function body should be removed
      expect(stripped).not.toContain("-- Function definition");
      expect(stripped).not.toContain("/* inline comment */");
      expect(stripped).not.toContain("-- End function");
      // Comment inside function body should be preserved
      expect(stripped).toContain("-- Add two numbers");
      expect(stripped).toContain("RETURN a + b");
    });
  });

  describe("Complex real-world examples", () => {
    it("should handle CREATE TRIGGER with dollar quotes", () => {
      const sql = `
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON users
        FOR EACH ROW
        EXECUTE FUNCTION $$ -- Old syntax uses dollar quotes
          NEW.updated_at = NOW(); /* Update timestamp */
          RETURN NEW;
        $$;
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("$$");
      expect(stripped).toContain("-- Old syntax uses dollar quotes");
      expect(stripped).toContain("/* Update timestamp */");
    });

    it("should handle DO blocks with dollar quotes", () => {
      const sql = `
        DO $$
        DECLARE
          r RECORD;
        BEGIN
          -- Loop through records
          FOR r IN SELECT * FROM users LOOP
            /* Process record */
            RAISE NOTICE 'User: %', r.name;
          END LOOP;
        END $$;
      `;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("-- Loop through records");
      expect(stripped).toContain("/* Process record */");
      expect(stripped).toContain("RAISE NOTICE");
    });

    it("should handle SQL injection examples in security tests", () => {
      const sql = "SELECT $$'; DROP TABLE users; --$$";
      const stripped = stripSqlComments(sql);
      // This should be preserved as-is since it's inside dollar quotes
      expect(stripped).toBe("SELECT $$'; DROP TABLE users; --$$");
    });

    it("should handle multiline dollar-quoted strings", () => {
      const sql = `SELECT $$
        Line 1 -- comment
        Line 2 /* block */
        Line 3 'quotes'
      $$`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toContain("Line 1 -- comment");
      expect(stripped).toContain("Line 2 /* block */");
      expect(stripped).toContain("Line 3 'quotes'");
    });
  });

  describe("Edge cases and error conditions", () => {
    it("should handle unclosed dollar quote gracefully", () => {
      const sql = "SELECT $$unclosed string";
      const stripped = stripSqlComments(sql);
      // Should preserve everything after opening
      expect(stripped).toBe("SELECT $$unclosed string");
    });

    it("should not match mismatched tags", () => {
      const sql = "SELECT $a$content$b$ FROM table";
      const stripped = stripSqlComments(sql);
      // $b$ doesn't match $a$, so $b$ should remain unclosed
      expect(stripped).toBe("SELECT $a$content$b$ FROM table");
    });

    it("should handle dollar signs in regular context", () => {
      const sql = "SELECT price, '$' || price FROM products";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT price, '$' || price FROM products");
    });

    it("should handle adjacent dollar quotes", () => {
      const sql = "SELECT $$first$$$$second$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$first$$$$second$$");
    });

    it("should handle empty dollar-quoted string", () => {
      const sql = "SELECT $$$$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$$$");
    });

    it("should handle dollar sign followed by non-tag characters", () => {
      const sql = "SELECT $100, $200 FROM prices";
      const stripped = stripSqlComments(sql);
      // Dollar signs followed by numbers (not valid tags) should be preserved
      expect(stripped).toBe("SELECT $100, $200 FROM prices");
    });

    it("should handle tags with special characters (invalid)", () => {
      const sql = "SELECT $tag-name$content$tag-name$";
      const stripped = stripSqlComments(sql);
      // Hyphen not allowed in tags, so this should not be recognized as dollar quote
      // Will be treated as: $ + "tag" + "-name$content$tag-name$"
      expect(stripped).toContain("$");
    });
  });

  describe("Interaction with other string types", () => {
    it("should handle dollar quotes and single quotes together", () => {
      const sql = "SELECT 'text', $$dollar$$, 'more' FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT 'text', $$dollar$$, 'more' FROM table");
    });

    it("should handle dollar quotes inside regular quotes", () => {
      // Dollar quotes inside regular quotes are just text
      const sql = "SELECT '$$not a dollar quote$$' FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT '$$not a dollar quote$$' FROM table");
    });

    it("should handle regular quotes inside dollar quotes", () => {
      const sql = "SELECT $$'single' and \"double\"$$ FROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe("SELECT $$'single' and \"double\"$$ FROM table");
    });

    it("should handle comments between different quote types", () => {
      const sql = "SELECT 'text' /* comment */, $$dollar$$ -- comment\nFROM table";
      const stripped = stripSqlComments(sql);
      expect(stripped).not.toContain("/* comment */");
      expect(stripped).not.toContain("-- comment");
      expect(stripped).toContain("'text'");
      expect(stripped).toContain("$$dollar$$");
    });
  });

  describe("Performance and stress tests", () => {
    it("should handle very long dollar-quoted strings", () => {
      const longContent = "a".repeat(10000);
      const sql = `SELECT $$${longContent}$$`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });

    it("should handle many dollar-quoted strings", () => {
      const parts = Array.from({ length: 50 }, (_, i) => `$$str${i}$$`);
      const sql = `SELECT ${parts.join(", ")}`;
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });

    it("should handle deeply nested content", () => {
      const sql = "SELECT $a$outer $b$middle $c$inner$c$ content$b$ text$a$";
      const stripped = stripSqlComments(sql);
      expect(stripped).toBe(sql);
    });
  });

  describe("Real PostgreSQL examples", () => {
    it("should handle plpgsql function with exception handling", () => {
      const sql = `
        CREATE OR REPLACE FUNCTION safe_divide(a numeric, b numeric)
        RETURNS numeric AS $$
        BEGIN
          -- Check for division by zero
          IF b = 0 THEN
            RAISE EXCEPTION 'Division by zero'; /* Error handling */
          END IF;
          RETURN a / b;
        EXCEPTION
          WHEN division_by_zero THEN
            RETURN NULL; -- Return NULL on error
        END;
        $$ LANGUAGE plpgsql STABLE;
      `;
      const stripped = stripSqlComments(sql);
      // All comments inside the function body should be preserved
      expect(stripped).toContain("-- Check for division by zero");
      expect(stripped).toContain("/* Error handling */");
      expect(stripped).toContain("-- Return NULL on error");
    });

    it("should handle SQL function returning query", () => {
      const sql = `
        CREATE FUNCTION get_active_users() RETURNS TABLE(id int, name text) AS $$
          SELECT id, name FROM users WHERE active = true -- Only active users
        $$ LANGUAGE sql;
      `;
      const stripped = stripSqlComments(sql);
      // Comment inside function should be preserved
      expect(stripped).toContain("-- Only active users");
    });

    it("should handle CREATE TYPE with dollar quotes", () => {
      const sql = `
        CREATE TYPE address AS (
          street text,
          city text,
          state text
        ); -- Type definition

        COMMENT ON TYPE address IS $$
          Address type for storing location data
          -- Can contain comments
          /* And block comments */
        $$;
      `;
      const stripped = stripSqlComments(sql);
      // Comment outside should be removed
      expect(stripped).not.toContain("-- Type definition");
      // Comments inside dollar quote should be preserved
      expect(stripped).toContain("-- Can contain comments");
      expect(stripped).toContain("/* And block comments */");
    });
  });
});
