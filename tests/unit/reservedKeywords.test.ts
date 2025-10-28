import { sanitizeSqlIdentifier } from "../../src/index";

describe("PostgreSQL Reserved Keywords", () => {
  describe("SQL Standard keywords", () => {
    it("should quote SELECT", () => {
      expect(sanitizeSqlIdentifier("select")).toBe('"select"');
      expect(sanitizeSqlIdentifier("SELECT")).toBe('"SELECT"');
    });

    it("should quote FROM", () => {
      expect(sanitizeSqlIdentifier("from")).toBe('"from"');
    });

    it("should quote WHERE", () => {
      expect(sanitizeSqlIdentifier("where")).toBe('"where"');
    });

    it("should quote common DML keywords", () => {
      expect(sanitizeSqlIdentifier("insert")).toBe('"insert"');
      expect(sanitizeSqlIdentifier("update")).toBe('"update"');
      expect(sanitizeSqlIdentifier("delete")).toBe('"delete"');
    });

    it("should quote DDL keywords", () => {
      expect(sanitizeSqlIdentifier("create")).toBe('"create"');
      expect(sanitizeSqlIdentifier("alter")).toBe('"alter"');
      expect(sanitizeSqlIdentifier("drop")).toBe('"drop"');
      expect(sanitizeSqlIdentifier("table")).toBe('"table"');
    });

    it("should quote JOIN keywords", () => {
      expect(sanitizeSqlIdentifier("join")).toBe('"join"');
      expect(sanitizeSqlIdentifier("inner")).toBe('"inner"');
      expect(sanitizeSqlIdentifier("left")).toBe('"left"');
      expect(sanitizeSqlIdentifier("right")).toBe('"right"');
      expect(sanitizeSqlIdentifier("full")).toBe('"full"');
      expect(sanitizeSqlIdentifier("cross")).toBe('"cross"');
      expect(sanitizeSqlIdentifier("natural")).toBe('"natural"');
    });

    it("should quote logical operators", () => {
      expect(sanitizeSqlIdentifier("and")).toBe('"and"');
      expect(sanitizeSqlIdentifier("or")).toBe('"or"');
      expect(sanitizeSqlIdentifier("not")).toBe('"not"');
    });

    it("should quote comparison keywords", () => {
      expect(sanitizeSqlIdentifier("in")).toBe('"in"');
    });

    it("should quote boolean literals", () => {
      expect(sanitizeSqlIdentifier("true")).toBe('"true"');
      expect(sanitizeSqlIdentifier("false")).toBe('"false"');
      expect(sanitizeSqlIdentifier("null")).toBe('"null"');
    });

    it("should quote CASE expression keywords", () => {
      expect(sanitizeSqlIdentifier("case")).toBe('"case"');
      expect(sanitizeSqlIdentifier("when")).toBe('"when"');
      expect(sanitizeSqlIdentifier("then")).toBe('"then"');
      expect(sanitizeSqlIdentifier("else")).toBe('"else"');
      expect(sanitizeSqlIdentifier("end")).toBe('"end"');
    });

    it("should quote set operations", () => {
      expect(sanitizeSqlIdentifier("union")).toBe('"union"');
      expect(sanitizeSqlIdentifier("intersect")).toBe('"intersect"');
      expect(sanitizeSqlIdentifier("except")).toBe('"except"');
    });

    it("should quote ordering keywords", () => {
      expect(sanitizeSqlIdentifier("order")).toBe('"order"');
      expect(sanitizeSqlIdentifier("asc")).toBe('"asc"');
      expect(sanitizeSqlIdentifier("desc")).toBe('"desc"');
    });

    it("should quote grouping keywords", () => {
      expect(sanitizeSqlIdentifier("group")).toBe('"group"');
      expect(sanitizeSqlIdentifier("having")).toBe('"having"');
    });

    it("should quote limit/offset", () => {
      expect(sanitizeSqlIdentifier("limit")).toBe('"limit"');
      expect(sanitizeSqlIdentifier("offset")).toBe('"offset"');
    });

    it("should quote constraint keywords", () => {
      expect(sanitizeSqlIdentifier("constraint")).toBe('"constraint"');
      expect(sanitizeSqlIdentifier("primary")).toBe('"primary"');
      expect(sanitizeSqlIdentifier("foreign")).toBe('"foreign"');
      expect(sanitizeSqlIdentifier("references")).toBe('"references"');
      expect(sanitizeSqlIdentifier("check")).toBe('"check"');
      expect(sanitizeSqlIdentifier("unique")).toBe('"unique"');
    });
  });

  describe("PostgreSQL-specific keywords", () => {
    it("should quote transaction control keywords", () => {
      expect(sanitizeSqlIdentifier("begin")).toBe('"begin"');
      expect(sanitizeSqlIdentifier("commit")).toBe('"commit"');
      expect(sanitizeSqlIdentifier("rollback")).toBe('"rollback"');
    });

    it("should quote procedural keywords", () => {
      expect(sanitizeSqlIdentifier("do")).toBe('"do"');
    });

    it("should quote access control keywords", () => {
      expect(sanitizeSqlIdentifier("grant")).toBe('"grant"');
      expect(sanitizeSqlIdentifier("user")).toBe('"user"');
    });

    it("should quote current value functions", () => {
      expect(sanitizeSqlIdentifier("current_user")).toBe('"current_user"');
      expect(sanitizeSqlIdentifier("current_date")).toBe('"current_date"');
      expect(sanitizeSqlIdentifier("current_time")).toBe('"current_time"');
      expect(sanitizeSqlIdentifier("current_timestamp")).toBe('"current_timestamp"');
      expect(sanitizeSqlIdentifier("current_catalog")).toBe('"current_catalog"');
      expect(sanitizeSqlIdentifier("current_role")).toBe('"current_role"');
      expect(sanitizeSqlIdentifier("session_user")).toBe('"session_user"');
    });

    it("should quote window function keywords", () => {
      expect(sanitizeSqlIdentifier("window")).toBe('"window"');
    });

    it("should quote vacuum/analyze keywords", () => {
      expect(sanitizeSqlIdentifier("analyze")).toBe('"analyze"');
      expect(sanitizeSqlIdentifier("analyse")).toBe('"analyse"');
    });
  });

  describe("Less common but important keywords", () => {
    it("should quote MERGE (PostgreSQL 15+)", () => {
      expect(sanitizeSqlIdentifier("merge")).toBe('"merge"');
    });

    it("should quote RETURNING", () => {
      expect(sanitizeSqlIdentifier("returning")).toBe('"returning"');
    });

    it("should quote WITH (CTE)", () => {
      expect(sanitizeSqlIdentifier("with")).toBe('"with"');
    });

    it("should quote LATERAL", () => {
      expect(sanitizeSqlIdentifier("lateral")).toBe('"lateral"');
    });

    it("should quote ONLY", () => {
      expect(sanitizeSqlIdentifier("only")).toBe('"only"');
    });

    it("should quote FETCH", () => {
      expect(sanitizeSqlIdentifier("fetch")).toBe('"fetch"');
    });

    it("should quote INTO", () => {
      expect(sanitizeSqlIdentifier("into")).toBe('"into"');
    });
  });

  describe("Case insensitivity", () => {
    it("should quote keywords regardless of case", () => {
      expect(sanitizeSqlIdentifier("SELECT")).toBe('"SELECT"');
      expect(sanitizeSqlIdentifier("Select")).toBe('"Select"');
      expect(sanitizeSqlIdentifier("SeLeCt")).toBe('"SeLeCt"');
    });

    it("should preserve original case when quoting", () => {
      expect(sanitizeSqlIdentifier("User")).toBe('"User"');
      expect(sanitizeSqlIdentifier("USER")).toBe('"USER"');
      expect(sanitizeSqlIdentifier("user")).toBe('"user"');
    });
  });

  describe("Qualified names with keywords", () => {
    it("should quote keyword parts in qualified names", () => {
      expect(sanitizeSqlIdentifier("public.user")).toBe('public."user"');
      expect(sanitizeSqlIdentifier("user.table")).toBe('"user"."table"');
      expect(sanitizeSqlIdentifier("myschema.order")).toBe('myschema."order"');
    });

    it("should handle multiple qualified parts", () => {
      // 'table' is a keyword, others are not
      expect(sanitizeSqlIdentifier("mydb.myschema.table")).toBe(
        'mydb.myschema."table"'
      );
    });
  });

  describe("Non-keywords should not be quoted", () => {
    it("should not quote regular column names", () => {
      expect(sanitizeSqlIdentifier("username")).toBe("username");
      expect(sanitizeSqlIdentifier("email")).toBe("email");
      expect(sanitizeSqlIdentifier("id")).toBe("id");
      expect(sanitizeSqlIdentifier("created_at")).toBe("created_at");
    });

    it("should not quote table names that are not keywords", () => {
      expect(sanitizeSqlIdentifier("users")).toBe("users");
      expect(sanitizeSqlIdentifier("posts")).toBe("posts");
      expect(sanitizeSqlIdentifier("articles")).toBe("articles");
    });
  });

  describe("Edge cases with keywords", () => {
    it("should handle keywords in qualified identifier chains", () => {
      const result = sanitizeSqlIdentifier("myschema.order.id");
      expect(result).toBe('myschema."order".id');
    });

    it("should quote ALL when used as identifier", () => {
      expect(sanitizeSqlIdentifier("all")).toBe('"all"');
    });

    it("should quote ANY when used as identifier", () => {
      expect(sanitizeSqlIdentifier("any")).toBe('"any"');
    });

    it("should quote SOME when used as identifier", () => {
      expect(sanitizeSqlIdentifier("some")).toBe('"some"');
    });
  });
});
