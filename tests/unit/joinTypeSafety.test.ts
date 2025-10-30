/**
 * Tests for JOIN type safety improvements
 * Verifies that JOIN operations properly merge table types
 */

import { TypedQuery } from "../../src";
import { MockPool } from "../test_utils/MockPool";

interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
    age: number;
  };
  posts: {
    id: number;
    user_id: number;
    title: string;
    content: string;
    published: boolean;
  };
  comments: {
    id: number;
    post_id: number;
    user_id: number;
    text: string;
    created_at: Date;
  };
}

describe("JOIN Type Safety", () => {
  let pool: MockPool;
  let usersQuery: TypedQuery<"users", TestSchema["users"], TestSchema>;

  beforeEach(() => {
    pool = new MockPool();
    usersQuery = new TypedQuery<"users", TestSchema["users"], TestSchema>(
      pool as any,
      "users",
      {} as TestSchema
    );
  });

  describe("INNER JOIN", () => {
    it("should merge types for INNER JOIN", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          user_id: 1,
          title: "First Post",
          content: "Hello World",
          published: true,
        },
      ]);

      const result = await usersQuery
        .innerJoin("posts", "users.id", "posts.user_id")
        .execute();

      // Type assertion to verify the merged type works
      const row = result[0];

      // Should have both users and posts columns
      const userName: string = row.name; // from users
      const postTitle: string = row.title; // from posts
      const userAge: number = row.age; // from users
      const published: boolean = row.published; // from posts

      expect(userName).toBe("Alice");
      expect(postTitle).toBe("First Post");
      expect(userAge).toBe(30);
      expect(published).toBe(true);
    });

    it("should support compound key INNER JOIN with type safety", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          post_id: 1,
          user_id: 1,
          text: "Great post!",
          created_at: new Date(),
        },
      ]);

      const result = await usersQuery
        .innerJoin("comments", [
          { leftColumn: "users.id", rightColumn: "comments.user_id" },
        ])
        .execute();

      const row = result[0];

      // Should have columns from both tables
      const userName: string = row.name;
      const commentText: string = row.text;
      const commentDate: Date = row.created_at;

      expect(userName).toBe("Alice");
      expect(commentText).toBe("Great post!");
      expect(commentDate).toBeInstanceOf(Date);
    });
  });

  describe("LEFT JOIN", () => {
    it("should make joined table columns optional for LEFT JOIN", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          user_id: 1,
          title: "First Post",
          content: "Hello",
          published: true,
        },
        {
          id: 2,
          name: "Bob",
          email: "bob@example.com",
          age: 25,
          user_id: null,
          title: null,
          content: null,
          published: null,
        },
      ]);

      const result = await usersQuery
        .leftJoin("posts", "users.id", "posts.user_id")
        .execute();

      const row1 = result[0];
      const row2 = result[1];

      // Users columns are always present
      const userName1: string = row1.name;
      const userName2: string = row2.name;

      // Posts columns might be undefined (LEFT JOIN)
      const postTitle1: string | undefined = row1.title;
      const postTitle2: string | undefined = row2.title;

      expect(userName1).toBe("Alice");
      expect(userName2).toBe("Bob");
      expect(postTitle1).toBe("First Post");
      expect(postTitle2).toBeNull();
    });
  });

  describe("RIGHT JOIN", () => {
    it("should make original table columns optional for RIGHT JOIN", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          user_id: 1,
          title: "Post with author",
          content: "Content",
          published: true,
        },
        {
          id: null,
          name: null,
          email: null,
          age: null,
          user_id: 99,
          title: "Orphaned post",
          content: "No author",
          published: false,
        },
      ]);

      const result = await usersQuery
        .rightJoin("posts", "users.id", "posts.user_id")
        .execute();

      const row1 = result[0];
      const row2 = result[1];

      // Posts columns are always present (RIGHT JOIN)
      const postTitle1: string = row1.title;
      const postTitle2: string = row2.title;

      // Users columns might be undefined
      const userName1: string | undefined = row1.name;
      const userName2: string | undefined = row2.name;

      expect(postTitle1).toBe("Post with author");
      expect(postTitle2).toBe("Orphaned post");
      expect(userName1).toBe("Alice");
      expect(userName2).toBeNull();
    });
  });

  describe("FULL JOIN", () => {
    it("should make all columns optional for FULL JOIN", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          user_id: 1,
          title: "Matched post",
          content: "Content",
          published: true,
        },
        {
          id: 2,
          name: "Bob",
          email: "bob@example.com",
          age: 25,
          user_id: null,
          title: null,
          content: null,
          published: null,
        },
        {
          id: null,
          name: null,
          email: null,
          age: null,
          user_id: 99,
          title: "Orphaned post",
          content: "No author",
          published: false,
        },
      ]);

      const result = await usersQuery
        .fullJoin("posts", "users.id", "posts.user_id")
        .execute();

      const row1 = result[0];
      const row2 = result[1];
      const row3 = result[2];

      // All columns might be undefined (FULL JOIN)
      const userName1: string | undefined = row1.name;
      const postTitle1: string | undefined = row1.title;

      const userName2: string | undefined = row2.name;
      const postTitle2: string | undefined = row2.title;

      const userName3: string | undefined = row3.name;
      const postTitle3: string | undefined = row3.title;

      expect(userName1).toBe("Alice");
      expect(postTitle1).toBe("Matched post");

      expect(userName2).toBe("Bob");
      expect(postTitle2).toBeNull();

      expect(userName3).toBeNull();
      expect(postTitle3).toBe("Orphaned post");
    });
  });

  describe("Multiple JOINs", () => {
    it("should accumulate types across multiple JOINs", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          user_id: 1,
          title: "First Post",
          content: "Hello",
          published: true,
          post_id: 1,
          text: "Great!",
          created_at: new Date(),
        },
      ]);

      const result = await usersQuery
        .innerJoin("posts", "users.id", "posts.user_id")
        .innerJoin("comments", "posts.id", "comments.post_id")
        .execute();

      const row = result[0];

      // Should have columns from all three tables
      const userName: string = row.name; // from users
      const postTitle: string = row.title; // from posts
      const commentText: string = row.text; // from comments

      expect(userName).toBe("Alice");
      expect(postTitle).toBe("First Post");
      expect(commentText).toBe("Great!");
    });

    it("should handle mixed JOIN types with proper optionality", async () => {
      pool.setMockResults([
        {
          id: 1,
          name: "Alice",
          email: "alice@example.com",
          age: 30,
          user_id: 1,
          title: "Post",
          content: "Content",
          published: true,
          post_id: null,
          text: null,
          created_at: null,
        },
      ]);

      const result = await usersQuery
        .innerJoin("posts", "users.id", "posts.user_id")
        .leftJoin("comments", "posts.id", "comments.post_id")
        .execute();

      const row = result[0];

      // users and posts columns required (INNER JOIN)
      const userName: string = row.name;
      const postTitle: string = row.title;

      // comments columns optional (LEFT JOIN)
      const commentText: string | undefined = row.text;

      expect(userName).toBe("Alice");
      expect(postTitle).toBe("Post");
      expect(commentText).toBeNull();
    });
  });

  describe("Backward compatibility", () => {
    it("should still support string-based table names (returns any)", async () => {
      pool.setMockResults([{ id: 1, name: "Alice", title: "Post" }]);

      // Using string literal instead of schema key - should work but return 'any'
      const stringTableQuery = new TypedQuery(pool as any, "users");

      const result = await stringTableQuery
        .innerJoin("unknown_table", "users.id", "unknown_table.user_id")
        .execute();

      // Still works at runtime
      expect(result[0].id).toBe(1);
      expect(result[0].name).toBe("Alice");
    });
  });
});
