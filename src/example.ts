import { createTypedPg, TypedPg, Pool } from "./index";

// ==========================================
// Example 2: With TypeScript schema (recommended)
// ==========================================

// Define your database schema - just plain interfaces!
interface BlogSchema {
  users: {
    id: number;
    name: string;
    email: string;
    active: boolean;
    created_at: Date;
  };
  posts: {
    id: number;
    user_id: number;
    title: string;
    content: string;
    published: boolean;
    tags: string[];
    created_at: Date;
  };
  comments: {
    id: number;
    post_id: number;
    user_id: number;
    content: string;
    created_at: Date;
  };
}

// Create typed database instance
const blogDb = createTypedPg<BlogSchema>({
  host: "localhost",
  port: 5432,
  database: "postgres",
  user: "postgres",
  password: "password",
});

async function blogExample() {
  try {
    // Type-safe queries with autocomplete - FIXED: filter/order before select
    const activeUsers = await blogDb
      .table("users")
      .where("active", "=", true) // ✅ Filter first
      .orderBy("created_at", "DESC") // ✅ Order next
      .select("id", "name", "email") // ✅ Select last
      .limit(10)
      .execute();
    const userNumber = Math.random() * 10;
    // Insert new user
    const newUsers = await blogDb.insert("users", [
      {
        name: "John Dibbs",
        email: `john@bongle${userNumber}.com`,
        active: true,
      },
      {
        name: "Jane Smipps",
        email: `jane@bongle${userNumber}.com`,
        active: true,
      },
    ]);

    // Update user
    const updatedUsers = await blogDb.update(
      "users",
      { active: false },
      { id: 1 }
    );

    // Complex query with multiple conditions - FIXED: filter/order before select
    const recentPosts = await blogDb
      .table("posts")
      .where("published", "=", true)
      .where("created_at", ">", new Date("2024-01-01"))
      .orderBy("created_at", "DESC")
      .select("id", "title", "user_id", "created_at")
      .limit(20)
      .execute();

    // Find published posts by specific tags - using raw SQL for array operations
    const taggedPosts = await blogDb.raw<{
      id: number;
      title: string;
      tags: string[];
    }>(
      `SELECT id, title, tags 
       FROM posts 
       WHERE published = true 
       AND $1 = ANY(tags)
       ORDER BY created_at DESC`,
      ["typescript"]
    );

    // Alternative: Find posts containing any of multiple tags
    const multiTaggedPosts = await blogDb.raw<{
      id: number;
      title: string;
      tags: string[];
    }>(
      `SELECT id, title, tags 
       FROM posts 
       WHERE published = true 
       AND tags && $1
       ORDER BY created_at DESC`,
      [["typescript", "javascript", "react"]] // Array overlap operator
    );

    // Find posts by multiple user IDs
    const userPosts = await blogDb
      .table("posts")
      .where("user_id", "IN", [1, 2, 3])
      .where("published", "=", true)
      .orderBy("created_at", "DESC")
      .select("id", "title", "user_id")
      .execute();

    // Get post with comments count using raw query
    const postsWithCommentCount = await blogDb.raw<{
      id: number;
      title: string;
      comment_count: number;
    }>(
      `SELECT p.id, p.title, COUNT(c.id) as comment_count 
       FROM posts p 
       LEFT JOIN comments c ON p.id = c.post_id 
       WHERE p.published = true 
       GROUP BY p.id, p.title 
       ORDER BY comment_count DESC 
       LIMIT $1`,
      [10]
    );

    // Transaction example
    await blogDb.transaction(async (tx) => {
      const userNumber = Math.random() * 10;
      const user = await tx.insert("users", {
        name: "Transaction User",
        email: `tx@example${userNumber}.com`,
        active: true,
      });

      await tx.insert("posts", {
        user_id: user[0].id,
        title: "My First Post",
        content: "Hello world!",
        published: true,
        tags: ["intro", "hello"],
      });
    });

    console.log({
      activeUsers,
      newUsers,
      recentPosts,
      postsWithCommentCount,
      taggedPosts,
      multiTaggedPosts,
      userPosts,
    });
  } catch (error) {
    console.error("Database error:", error);
  }
}

// ==========================================
// Example 3: E-commerce schema
// ==========================================

interface EcommerceSchema {
  customers: {
    id: number;
    first_name: string;
    last_name: string;
    email: string;
    phone?: string;
    created_at: Date;
  };
  products: {
    id: number;
    name: string;
    description: string;
    price: number;
    category_id: number;
    in_stock: boolean;
    tags: string[];
  };
  orders: {
    id: number;
    customer_id: number;
    total: number;
    status: "pending" | "processing" | "shipped" | "delivered" | "cancelled";
    created_at: Date;
    updated_at: Date;
  };
  order_items: {
    id: number;
    order_id: number;
    product_id: number;
    quantity: number;
    price: number;
  };
}

const ecommerceDb = createTypedPg<EcommerceSchema>({
  host: "localhost",
  port: 5432,
  database: "postgres",
  user: "postgres",
  password: "password",
});

async function ecommerceExample() {
  // Find products by category and availability - FIXED: filter/order before select
  const availableProducts = await ecommerceDb
    .table("products")
    .where("in_stock", "=", true)
    .where("category_id", "IN", [1, 2, 3])
    .orderBy("price", "ASC")
    .select("id", "name", "price")
    .execute();

  // Get expensive products with price range filtering
  const expensiveProducts = await ecommerceDb
    .table("products")
    .where("price", ">", 100)
    .where("price", "<=", 1000)
    .where("in_stock", "=", true)
    .orderBy("price", "DESC")
    .select("id", "name", "price", "category_id")
    .limit(20)
    .execute();

  // Find recent orders by status
  const recentOrders = await ecommerceDb
    .table("orders")
    .where("status", "IN", ["pending", "processing"])
    .where("created_at", ">", new Date("2024-01-01"))
    .orderBy("created_at", "DESC")
    .select("id", "customer_id", "total", "status")
    .limit(50)
    .execute();

  // Get customer orders with details using raw query
  const customerOrders = await ecommerceDb.raw<{
    order_id: number;
    customer_name: string;
    total: number;
    status: string;
    item_count: number;
  }>(
    `SELECT
      o.id as order_id,
      c.first_name || ' ' || c.last_name as customer_name,
      o.total,
      o.status,
      COUNT(oi.id) as item_count
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN order_items oi ON o.id = oi.order_id
    WHERE o.created_at > $1
    GROUP BY o.id, c.first_name, c.last_name, o.total, o.status
    ORDER BY o.created_at DESC`,
    [new Date("2024-01-01")]
  );

  console.log({
    availableProducts,
    expensiveProducts,
    recentOrders,
    customerOrders,
  });
}

// ==========================================
// Example 4: Dynamic table operations
// ==========================================

async function dynamicExample() {
  // You can still work with dynamic table names when needed
  const tableName = "users"; // Could come from config, user input, etc.
  const results = await blogDb
    .table(tableName as keyof BlogSchema)
    .limit(5)
    .execute();

  // For type-safe dynamic filtering, handle each table type specifically
  async function getTableData(table: keyof BlogSchema) {
    switch (table) {
      case "users":
        return await blogDb
          .table("users")
          .where("active", "=", true)
          .orderBy("created_at", "DESC")
          .limit(10)
          .execute();

      case "posts":
        return await blogDb
          .table("posts")
          .where("published", "=", true)
          .orderBy("created_at", "DESC")
          .limit(10)
          .execute();

      case "comments":
        return await blogDb
          .table("comments")
          .orderBy("created_at", "DESC")
          .limit(10)
          .execute();

      default:
        throw new Error(`Unknown table: ${table}`);
    }
  }

  const filteredResults = await getTableData("users");
  const postResults = await getTableData("posts");

  // Alternative: Use raw SQL for truly dynamic operations
  const dynamicRawResults = await blogDb.raw(
    `SELECT * FROM ${tableName} WHERE created_at > $1 ORDER BY created_at DESC LIMIT $2`,
    [new Date("2024-01-01"), 10]
  );

  // If you need a more flexible approach without type safety
  const flexibleResults = await blogDb.raw(
    `SELECT * FROM ${tableName} ORDER BY created_at DESC LIMIT $1`,
    [5]
  );

  console.log({
    results: results.length,
    filteredResults: filteredResults.length,
    postResults: postResults.length,
    dynamicRawResults: dynamicRawResults.length,
    flexibleResults: flexibleResults.length,
  });
}

// ==========================================
// Example 5: Utility functions for common patterns
// ==========================================

// Helper function to find user by email
async function findUserByEmail(email: string) {
  return await blogDb
    .table("users")
    .where("email", "=", email)
    .select("id", "name", "email", "active") // Select after where for type safety
    .first();
}

// Helper function to find active users with pagination
async function findActiveUsers(limit: number = 10, offset: number = 0) {
  return await blogDb
    .table("users")
    .where("active", "=", true)
    .orderBy("created_at", "DESC")
    .select("id", "name", "email", "created_at")
    .limit(limit)
    .offset(offset)
    .execute();
}

// Helper function for paginated results
async function getPaginatedPosts(page: number = 1, limit: number = 10) {
  const offset = (page - 1) * limit;

  const [posts, total] = await Promise.all([
    blogDb
      .table("posts")
      .where("published", "=", true)
      .orderBy("created_at", "DESC")
      .select("id", "title", "created_at", "user_id")
      .limit(limit)
      .offset(offset)
      .execute(),

    blogDb.table("posts").where("published", "=", true).count(),
  ]);

  return {
    posts,
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
      hasNextPage: page * limit < total,
      hasPrevPage: page > 1,
    },
  };
}

// Helper function to get posts by user with filtering
async function getPostsByUser(userId: number, publishedOnly: boolean = true) {
  let query = blogDb.table("posts").where("user_id", "=", userId);

  if (publishedOnly) {
    query = query.where("published", "=", true);
  }

  return await query
    .orderBy("created_at", "DESC")
    .select("id", "title", "content", "published", "created_at")
    .execute();
}

// Helper function for search with multiple OR conditions
async function searchUsersByNameOrEmail(searchTerm: string) {
  // For complex OR queries, raw SQL is often cleaner
  return await blogDb.raw<{
    id: number;
    name: string;
    email: string;
    active: boolean;
  }>(
    `SELECT id, name, email, active 
     FROM users 
     WHERE (name ILIKE $1 OR email ILIKE $1) 
     AND active = true 
     ORDER BY name`,
    [`%${searchTerm}%`]
  );
}

/**
 * Note about array fields:
 * PostgreSQL array operations like @>, &&, = ANY() require raw SQL
 * because they're not supported in the simplified where() method.
 * For array fields, use the raw() method for complex queries.
 */

async function arrayQueryExamples() {
  // PostgreSQL array operators require raw SQL for complex queries

  // 1. Check if array contains a specific value (= ANY)
  const postsWithTypeScript = await blogDb.raw<{
    id: number;
    title: string;
    tags: string[];
  }>(
    `SELECT id, title, tags 
     FROM posts 
     WHERE published = true 
     AND 'typescript' = ANY(tags)
     ORDER BY created_at DESC`,
    []
  );

  // 2. Check if array overlaps with another array (&&)
  const postsWithAnyTag = await blogDb.raw<{
    id: number;
    title: string;
    tags: string[];
  }>(
    `SELECT id, title, tags 
     FROM posts 
     WHERE published = true 
     AND tags && $1
     ORDER BY created_at DESC`,
    [["typescript", "javascript", "react"]]
  );

  // 3. Check if array contains all specified values (@>)
  const postsWithAllTags = await blogDb.raw<{
    id: number;
    title: string;
    tags: string[];
  }>(
    `SELECT id, title, tags 
     FROM posts 
     WHERE published = true 
     AND tags @> $1
     ORDER BY created_at DESC`,
    [["typescript", "tutorial"]]
  );

  // 4. Check if array is contained by another array (<@)
  const postsOnlyWithSpecificTags = await blogDb.raw<{
    id: number;
    title: string;
    tags: string[];
  }>(
    `SELECT id, title, tags 
     FROM posts 
     WHERE published = true 
     AND tags <@ $1
     ORDER BY created_at DESC`,
    [["typescript", "javascript", "react", "tutorial"]]
  );

  // 5. Array length queries
  const postsWithManyTags = await blogDb.raw<{
    id: number;
    title: string;
    tags: string[];
    tag_count: number;
  }>(
    `SELECT id, title, tags, array_length(tags, 1) as tag_count
     FROM posts 
     WHERE published = true 
     AND array_length(tags, 1) > $1
     ORDER BY tag_count DESC`,
    [3]
  );

  // 6. Using array functions with ILIKE for partial matching
  const postsWithTagLike = await blogDb.raw<{
    id: number;
    title: string;
    tags: string[];
  }>(
    `SELECT id, title, tags 
     FROM posts 
     WHERE published = true 
     AND EXISTS (
       SELECT 1 FROM unnest(tags) tag 
       WHERE tag ILIKE $1
     )
     ORDER BY created_at DESC`,
    ["%script%"] // Matches "typescript", "javascript", etc.
  );

  console.log({
    postsWithTypeScript: postsWithTypeScript.length,
    postsWithAnyTag: postsWithAnyTag.length,
    postsWithAllTags: postsWithAllTags.length,
    postsOnlyWithSpecificTags: postsOnlyWithSpecificTags.length,
    postsWithManyTags: postsWithManyTags.length,
    postsWithTagLike: postsWithTagLike.length,
  });
}

async function advancedQueryExamples() {
  // Example with multiple date range filters
  const postsInRange = await blogDb
    .table("posts")
    .where("created_at", ">=", new Date("2024-01-01"))
    .where("created_at", "<", new Date("2024-12-31"))
    .where("published", "=", true)
    .orderBy("created_at", "ASC")
    .select("id", "title", "user_id", "created_at")
    .execute();

  // Example with LIKE pattern matching
  const blogPosts = await blogDb
    .table("posts")
    .where("title", "ILIKE", "%blog%")
    .where("published", "=", true)
    .orderBy("created_at", "DESC")
    .select("id", "title", "content")
    .limit(5)
    .execute();

  // Get first unpublished post (example of using first())
  const draftPost = await blogDb
    .table("posts")
    .where("published", "=", false)
    .orderBy("created_at", "DESC")
    .select("id", "title", "user_id")
    .first();

  // Count active users (no select needed for count)
  const activeUserCount = await blogDb
    .table("users")
    .where("active", "=", true)
    .count();

  console.log({
    postsInRange: postsInRange.length,
    blogPosts: blogPosts.length,
    draftPost,
    activeUserCount,
  });
}

// Export everything for use in other modules
export {
  blogDb,
  ecommerceDb,
  findUserByEmail,
  findActiveUsers,
  getPaginatedPosts,
  getPostsByUser,
  searchUsersByNameOrEmail,
  type BlogSchema,
  type EcommerceSchema,
};

// Example usage in main function
async function main() {
  try {
    console.log("Running blog example...");
    await blogExample();

    console.log("Running e-commerce example...");
    await ecommerceExample();

    console.log("Running dynamic example...");
    await dynamicExample();

    console.log("Running advanced query examples...");
    await advancedQueryExamples();

    console.log("Running array query examples...");
    await arrayQueryExamples();

    console.log("Testing utility functions...");
    const user = await findUserByEmail("john@example.com");
    console.log("Found user:", user);

    const activeUsers = await findActiveUsers(5, 0);
    console.log("Active users:", activeUsers);

    const paginatedPosts = await getPaginatedPosts(1, 5);
    console.log("Paginated posts:", paginatedPosts);

    const userPosts = await getPostsByUser(1, true);
    console.log("User posts:", userPosts);

    const searchResults = await searchUsersByNameOrEmail("john");
    console.log("Search results:", searchResults);
  } catch (error) {
    console.error("Example failed:", error);
  } finally {
    // Clean up connections
    await blogDb.close();
    await ecommerceDb.close();
  }
}

// Uncomment to run examples
main().catch(console.error);
