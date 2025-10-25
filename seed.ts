import { Pool } from "pg";

// npx ts-node seed.ts

// Database connection configuration
const pool = new Pool({
  host: "localhost",
  port: 5432,
  database: "postgres",
  user: "postgres",
  password: "password",
});

async function seedDatabase() {
  const client = await pool.connect();

  try {
    console.log("🌱 Starting database seeding...");

    // Drop existing tables if they exist (for clean setup)
    await client.query(`
      DROP TABLE IF EXISTS order_items CASCADE;
      DROP TABLE IF EXISTS orders CASCADE;
      DROP TABLE IF EXISTS products CASCADE;
      DROP TABLE IF EXISTS customers CASCADE;
      DROP TABLE IF EXISTS comments CASCADE;
      DROP TABLE IF EXISTS posts CASCADE;
      DROP TABLE IF EXISTS users CASCADE;
    `);

    console.log("🗑️  Dropped existing tables");

    // ==========================================
    // Blog Schema Tables
    // ==========================================

    // Create users table - matches BlogSchema.users
    await client.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);

    console.log("✅ Created users table");

    // Create posts table - matches BlogSchema.posts
    await client.query(`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        title VARCHAR(500) NOT NULL,
        content TEXT,
        published BOOLEAN DEFAULT FALSE,
        tags TEXT[] DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);

    console.log("✅ Created posts table");

    // Create comments table - matches BlogSchema.comments
    await client.query(`
      CREATE TABLE comments (
        id SERIAL PRIMARY KEY,
        post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        content TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);

    console.log("✅ Created comments table");

    // ==========================================
    // E-commerce Schema Tables
    // ==========================================

    // Create customers table - matches EcommerceSchema.customers
    await client.query(`
      CREATE TABLE customers (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        phone VARCHAR(20),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);

    console.log("✅ Created customers table");

    // Create products table - matches EcommerceSchema.products
    await client.query(`
      CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        price DECIMAL(10,2) NOT NULL,
        category_id INTEGER NOT NULL,
        in_stock BOOLEAN DEFAULT TRUE,
        tags TEXT[] DEFAULT '{}'
      );
    `);

    console.log("✅ Created products table");

    // Create orders table - matches EcommerceSchema.orders
    await client.query(`
      CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
        total DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')) DEFAULT 'pending',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);

    console.log("✅ Created orders table");

    // Create order_items table - matches EcommerceSchema.order_items
    await client.query(`
      CREATE TABLE order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
        product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        price DECIMAL(10,2) NOT NULL
      );
    `);

    console.log("✅ Created order_items table");

    // Insert sample users
    const userInsertQuery = `
      INSERT INTO users (name, email, active, created_at) VALUES
      ($1, $2, $3, $4),
      ($5, $6, $7, $8),
      ($9, $10, $11, $12),
      ($13, $14, $15, $16),
      ($17, $18, $19, $20)
      RETURNING id;
    `;

    const userValues = [
      "John Doe",
      "john@example.com",
      true,
      new Date("2024-01-15"),
      "Jane Smith",
      "jane@example.com",
      true,
      new Date("2024-02-01"),
      "Bob Johnson",
      "bob@example.com",
      true,
      new Date("2024-01-05"),
      "Alice Brown",
      "alice@example.com",
      false, // inactive user for testing
      new Date("2023-12-20"),
      "Charlie Wilson",
      "charlie@example.com",
      true,
      new Date("2024-03-10"),
    ];

    const userResult = await client.query(userInsertQuery, userValues);
    console.log(`✅ Inserted ${userResult.rowCount} users`);

    // Insert sample posts with tags
    const postInsertQuery = `
      INSERT INTO posts (user_id, title, content, published, tags, created_at) VALUES
      ($1, $2, $3, $4, $5, $6),
      ($7, $8, $9, $10, $11, $12),
      ($13, $14, $15, $16, $17, $18),
      ($19, $20, $21, $22, $23, $24),
      ($25, $26, $27, $28, $29, $30),
      ($31, $32, $33, $34, $35, $36),
      ($37, $38, $39, $40, $41, $42),
      ($43, $44, $45, $46, $47, $48),
      ($49, $50, $51, $52, $53, $54),
      ($55, $56, $57, $58, $59, $60);
    `;

    const postValues = [
      1,
      "Getting Started with TypeScript",
      "TypeScript is a powerful superset of JavaScript that adds static typing...",
      true,
      ["typescript", "javascript", "tutorial"],
      new Date("2024-01-16"),

      1,
      "Advanced Database Queries",
      "Let's explore some advanced PostgreSQL features including array operations...",
      true,
      ["postgresql", "database", "sql"],
      new Date("2024-01-20"),

      2,
      "Draft: My Thoughts on AI",
      "This is still a work in progress about artificial intelligence...",
      false,
      ["ai", "draft", "machine-learning"],
      new Date("2024-02-02"),

      2,
      "The Future of Web Development",
      "Web development is constantly evolving with new frameworks and tools...",
      true,
      ["web-development", "react", "javascript"],
      new Date("2024-02-05"),

      3,
      "Building Scalable APIs",
      "When building APIs, scalability should be a primary concern from the start...",
      true,
      ["api", "backend", "scalability"],
      new Date("2024-01-10"),

      3,
      "Draft: Performance Optimization",
      "Some notes on optimization techniques for web applications...",
      false,
      ["performance", "optimization", "draft"],
      new Date("2024-01-12"),

      4,
      "Understanding Database Indexing",
      "Proper indexing can dramatically improve query performance in PostgreSQL...",
      true,
      ["database", "postgresql", "indexing", "performance"],
      new Date("2023-12-25"),

      5,
      "Modern JavaScript Features",
      "ES2024 brings some exciting new features to the JavaScript language...",
      true,
      ["javascript", "es2024", "modern"],
      new Date("2024-03-15"),

      1,
      "TypeScript Best Practices",
      "Learn the best practices for writing maintainable TypeScript code...",
      true,
      ["typescript", "best-practices", "coding"],
      new Date("2024-01-25"),

      2,
      "React Hooks Deep Dive",
      "A comprehensive guide to React hooks and their use cases...",
      true,
      ["react", "hooks", "frontend", "javascript"],
      new Date("2024-02-10"),
    ];

    const postResult = await client.query(postInsertQuery, postValues);
    console.log(`✅ Inserted ${postResult.rowCount} posts`);

    // Insert sample comments
    const commentInsertQuery = `
      INSERT INTO comments (post_id, user_id, content, created_at) VALUES
      ($1, $2, $3, $4),
      ($5, $6, $7, $8),
      ($9, $10, $11, $12),
      ($13, $14, $15, $16),
      ($17, $18, $19, $20),
      ($21, $22, $23, $24),
      ($25, $26, $27, $28),
      ($29, $30, $31, $32);
    `;

    const commentValues = [
      1,
      2,
      "Great tutorial! Very helpful for beginners.",
      new Date("2024-01-17"),
      1,
      3,
      "Thanks for sharing this. The examples are clear.",
      new Date("2024-01-18"),
      2,
      1,
      "Interesting perspective on PostgreSQL arrays.",
      new Date("2024-01-21"),
      4,
      5,
      "Web development keeps evolving indeed!",
      new Date("2024-02-06"),
      5,
      2,
      "Scalability tips are always welcome.",
      new Date("2024-01-11"),
      7,
      3,
      "Database indexing is such an important topic.",
      new Date("2023-12-26"),
      8,
      1,
      "ES2024 features look promising.",
      new Date("2024-03-16"),
      9,
      4,
      "Love the TypeScript best practices!",
      new Date("2024-01-26"),
    ];

    const commentResult = await client.query(commentInsertQuery, commentValues);
    console.log(`✅ Inserted ${commentResult.rowCount} comments`);

    // ==========================================
    // E-commerce Sample Data
    // ==========================================

    // Insert sample customers
    const customerInsertQuery = `
      INSERT INTO customers (first_name, last_name, email, phone, created_at) VALUES
      ($1, $2, $3, $4, $5),
      ($6, $7, $8, $9, $10),
      ($11, $12, $13, $14, $15),
      ($16, $17, $18, $19, $20),
      ($21, $22, $23, $24, $25)
      RETURNING id;
    `;

    const customerValues = [
      "Emma",
      "Johnson",
      "emma.johnson@email.com",
      "+1-555-0101",
      new Date("2024-01-10"),
      "Michael",
      "Smith",
      "michael.smith@email.com",
      "+1-555-0102",
      new Date("2024-01-15"),
      "Sarah",
      "Davis",
      "sarah.davis@email.com",
      "+1-555-0103",
      new Date("2024-02-01"),
      "David",
      "Wilson",
      "david.wilson@email.com",
      "+1-555-0104",
      new Date("2024-02-10"),
      "Lisa",
      "Brown",
      "lisa.brown@email.com",
      "+1-555-0105",
      new Date("2024-03-01"),
    ];

    const customerResult = await client.query(
      customerInsertQuery,
      customerValues
    );
    console.log(`✅ Inserted ${customerResult.rowCount} customers`);

    // Insert sample products
    const productInsertQuery = `
      INSERT INTO products (name, description, price, category_id, in_stock, tags) VALUES
      ($1, $2, $3, $4, $5, $6),
      ($7, $8, $9, $10, $11, $12),
      ($13, $14, $15, $16, $17, $18),
      ($19, $20, $21, $22, $23, $24),
      ($25, $26, $27, $28, $29, $30),
      ($31, $32, $33, $34, $35, $36),
      ($37, $38, $39, $40, $41, $42),
      ($43, $44, $45, $46, $47, $48),
      ($49, $50, $51, $52, $53, $54),
      ($55, $56, $57, $58, $59, $60);
    `;

    const productValues = [
      'MacBook Pro 16"',
      "Apple MacBook Pro with M3 chip, 16GB RAM, 512GB SSD",
      2499.0,
      1,
      true,
      ["laptop", "apple", "premium"],
      "Dell XPS 13",
      "Dell XPS 13 ultrabook with Intel Core i7, 16GB RAM, 256GB SSD",
      1299.0,
      1,
      true,
      ["laptop", "dell", "ultrabook"],
      "iPad Air",
      "Apple iPad Air with M1 chip, 64GB storage, Wi-Fi",
      599.0,
      2,
      true,
      ["tablet", "apple", "portable"],
      "Samsung Galaxy Tab S8",
      "Samsung Galaxy Tab S8 with S Pen, 128GB storage",
      699.0,
      2,
      false,
      ["tablet", "samsung", "android"],
      "iPhone 15 Pro",
      "Apple iPhone 15 Pro with 256GB storage, Titanium Blue",
      1199.0,
      3,
      true,
      ["smartphone", "apple", "premium"],
      "Google Pixel 8",
      "Google Pixel 8 with AI features, 128GB storage",
      699.0,
      3,
      true,
      ["smartphone", "google", "android"],
      "Sony WH-1000XM5",
      "Sony wireless noise-canceling headphones",
      399.0,
      4,
      true,
      ["headphones", "sony", "wireless"],
      "AirPods Pro",
      "Apple AirPods Pro with spatial audio",
      249.0,
      4,
      true,
      ["earbuds", "apple", "wireless"],
      "Mechanical Keyboard",
      "RGB mechanical gaming keyboard with blue switches",
      129.0,
      5,
      true,
      ["keyboard", "gaming", "mechanical"],
      "Wireless Mouse",
      "Ergonomic wireless mouse with precision tracking",
      79.0,
      5,
      true,
      ["mouse", "wireless", "ergonomic"],
    ];

    const productResult = await client.query(productInsertQuery, productValues);
    console.log(`✅ Inserted ${productResult.rowCount} products`);

    // Insert sample orders
    const orderInsertQuery = `
      INSERT INTO orders (customer_id, total, status, created_at, updated_at) VALUES
      ($1, $2, $3, $4, $5),
      ($6, $7, $8, $9, $10),
      ($11, $12, $13, $14, $15),
      ($16, $17, $18, $19, $20),
      ($21, $22, $23, $24, $25),
      ($26, $27, $28, $29, $30),
      ($31, $32, $33, $34, $35),
      ($36, $37, $38, $39, $40);
    `;

    const orderValues = [
      1,
      2748.0,
      "delivered",
      new Date("2024-01-12"),
      new Date("2024-01-18"),
      2,
      1299.0,
      "shipped",
      new Date("2024-01-20"),
      new Date("2024-01-22"),
      3,
      1798.0,
      "processing",
      new Date("2024-02-05"),
      new Date("2024-02-05"),
      1,
      649.0,
      "delivered",
      new Date("2024-02-15"),
      new Date("2024-02-20"),
      4,
      208.0,
      "pending",
      new Date("2024-03-01"),
      new Date("2024-03-01"),
      5,
      699.0,
      "shipped",
      new Date("2024-03-05"),
      new Date("2024-03-07"),
      2,
      399.0,
      "delivered",
      new Date("2024-03-10"),
      new Date("2024-03-15"),
      3,
      79.0,
      "delivered",
      new Date("2024-03-12"),
      new Date("2024-03-14"),
    ];

    const orderResult = await client.query(orderInsertQuery, orderValues);
    console.log(`✅ Inserted ${orderResult.rowCount} orders`);

    // Insert sample order items
    const orderItemInsertQuery = `
      INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
      ($1, $2, $3, $4),
      ($5, $6, $7, $8),
      ($9, $10, $11, $12),
      ($13, $14, $15, $16),
      ($17, $18, $19, $20),
      ($21, $22, $23, $24),
      ($25, $26, $27, $28),
      ($29, $30, $31, $32),
      ($33, $34, $35, $36),
      ($37, $38, $39, $40),
      ($41, $42, $43, $44);
    `;

    const orderItemValues = [
      1,
      1,
      1,
      2499.0, // Order 1: MacBook Pro
      1,
      8,
      1,
      249.0, // Order 1: AirPods Pro
      2,
      2,
      1,
      1299.0, // Order 2: Dell XPS 13
      3,
      5,
      1,
      1199.0, // Order 3: iPhone 15 Pro
      3,
      3,
      1,
      599.0, // Order 3: iPad Air
      4,
      6,
      1,
      699.0, // Order 4: Google Pixel 8 (price reduced)
      5,
      9,
      1,
      129.0, // Order 5: Mechanical Keyboard
      5,
      10,
      1,
      79.0, // Order 5: Wireless Mouse
      6,
      6,
      1,
      699.0, // Order 6: Google Pixel 8
      7,
      7,
      1,
      399.0, // Order 7: Sony Headphones
      8,
      10,
      1,
      79.0, // Order 8: Wireless Mouse
    ];

    const orderItemResult = await client.query(
      orderItemInsertQuery,
      orderItemValues
    );
    console.log(`✅ Inserted ${orderItemResult.rowCount} order items`);

    // Display some statistics
    const userCount = await client.query("SELECT COUNT(*) as count FROM users");
    const postCount = await client.query("SELECT COUNT(*) as count FROM posts");
    const publishedPostCount = await client.query(
      "SELECT COUNT(*) as count FROM posts WHERE published = true"
    );
    const commentCount = await client.query(
      "SELECT COUNT(*) as count FROM comments"
    );
    const activeUserCount = await client.query(
      "SELECT COUNT(*) as count FROM users WHERE active = true"
    );

    // E-commerce statistics
    const customerCount = await client.query(
      "SELECT COUNT(*) as count FROM customers"
    );
    const productCount = await client.query(
      "SELECT COUNT(*) as count FROM products"
    );
    const inStockProductCount = await client.query(
      "SELECT COUNT(*) as count FROM products WHERE in_stock = true"
    );
    const orderCount = await client.query(
      "SELECT COUNT(*) as count FROM orders"
    );
    const orderItemCount = await client.query(
      "SELECT COUNT(*) as count FROM order_items"
    );

    console.log("\n📊 Database Statistics:");
    console.log("📝 Blog Schema:");
    console.log(`   Total users: ${userCount.rows[0].count}`);
    console.log(`   Active users: ${activeUserCount.rows[0].count}`);
    console.log(`   Total posts: ${postCount.rows[0].count}`);
    console.log(`   Published posts: ${publishedPostCount.rows[0].count}`);
    console.log(`   Total comments: ${commentCount.rows[0].count}`);

    console.log("🛒 E-commerce Schema:");
    console.log(`   Total customers: ${customerCount.rows[0].count}`);
    console.log(`   Total products: ${productCount.rows[0].count}`);
    console.log(`   In-stock products: ${inStockProductCount.rows[0].count}`);
    console.log(`   Total orders: ${orderCount.rows[0].count}`);
    console.log(`   Total order items: ${orderItemCount.rows[0].count}`);

    console.log("\n🎉 Database seeding completed successfully!");
  } catch (error) {
    console.error("❌ Error seeding database:", error);
    throw error;
  } finally {
    client.release();
  }
}

// Function to verify the seeded data
async function verifyData() {
  const client = await pool.connect();

  try {
    console.log("\n🔍 Verifying seeded data...");

    // Test queries that match our examples
    const johnUser = await client.query(
      "SELECT * FROM users WHERE email = 'john@example.com'"
    );
    console.log("✅ John Doe user exists:", johnUser.rows[0]?.name);

    const activeUsers = await client.query(`
      SELECT COUNT(*) as count 
      FROM users 
      WHERE active = true
    `);
    console.log(`✅ Found ${activeUsers.rows[0].count} active users`);

    const publishedPosts = await client.query(`
      SELECT p.id, p.title, p.tags
      FROM posts p 
      WHERE p.published = true
      ORDER BY p.created_at DESC
      LIMIT 3
    `);
    console.log(
      `✅ Found ${publishedPosts.rows.length} published posts (showing first 3):`
    );
    publishedPosts.rows.forEach((post) => {
      console.log(`   - ${post.title} (tags: ${post.tags.join(", ")})`);
    });

    const typescriptPosts = await client.query(`
      SELECT COUNT(*) as count 
      FROM posts 
      WHERE published = true 
      AND 'typescript' = ANY(tags)
    `);
    console.log(
      `✅ Found ${typescriptPosts.rows[0].count} published TypeScript posts`
    );

    const postsWithComments = await client.query(`
      SELECT p.id, p.title, COUNT(c.id) as comment_count
      FROM posts p
      LEFT JOIN comments c ON p.id = c.post_id
      WHERE p.published = true
      GROUP BY p.id, p.title
      HAVING COUNT(c.id) > 0
      ORDER BY comment_count DESC
    `);
    console.log(
      `✅ Found ${postsWithComments.rows.length} posts with comments:`
    );
    postsWithComments.rows.forEach((post) => {
      console.log(`   - ${post.title} (${post.comment_count} comments)`);
    });

    const recentUsers = await client.query(`
      SELECT COUNT(*) as count 
      FROM users 
      WHERE created_at > '2024-01-01'
    `);
    console.log(
      `✅ Found ${recentUsers.rows[0].count} users created after 2024-01-01`
    );

    // ==========================================
    // E-commerce Data Verification
    // ==========================================

    console.log("\n🛒 Verifying e-commerce data...");

    const availableProducts = await client.query(`
      SELECT COUNT(*) as count 
      FROM products 
      WHERE in_stock = true AND category_id IN (1, 2, 3)
    `);
    console.log(
      `✅ Found ${availableProducts.rows[0].count} available products in categories 1-3`
    );

    const expensiveProducts = await client.query(`
      SELECT p.name, p.price
      FROM products p 
      WHERE p.price > 100 AND p.price <= 1000 AND p.in_stock = true
      ORDER BY p.price DESC
      LIMIT 3
    `);
    console.log(
      `✅ Found ${expensiveProducts.rows.length} expensive products (showing top 3):`
    );
    expensiveProducts.rows.forEach((product) => {
      console.log(`   - ${product.name}: $${product.price}`);
    });

    const recentOrders = await client.query(`
      SELECT COUNT(*) as count 
      FROM orders 
      WHERE status IN ('pending', 'processing') 
      AND created_at > '2024-01-01'
    `);
    console.log(
      `✅ Found ${recentOrders.rows[0].count} recent pending/processing orders`
    );

    const customerOrderSummary = await client.query(`
      SELECT 
        c.first_name || ' ' || c.last_name as customer_name,
        COUNT(o.id) as order_count,
        SUM(o.total) as total_spent
      FROM customers c
      LEFT JOIN orders o ON c.id = o.customer_id
      GROUP BY c.id, c.first_name, c.last_name
      ORDER BY total_spent DESC NULLS LAST
      LIMIT 3
    `);
    console.log(`✅ Top customers by spending:`);
    customerOrderSummary.rows.forEach((customer) => {
      const spent = customer.total_spent || 0;
      console.log(
        `   - ${customer.customer_name}: ${customer.order_count} orders, $${spent} spent`
      );
    });
  } catch (error) {
    console.error("❌ Error verifying data:", error);
  } finally {
    client.release();
  }
}

// Main execution
async function main() {
  try {
    await seedDatabase();
    await verifyData();
  } catch (error) {
    console.error("❌ Failed to seed database:", error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run the seeding script
if (require.main === module) {
  main().catch(console.error);
}

export { seedDatabase, verifyData };
