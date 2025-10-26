/**
 * Test schema interface for consistent testing across all test files
 */
export interface TestSchema {
  users: {
    id: number;
    name: string;
    email: string;
    age: number;
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
  products: {
    id: number;
    name: string;
    price: number;
    category_id: number;
    in_stock: boolean;
  };
}

/**
 * Mock data factory for testing
 */
export const createMockUser = (
  overrides: Partial<TestSchema["users"]> = {}
): TestSchema["users"] => ({
  id: 1,
  name: "John Doe",
  email: "john@example.com",
  age: 30,
  active: true,
  created_at: new Date("2024-01-01T10:00:00Z"),
  ...overrides,
});

export const createMockPost = (
  overrides: Partial<TestSchema["posts"]> = {}
): TestSchema["posts"] => ({
  id: 1,
  user_id: 1,
  title: "Test Post",
  content: "Test content",
  published: true,
  tags: ["test"],
  created_at: new Date("2024-01-01T10:00:00Z"),
  ...overrides,
});

export const createMockComment = (
  overrides: Partial<TestSchema["comments"]> = {}
): TestSchema["comments"] => ({
  id: 1,
  post_id: 1,
  user_id: 1,
  content: "Test comment",
  created_at: new Date("2024-01-01T10:00:00Z"),
  ...overrides,
});

export const createMockProduct = (
  overrides: Partial<TestSchema["products"]> = {}
): TestSchema["products"] => ({
  id: 1,
  name: "Test Product",
  price: 99.99,
  category_id: 1,
  in_stock: true,
  ...overrides,
});
