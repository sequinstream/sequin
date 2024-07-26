import Database from "better-sqlite3";
import fs from "fs";
import path from "path";

const DB_FILE = "ecommerce.db";
let db;

// Initialize database
function initializeDatabase() {
  const dbExists = fs.existsSync(path.join(process.cwd(), DB_FILE));

  db = new Database(DB_FILE);

  if (!dbExists) {
    console.log("Creating new database...");
    db.exec(`
      CREATE TABLE IF NOT EXISTS product_inventory (
        id INTEGER PRIMARY KEY,
        product_name TEXT NOT NULL,
        price REAL NOT NULL,
        quantity_available INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        product_payload TEXT NOT NULL,
        shipped BOOLEAN DEFAULT FALSE,
        paid BOOLEAN DEFAULT FALSE
      );
    `);

    // Insert fake products
    const insert = db.prepare(
      "INSERT INTO product_inventory (product_name, price, quantity_available) VALUES (?, ?, ?)"
    );
    const products = [
      ["Bike", 299.99, 50],
      ["Car", 19999.99, 10],
      ["Laptop", 999.99, 100],
      ["Smartphone", 699.99, 200],
      ["Headphones", 149.99, 300],
      ["Tablet", 399.99, 75],
      ["Smartwatch", 249.99, 150],
      ["Camera", 599.99, 50],
      ["TV", 799.99, 30],
      ["Gaming Console", 499.99, 60],
    ];
    products.forEach((product) => insert.run(product));
    console.log("Database initialized with sample products.");
  } else {
    console.log("Using existing database.");
  }
}

export { db, initializeDatabase };
