-- Create the products table for testing Sequin MySQL sink
CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    description TEXT,
    category VARCHAR(100),
    in_stock BOOLEAN DEFAULT TRUE,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create an index on commonly queried fields
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_in_stock ON products(in_stock);

-- Insert some sample data for testing
INSERT INTO products (id, name, price, description, category, in_stock) VALUES
(1, 'Sample Product 1', 19.99, 'This is a sample product for testing', 'electronics', TRUE),
(2, 'Sample Product 2', 29.99, 'Another sample product', 'books', TRUE),
(3, 'Sample Product 3', 39.99, 'Third sample product', 'clothing', FALSE);

-- Show the created table structure
DESCRIBE products; 