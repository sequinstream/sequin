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

-- Create additional tables for routing examples
CREATE TABLE IF NOT EXISTS user_events (
    id INT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id INT,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchase_events (
    id INT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id INT,
    product_id INT,
    amount DECIMAL(10,2),
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_events (
    id INT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    session_id VARCHAR(255),
    page_url VARCHAR(500),
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS other_events (
    id INT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Show the created table structures
DESCRIBE products;
DESCRIBE user_events;
DESCRIBE purchase_events;
