-- ==================================================
-- Sequin MySQL Sink Example - Database Initialization
-- ==================================================
-- This script creates the necessary tables and sample data
-- for demonstrating Sequin's MySQL sink capabilities.

USE sequin_test;

-- ==================================================
-- PRODUCTS TABLE
-- ==================================================
-- Main products table demonstrating various MySQL data types
-- and how they map from Postgres sources
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    description TEXT,
    category VARCHAR(100),
    in_stock BOOLEAN DEFAULT TRUE,
    metadata JSON,
    tags VARCHAR(500),  -- Comma-separated tags for simple arrays
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Add some constraints for realistic data modeling
    CONSTRAINT chk_price_positive CHECK (price >= 0),
    INDEX idx_products_category (category),
    INDEX idx_products_in_stock (in_stock),
    INDEX idx_products_created_at (created_at)
);

-- Insert realistic sample products
INSERT INTO products (id, name, price, description, category, in_stock, metadata, tags) VALUES
(1, 'Wireless Bluetooth Headphones', 89.99, 'High-quality wireless headphones with noise cancellation', 'electronics', TRUE, '{"brand": "TechCorp", "warranty": "2 years", "features": ["wireless", "noise-cancellation", "long-battery"]}', 'audio,wireless,bluetooth'),
(2, 'Programming in Elixir', 45.50, 'Comprehensive guide to functional programming with Elixir', 'books', TRUE, '{"author": "Jane Developer", "pages": 420, "edition": "2nd", "language": "english"}', 'programming,elixir,functional'),
(3, 'Organic Cotton T-Shirt', 29.99, 'Comfortable organic cotton t-shirt in various colors', 'clothing', FALSE, '{"material": "organic cotton", "sizes": ["S", "M", "L", "XL"], "colors": ["white", "black", "gray"]}', 'organic,cotton,casual'),
(4, 'Smart Home Hub', 199.99, 'Central control unit for smart home devices', 'electronics', TRUE, '{"connectivity": ["wifi", "bluetooth", "zigbee"], "compatible_devices": 500, "voice_assistant": true}', 'smart-home,iot,automation');

-- ==================================================
-- EVENT TABLES FOR DYNAMIC ROUTING EXAMPLES
-- ==================================================

-- User Events Table - for user activity tracking
CREATE TABLE IF NOT EXISTS user_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id INT,
    session_id VARCHAR(255),
    data JSON,
    ip_address VARCHAR(45),  -- Support both IPv4 and IPv6
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_user_events_type (event_type),
    INDEX idx_user_events_user_id (user_id),
    INDEX idx_user_events_created_at (created_at)
);

-- Purchase Events Table - for e-commerce transactions
CREATE TABLE IF NOT EXISTS purchase_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id INT,
    product_id INT,
    quantity INT DEFAULT 1,
    amount DECIMAL(10,2),
    currency VARCHAR(3) DEFAULT 'USD',
    payment_method VARCHAR(50),
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_amount_positive CHECK (amount >= 0),
    INDEX idx_purchase_events_user_id (user_id),
    INDEX idx_purchase_events_product_id (product_id),
    INDEX idx_purchase_events_created_at (created_at)
);

-- Analytics Events Table - for web analytics and tracking
CREATE TABLE IF NOT EXISTS analytics_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    session_id VARCHAR(255),
    page_url VARCHAR(1000),
    referrer_url VARCHAR(1000),
    device_type VARCHAR(50),
    browser VARCHAR(100),
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_analytics_events_type (event_type),
    INDEX idx_analytics_events_session_id (session_id),
    INDEX idx_analytics_events_created_at (created_at)
);

-- Other Events Table - catch-all for unmatched event types
CREATE TABLE IF NOT EXISTS other_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    source_system VARCHAR(100),
    data JSON,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_other_events_type (event_type),
    INDEX idx_other_events_processed (processed),
    INDEX idx_other_events_created_at (created_at)
);

-- ==================================================
-- INSERT SAMPLE EVENT DATA
-- ==================================================

-- Sample user events
INSERT INTO user_events (event_type, user_id, session_id, data, ip_address) VALUES
('user_signup', 101, 'sess_abc123', '{"signup_method": "email", "referrer": "google"}', '192.168.1.100'),
('user_login', 102, 'sess_def456', '{"login_method": "email", "remember_me": true}', '192.168.1.101'),
('profile_update', 101, 'sess_abc123', '{"fields_updated": ["email", "name"], "source": "settings_page"}', '192.168.1.100');

-- Sample purchase events  
INSERT INTO purchase_events (event_type, user_id, product_id, quantity, amount, payment_method, data) VALUES
('purchase', 102, 1, 1, 89.99, 'credit_card', '{"promotion_code": "SAVE10", "shipping_method": "express"}'),
('purchase', 101, 2, 2, 91.00, 'paypal', '{"gift_message": "Happy Birthday!", "shipping_method": "standard"}');

-- Sample analytics events
INSERT INTO analytics_events (event_type, session_id, page_url, device_type, browser, data) VALUES
('page_view', 'sess_abc123', '/products/headphones', 'desktop', 'Chrome', '{"time_on_page": 45, "scroll_depth": 80}'),
('click', 'sess_def456', '/products/books', 'mobile', 'Safari', '{"element": "add_to_cart", "product_id": 2}');

-- ==================================================
-- SHOW TABLE STRUCTURES
-- ==================================================
-- Display the created table structures for reference

SHOW TABLES;

DESCRIBE products;
DESCRIBE user_events;
DESCRIBE purchase_events;
DESCRIBE analytics_events;
DESCRIBE other_events;

-- ==================================================
-- SUMMARY INFORMATION
-- ==================================================
SELECT 'Database setup complete!' as status;
SELECT 'Tables created:' as info, COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'sequin_test';

-- Show sample data counts
SELECT 'Sample data inserted:' as info;
SELECT 'products' as table_name, COUNT(*) as row_count FROM products
UNION ALL
SELECT 'user_events', COUNT(*) FROM user_events
UNION ALL  
SELECT 'purchase_events', COUNT(*) FROM purchase_events
UNION ALL
SELECT 'analytics_events', COUNT(*) FROM analytics_events
UNION ALL
SELECT 'other_events', COUNT(*) FROM other_events;
