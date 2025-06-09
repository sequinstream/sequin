CREATE TABLE IF NOT EXISTS demo_table (
    id SERIAL PRIMARY KEY,
    demo_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
