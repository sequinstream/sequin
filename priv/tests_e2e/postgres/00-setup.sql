CREATE TABLE IF NOT EXISTS kafka_test_table (
    id SERIAL PRIMARY KEY,
    demo_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sqs_test_table (
    id SERIAL PRIMARY KEY,
    demo_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
