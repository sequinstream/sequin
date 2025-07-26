# MySQL Sink Example

This example demonstrates how to set up Sequin to stream Postgres changes to a MySQL database.

## Prerequisites

- A running Postgres database with logical replication enabled
- A running MySQL database
- Sequin installed and configured

## Setup

1. **Create a MySQL table** to receive the data:

```sql
CREATE DATABASE sequin_test;
USE sequin_test;

CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

2. **Configure the MySQL Sink** in Sequin:

```yaml
name: products-to-mysql
source:
  database: my_postgres_db
  table: products
  actions: [insert, update, delete]
  
destination:
  type: mysql
  host: localhost
  port: 3306
  database: sequin_test
  table_name: products
  username: mysql_user
  password: mysql_password
  ssl: false
  batch_size: 100
  timeout_seconds: 30
  upsert_on_duplicate: true

transform: |
  def transform(action, record, changes, metadata) do
    # Map the Postgres record to MySQL-compatible format
    %{
      id: record["id"],
      name: record["name"],
      price: record["price"]
    }
  end
```

## Features

- **Upsert support**: Uses MySQL's `ON DUPLICATE KEY UPDATE` for handling updates
- **Batch processing**: Efficiently processes multiple records in batches
- **SSL support**: Can connect to MySQL over SSL
- **Type handling**: Automatically handles different data types and JSON encoding for complex values
- **Error handling**: Comprehensive error handling with detailed error messages

## Usage

Once configured, Sequin will:

1. Capture changes from your Postgres table
2. Transform the data using your transform function
3. Insert/update records in MySQL using batch operations
4. Handle deletes by removing records from MySQL

The sink supports both insert-only mode and upsert mode depending on your `upsert_on_duplicate` setting.

## Connection Options

- `host`: MySQL server hostname
- `port`: MySQL server port (default: 3306)
- `database`: Target database name
- `table_name`: Target table name
- `username`: MySQL username
- `password`: MySQL password
- `ssl`: Enable SSL connection (default: false)
- `batch_size`: Number of records to process in each batch (default: 100)
- `timeout_seconds`: Connection timeout in seconds (default: 30)
- `upsert_on_duplicate`: Use upsert instead of insert-only (default: true) 