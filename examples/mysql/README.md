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
  table_name: products  # Used when routing_mode is static
  username: mysql_user
  password: mysql_password
  ssl: false
  batch_size: 100
  timeout_seconds: 30
  upsert_on_duplicate: true
  routing_mode: static  # Can be 'static' or 'dynamic'

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

## Dynamic Table Routing

You can use routing functions to dynamically choose which MySQL table to route data to based on the record content:

```yaml
name: multi-table-mysql-sink
source:
  database: my_postgres_db
  table: events
  actions: [insert, update, delete]
  
destination:
  type: mysql
  host: localhost
  port: 3306
  database: sequin_test
  table_name: default_events  # Fallback table name
  username: mysql_user
  password: mysql_password
  routing_mode: dynamic  # Enable dynamic routing

routing: |
  def route(action, record, changes, metadata) do
    # Route based on event type
    table_name = case record["event_type"] do
      "user_signup" -> "user_events"
      "purchase" -> "purchase_events"
      "analytics" -> "analytics_events"
      _ -> "other_events"
    end
    
    %{table_name: table_name}
  end

transform: |
  def transform(action, record, changes, metadata) do
    %{
      id: record["id"],
      event_type: record["event_type"],
      data: record["data"],
      created_at: record["created_at"]
    }
  end
```

## Features

- **Dynamic routing**: Route to different MySQL tables based on record content
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
- `table_name`: Target table name (used as fallback when routing_mode is dynamic)
- `username`: MySQL username
- `password`: MySQL password
- `ssl`: Enable SSL connection (default: false)
- `batch_size`: Number of records to process in each batch (default: 100)
- `timeout_seconds`: Connection timeout in seconds (default: 30)
- `upsert_on_duplicate`: Use upsert instead of insert-only (default: true)
- `routing_mode`: Set to "dynamic" to enable routing functions, "static" for fixed table (default: static) 