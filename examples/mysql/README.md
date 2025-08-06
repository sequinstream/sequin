# MySQL Sink Example

This example demonstrates how to set up Sequin to stream Postgres changes to a MySQL database using Sequin's MySQL sink.

## Overview

This example includes:
- A complete Docker Compose setup for MySQL
- Sample database schema with realistic tables
- Multiple configuration examples (static and dynamic routing)
- Transform functions for data mapping

## Quick Start with Docker

1. **Start MySQL using Docker Compose:**

```bash
cd examples/mysql
docker-compose up -d
```

This will create a MySQL instance with:
- Database: `sequin_test`
- User: `sequin_user` / Password: `sequin_password`
- Root password: `rootpassword`
- Port: `3306` (mapped to host)

2. **The database will be automatically initialized** with sample tables from `init.sql`.

## Prerequisites

- A running Postgres database with logical replication enabled
- Docker and Docker Compose (for the provided setup)
- Sequin installed and configured

## Configuration Examples

### Basic Static Routing

Create a `sequin.yaml` configuration file:

```yaml
databases:
  - name: "source-db"
    hostname: "localhost"
    database: "my_postgres_db"
    username: "postgres"
    password: "postgres"
    slot:
      name: "sequin_slot"
      create_if_not_exists: true
    publication:
      name: "sequin_pub"
      create_if_not_exists: true

sinks:
  - name: "products-to-mysql"
    database: "source-db"
    source:
      include_tables:
        - "public.products"
    destination:
      type: "mysql"
      host: "localhost"
      port: 3306
      database: "sequin_test"
      table_name: "products"
      username: "sequin_user"
      password: "sequin_password"
      ssl: false
      batch_size: 100
      timeout_seconds: 30
      upsert_on_duplicate: true
      routing_mode: "static"
    transform: "product-transform"

functions:
  - name: "product-transform"
    type: "transform"
    code: |-
      def transform(_action, record, _changes, _metadata) do
        %{
          id: record["id"],
          name: record["name"],
          price: record["price"],
          description: record["description"],
          category: record["category"],
          in_stock: record["in_stock"],
          metadata: record["metadata"]
        }
      end
```

### Dynamic Routing Example

For routing to different tables based on record content:

```yaml
databases:
  - name: "source-db"
    hostname: "localhost"
    database: "my_postgres_db"
    username: "postgres"
    password: "postgres"
    slot:
      name: "sequin_slot"
      create_if_not_exists: true
    publication:
      name: "sequin_pub"
      create_if_not_exists: true

sinks:
  - name: "events-to-mysql"
    database: "source-db"
    source:
      include_tables:
        - "public.events"
    destination:
      type: "mysql"
      host: "localhost"
      port: 3306
      database: "sequin_test"
      table_name: "other_events"  # Fallback table
      username: "sequin_user"
      password: "sequin_password"
      routing_mode: "dynamic"
    routing: "event-router"
    transform: "event-transform"

functions:
  - name: "event-router"
    type: "routing"
    sink_type: "mysql"
    code: |-
      def route(_action, record, _changes, _metadata) do
        table_name = case record["event_type"] do
          "user_signup" -> "user_events"
          "user_login" -> "user_events"
          "purchase" -> "purchase_events"
          "page_view" -> "analytics_events"
          "click" -> "analytics_events"
          _ -> "other_events"
        end
        
        %{table_name: table_name}
      end

  - name: "event-transform"
    type: "transform"
    code: |-
      def transform(_action, record, _changes, _metadata) do
        %{
          id: record["id"],
          event_type: record["event_type"],
          data: record["data"],
          created_at: record["created_at"]
        }
      end
```

## Features Demonstrated

- **Static routing**: Direct table mapping for simple use cases
- **Dynamic routing**: Route to different MySQL tables based on record content
- **Upsert support**: Uses MySQL's `ON DUPLICATE KEY UPDATE` for handling updates
- **Batch processing**: Efficiently processes multiple records in batches
- **SSL support**: Can connect to MySQL over SSL (set `ssl: true`)
- **Type handling**: Automatically handles JSON and complex data types
- **Transform functions**: Map Postgres data to MySQL-compatible format

## Database Schema

The example creates several tables to demonstrate different scenarios:

- `products` - Basic product catalog with various data types
- `user_events` - User activity events
- `purchase_events` - E-commerce transaction events  
- `analytics_events` - Web analytics data
- `other_events` - Catch-all for unmatched event types

## Usage Flow

Once configured, Sequin will:

1. **Capture** changes from your Postgres tables via logical replication
2. **Transform** the data using your transform function
3. **Route** records to appropriate MySQL tables (dynamic mode)
4. **Batch** multiple records for efficient processing
5. **Upsert/Insert** data into MySQL using optimized SQL operations

## Configuration Options

For complete configuration reference, see the [MySQL sink documentation](/reference/sinks/mysql).

Key MySQL sink options:
- `host` - MySQL server hostname
- `port` - MySQL server port (default: 3306)
- `database` - Target MySQL database
- `table_name` - Target table (or fallback for dynamic routing)
- `username`/`password` - Authentication credentials
- `ssl` - Enable SSL/TLS connection (default: false)
- `batch_size` - Records per batch (default: 100, max: 10,000)
- `timeout_seconds` - Connection timeout (default: 30, max: 300)
- `upsert_on_duplicate` - Use MySQL upsert mode (default: true)
- `routing_mode` - `"static"` or `"dynamic"` (default: "static")

## Cleanup

To stop and remove the MySQL container:

```bash
docker-compose down -v  # -v removes volumes
```

## Next Steps

- Learn more about [MySQL sinks](/reference/sinks/mysql)
- Explore [routing functions](/reference/routing) for advanced table routing
- See [transform functions](/reference/transforms) for data mapping
- Check out the complete [sequin.yaml reference](/reference/sequin-yaml) 