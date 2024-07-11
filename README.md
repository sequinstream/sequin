# Sequin

## What is Sequin?

Sequin is an open-source message streaming system built on top of PostgreSQL. It bridges the gap between simple message queues and complex streaming platforms, offering a powerful yet familiar solution for developers who need reliable, asynchronous processing with the robustness of a database they already know and trust.

Sequin consists of a **stateless Docker image** that can connect to any Postgres database. And a **CLI** for managing streams, consumers, and sources.

### Key Features:

- **Postgres-powered streaming**: Harness the robustness of PostgreSQL for high-performance message streaming.
- **Seamless table integration**: Transform existing Postgres tables into message streams effortlessly.
- **Hierarchical subject routing**: Utilize dot-separated subjects for precise message filtering and routing.
- **At-least-once delivery**: Ensure message reliability with durable consumers and automatic redelivery.
- **HTTP interface**: Access streams via a RESTful API.
- **Pull and push consumers**: Support both polling and webhook-based message consumption.
- **Record and event streaming**: Optimized for both database record changes and event-driven architectures.
- **Replay capability**: Easily replay messages from any point in a stream's history.
- **Observability and management**: Monitor and control your streams using familiar PostgreSQL tools and Sequin's management API and CLI.

## Core concepts

### Streams

**Streams** are the fundamental building blocks in Sequin. A stream is an ordered list of messages. Each stream is backed by a PostgreSQL table.

You can create a stream by running the following command:

```
sequin stream add <stream-name>
```

And publish to the stream by running:

```
sequin stream pub <stream-name> <subject> <message>
```

### Subjects

Subjects in Sequin provide a hierarchical naming system for organizing and routing messages within streams. They use dot-separated strings. This allows for precise message filtering and routing.

For example:

- `orders.new`
- `users.profile.updated`
- `sensors.temperature.us-west`

Subjects support wildcards for flexible message consumption:

- `*` matches a single token (e.g., `orders.*` matches `orders.new` and `orders.cancelled`)
- `>` matches one or more tokens (e.g., `sensors.>` matches all sensor-related subjects)

### Consumers

Consumers in Sequin are stateful views of a stream that allow clients to process messages. They keep track of which messages have been delivered and acknowledged, ensuring at-least-once delivery semantics.

Sequin supports two types of consumers:

1. Pull Consumers: Clients actively request batches of messages on demand. This is ideal for applications that need fine-grained control over message processing rates.

2. Push Consumers: Messages are automatically delivered to a specified endpoint (e.g., a webhook URL). This is suitable for real-time processing scenarios.

Key features of consumers include:

- Durable subscriptions: Consumers maintain their position in the stream, even if disconnected.
- Filtered consumption: Consumers can subscribe to specific subjects within a stream.
- Acknowledgement tracking: Ensures messages are processed at least once.

### Sources (PostgreSQL Replication)

Sources in Sequin allow you to transform existing PostgreSQL tables into streams.

## Why Sequin?

> CLAUDE TODO

In the landscape of message streaming solutions, Sequin occupies a unique position:

- **Simplicity of SQS, Power of Kafka**: Sequin offers an intuitive interface like SQS, but with the robustness and scalability approaching that of Kafka.
- **Familiar Technology**: Built on PostgreSQL, Sequin leverages a database that many teams already know how to operate and scale.
- **Low Operational Burden**: By using PostgreSQL as the underlying storage, Sequin minimizes the need for additional infrastructure or specialized knowledge.
- **Feature-Rich**: Sequin's tight integration with PostgreSQL allows for rapid feature development, especially in areas of observability and control.

Whether you're looking to implement event-sourcing, build a robust pub/sub system, or simply need a reliable queue for asynchronous processing, Sequin provides a powerful solution that grows with your needs.

## Getting Started

See the [getting started guide](./getting-started.md).

## Setup Postgres Replication

To prepare your Postgres database for replication with Sequin, follow these steps:

1. Create a replication slot:

   ```sql
   SELECT pg_create_logical_replication_slot('your_slot_name', 'pgoutput');
   ```

2. Create a publication for the tables you want to replicate:

   ```sql
   CREATE PUBLICATION your_publication_name FOR TABLE schema.table1, schema.table2;
   ```

   You can add more tables by separating them with commas. To publish all tables in a schema:

   ```sql
   CREATE PUBLICATION your_publication_name FOR ALL TABLES IN SCHEMA your_schema;
   ```

3. Set the replica identity for each table. There are two main options:

   a. Default (only primary key is replicated for updates/deletes):

   ```sql
   ALTER TABLE your_schema.your_table REPLICA IDENTITY DEFAULT;
   ```

   b. Full (entire old row is replicated for updates/deletes):

   ```sql
   ALTER TABLE your_schema.your_table REPLICA IDENTITY FULL;
   ```

   The "full" option provides more detailed change information but may increase replication
   overhead.

Remember to adjust your Postgres configuration to allow replication connections. Also, ensure that the `wal_level` parameter in your PostgreSQL configuration is set to 'logical'. This setting enables logical decoding, which is necessary for logical replication.
