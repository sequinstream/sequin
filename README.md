<div align="center">

# Sequin

### Postgres change data capture to streams and queues like Kafka, SQS, HTTP endpoints, and more

[![Docs](https://img.shields.io/badge/docs-sequinstream.com%2Fdocs-blue)](https://sequinstream.com/docs) [![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://opensource.org/licenses/MIT)

<p align="center">
  <a href="https://console.sequinstream.com">Sequin Cloud</a>
  ·
  <a href="https://sequinstream.com/docs">Documentation</a>
  ·
  <a href="https://sequinstream.com">Website</a>
  ·
  <a href="https://discord.gg/BV8wFXvNtY">Discord</a>
</p>

</div>

## What is Sequin?

Sequin is a tool for change data capture (CDC) in Postgres. Sequin makes it easy to stream Postgres rows and changes to streaming platforms and queues (e.g. Kafka and SQS). You can backfill existing rows and stream new changes in real-time.

Sequin even supports native sinks (HTTP GET and webhooks), so you can get started without any other infrastructure.

Change data capture enables applications and services to track and respond to row-level changes in database tables as they occur. With CDC via Sequin, you can:

1. **Replicate data** from your existing tables to other apps, databases, caches, materialized views, or frontend clients.
2. **Build event driven workflows** such as triggering side effects when data in Postgres changes.

Sequin itself is [built on Postgres](https://sequinstream.com/docs/how-sequin-works). It uses a logical replication slot to detect changes and internal tables to store sink state.

Unlike Debezium, another change data capture tool, Sequin doesn't require Kafka to operate. Sequin is a standalone Docker container that you can deploy next to your Postgres database. Or, you can use [our hosted offering](https://console.sequinstream.com).

Sequin is open source/MIT. To help us make this project great, tell us what you're building in our [Discord Server](https://discord.gg/BV8wFXvNtY).

## Sinks

| Sink | Support | Description |
|-------------|---------|-------------|
| Kafka | ✅ Real-time streaming<br />✅ Backfill existing rows | Stream changes to Apache Kafka topics |
| SQS | ✅ Real-time streaming<br />✅ Backfill existing rows | Send messages to Amazon SQS queues |
| Redis | ✅ Real-time streaming<br />✅ Backfill existing rows | `XADD` to Redis Streams |
| Webhook Subscription (Native) | ✅ Real-time streaming<br />✅ Backfill existing rows | Send changes to any HTTP endpoint |
| HTTP Pull (Native) | ✅ Real-time streaming<br />✅ Backfill existing rows | Consume changes directly from Sequin with exactly-once processing |
| GCP Pub/Sub | ✅ Real-time streaming<br />✅ Backfill existing rows | Publish messages to Google Cloud Pub/Sub topics |
| NATS | ✅ Real-time streaming<br />✅ Backfill existing rows | Stream changes to NATS subjects |
| RabbitMQ | ✅ Real-time streaming<br />✅ Backfill existing rows | Publish messages to RabbitMQ exchanges |
| Azure EventHubs | Coming soon | (Late Dec 2024) |
| Amazon SNS | Coming soon | (Jan 2025) |
| AWS Kinesis | Coming soon | (Jan 2025) |

## Killer features

- **Never miss a change:** Sequin ensures all database changes are delivered to sinks.
- **SQL-based routing:** Filter and route messages to sinks using SQL `where` conditions.
- **Backfills:** Backfill existing rows from your tables to sinks.
- **Bring your database:** Sequin is not an extension. It works with any Postgres database version 12\+.
- **Transforms** \(coming soon\!\): Transform message payloads by writing functions in Lua, JavaScript, or Go.

## Change data capture use cases

Sequin works great for change data capture use cases like:

- **Triggering a workflow when data changes in Postgres:** Execute custom business logic whenever specific rows are inserted, updated, or deleted in your database.
- **Making events available to downstream services:** Stream changes from your database tables as events that other services can consume.
- **Informing downstream services when rows change:** Notify dependent services about data changes to keep systems in sync.
- **Audit logging:** Track and record all changes made to data in your database for compliance or feature development.
- **Sync a table from one database to another:** Keep tables synchronized across different database instances in real-time.
- **Materializing another table in your database:** Create and maintain derived tables based on changes to source tables.
- **Maintaining a cache:** Keep caches up-to-date by streaming database changes.
- **Refreshing search indexes:** Keep search indexes fresh by streaming updates from your database.

## Getting started

See our [quickstart](https://sequinstream.com/docs/quickstart/webhooks).

Alternatively, you can try Sequin for free on [Sequin Cloud](https://console.sequinstream.com/register). Follow the instructions in the app to start streaming your data in a couple minutes.

## How Sequin works

Sequin connects to any Postgres database. Specify the tables you want to stream, as well as optional filters and transformations. Route changes to sinks like Kafka, SQS, Redis, or HTTP endpoints.

When you setup a sink, you can opt to backfill data from the source table to the sink.

After setup, Sequin will stream new changes to the sink as they occur in real-time. If there are any issues with delivery, Sequin will automatically retry delivery with exponential backoff.

Sequin comes with a web console/UI for configuration.

You can also configure Sequin as code using YAML config files.

## Benchmarks

Sequin delivers industry-leading performance for change data capture (CDC), consistently achieving sub-200ms latency at 2,000 sustained operations per second. Here's how Sequin compares to other solutions:

| Tool | Latency at 2k ops/s |
|------|-------------------|
| **Sequin** | **192ms** |
| Debezium | 234ms |
| Fivetran | 5 minutes |
| Airbyte | 1+ hours |

In our production benchmarks with AWS RDS Postgres and MSK Kafka, Sequin maintains consistent performance under sustained load:

- Average throughput: 2,054.4 ops/sec
- Average latency: 192ms
- 99th percentile latency: 245ms

[Learn more](https://sequinstream.com/docs/performance) about Sequin's current performance in our docs.

## How Sequin compares

<details open>

<summary>Sequin vs Debezium</summary>

### Sequin vs Debezium

Debezium is also a change data capture tool that captures changes from Postgres and streams them to messaging systems like Kafka. Debezium requires significant infrastructure (Kafka, Connect) to operate.

Sequin provides the same change data capture capabilities but with a much simpler setup:

1. **No Kafka required**: Sequin doesn't require Kafka.
2. **Native destinations**: With Debezium, you route to destinations via Kafka Connect. With Sequin, you stream directly to your destination of choice. This means simpler setup, fewer transforms, and fewer moving parts.
3. **Full-featured web console**: Sequin includes a web console for configuration and monitoring.

</details>

<details>

<summary>Sequin vs Fivetran/Airbyte</summary>

### Sequin vs Fivetran/Airbyte

Fivetran and Airbyte are ETL tools designed primarily for data warehouse ingestion. They excel at moving data in batch intervals (minutes to hours) to analytical databases.

Sequin is purpose-built for real-time operational use cases. Key differences:

1. **Real-time streaming**: Versus batch intervals.
2. **Focus on operational destinations**: Queues, streams, webhooks, etc. vs data warehouses.
3. **Flexible backfills**: Deployed a bug and need to replay the last 2 hours of data? Sequin makes it easy.

</details>

<details>

<summary>Sequin vs custom change data capture solutions</summary>

### Sequin vs custom change data capture solutions

Many teams build custom change data capture solutions using triggers, LISTEN/NOTIFY, or logical replication. While these can work, they often:

- Lack exactly-once processing guarantees
- Don't scale
- Don't handle backfills well
- Need ongoing maintenance
- Become a burden when the engineer who built it leaves the team

Sequin provides all this functionality out of the box:
- Exactly-once processing
- Seamless backfills
- Automatic retries and error handling
- Simple monitoring and observability
- Zero maintenance of change data capture infrastructure

</details>

## Contribute

Sequin is open source (MIT license). The project is just getting started, so the best way to contribute right now is to open an issue or join the [Discord Server](https://discord.gg/BV8wFXvNtY) and let us know what you want to build.
