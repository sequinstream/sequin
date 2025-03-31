<div align="center">

# Sequin

### Postgres change data capture to streams and queues like Kafka, SQS, HTTP endpoints, and more

[![Docs](https://img.shields.io/badge/docs-sequinstream.com%2Fdocs-blue)](https://sequinstream.com/docs) [![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](./LICENSE)

<p align="center">
  <a href="https://sequinstream.com/docs">Documentation</a>
  ·
  <a href="https://sequinstream.com">Website</a>
  ·
  <a href="https://discord.gg/BV8wFXvNtY">Discord</a>
  ·
  <a href="https://console.sequinstream.com">Sequin Cloud</a>
</p>

</div>

## What is Sequin?

Sequin is the [fastest](#performance) change data capture (CDC) platform for Postgres. Sequin makes it easy to stream Postgres to streaming platforms, queues, and more (e.g. Kafka, GCP Pub/Sub, another Postgres, etc.). You can backfill existing rows and stream new changes in real-time.

Sequin uses a logical replication slot to detect and stream changes in real-time.

Sequin is a standalone Docker container that you can deploy next to your Postgres database. Alternatively, you can use [our hosted offering](https://console.sequinstream.com) or we'll [manage Sequin](https://sequinstream.com/docs/sequin-managed/overview) for you in your cloud.

Sequin is open source/[MIT](./LICENSE). To help us make this project great, tell us what you're building in our [Discord Server](https://discord.gg/BV8wFXvNtY).

## Getting started

See our [quickstart](https://sequinstream.com/docs/quickstart/webhooks) to get up and running in your dev environment in about 5 minutes.

Alternatively, you can try Sequin for free on [Sequin Cloud](https://console.sequinstream.com/register). Follow the instructions in the app to start streaming your data in a couple minutes.

## Performance

Sequin delivers industry-leading performance for change data capture (CDC), sustaining **40k operations per second** (40 MB/s) with **55ms average latency**.

| Tool | Latency at 40k ops/s |
|------|-------------------|
| **Sequin** | **55ms** |
| AWS MSK Debezium | 258ms |
| Fivetran | 5 minutes |
| Airbyte | 1+ hours |

In our production benchmarks with AWS RDS Postgres and MSK Kafka, Sequin maintains consistent performance under sustained load:

- Average throughput: 40,000 ops/sec
- Average bandwidth: 40 MB/sec
- Average latency: 55ms
- 99th percentile latency: 253ms

[Read more](https://sequinstream.com/docs/performance) about Sequin's current performance in our docs.

## Sinks

| Sink | Support | Description |
|-------------|---------|-------------|
| Azure EventHubs | ✅ Real-time streaming<br />✅ Backfill existing rows | Publish messages to Azure EventHubs |
| GCP Pub/Sub | ✅ Real-time streaming<br />✅ Backfill existing rows | Publish messages to Google Cloud Pub/Sub topics |
| HTTP Pull (Native) | ✅ Real-time streaming<br />✅ Backfill existing rows | Consume changes directly from Sequin with exactly-once processing |
| Kafka | ✅ Real-time streaming<br />✅ Backfill existing rows | Stream changes to Apache Kafka topics |
| NATS | ✅ Real-time streaming<br />✅ Backfill existing rows | Stream changes to NATS subjects |
| Postgres | 🛠️ Coming April 2025 | Replicate changes to another Postgres |
| RabbitMQ | ✅ Real-time streaming<br />✅ Backfill existing rows | Publish messages to RabbitMQ exchanges |
| Redis | ✅ Real-time streaming<br />✅ Backfill existing rows | `XADD` to Redis Streams |
| SQS | ✅ Real-time streaming<br />✅ Backfill existing rows | Send messages to Amazon SQS queues |
| Webhook Subscription (Native) | ✅ Real-time streaming<br />✅ Backfill existing rows | Send changes to any HTTP endpoint |

## Change data capture use cases

Sequin works great for change data capture use cases like:

- **[Triggering a workflow when data changes in Postgres](https://sequinstream.com/docs/how-to/trigger-automated-workflows):** Execute custom business logic whenever specific rows are inserted, updated, or deleted in your database.
- **Making events available to downstream services:** Stream changes from your database tables as events that other services can consume.
- **Informing downstream services when rows change:** Notify dependent services about data changes to keep systems in sync.
- **[Audit logging](https://sequinstream.com/docs/how-to/create-audit-logs):** Track and record all changes made to data in your database for compliance or feature development.
- **[Sync a table from one database to another](https://sequinstream.com/docs/how-to/replicate-tables):** Keep tables synchronized across different database instances in real-time.
- **[Materializing another table in your database](https://sequinstream.com/docs/how-to/replicate-tables):** Create and maintain derived tables based on changes to source tables.
- **[Maintaining a cache](https://sequinstream.com/docs/how-to/maintain-caches):** Keep caches up-to-date by streaming database changes.
- **Refreshing search indexes:** Keep search indexes fresh by streaming updates from your database.

## Killer features

- **Never miss a change:** Sequin ensures all database changes are delivered to sinks.
- **The fastest CDC:** Sequin delivers industry-leading performance for change data capture (CDC), sustaining [40k operations per second with 55ms average latency](https://sequinstream.com/docs/performance).
- **Bring your database:** Sequin is not an extension. It works with any Postgres database version 14\+.
- **SQL-based routing:** Filter and route messages to sinks using SQL `where` conditions.
- **Backfills:** Backfill existing rows from your tables to sinks at any time.
- **Transforms** \(coming soon\!\): Transform message payloads by writing functions in Lua, JavaScript, or Go.
- **Infrastructure as code:** Manage Sequin as code using a YAML file paired with the Sequin CLI. Or use our management API.
- **Rich web console:** Your team can configure and monitor sinks from a full feature web console.
- **Observability:** Built in monitoring and metrics.

## How Sequin works

Sequin connects to any Postgres database. Specify the tables you want to stream, as well as optional filters and transformations. Route changes to sinks like Kafka, Google Pub/Sub, Redis, etc.

When you setup a sink, you can opt to backfill data from the source table to the sink.

After setup, Sequin will stream new changes to the sink as they occur in real-time. If there are any issues with delivery, Sequin will automatically retry delivery with exponential backoff.

Sequin comes with a web console/UI for configuration and monitoring. You can also configure Sequin as code using YAML config files and our management API.

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

## Development

### Prerequisites

- Elixir 1.18+
- PostgreSQL 14+
- GitHub CLI (`gh`)
- Node.js (for frontend assets)
- Go (only necessary for CLI development)

### Getting started

1. Fork and clone the repository
2. Install dependencies:
   ```bash
   # Install Elixir dependencies
   mix deps.get

   # Install frontend dependencies
   cd assets && npm install
   ```
3. Start PostgreSQL and Redis services:
   ```bash
   docker-compose up -d
   ```
4. Start the development server from the top-level directory:
   ```bash
   make dev
   ```

The app will be available at `http://localhost:4000`.

Sequin uses LiveView + LiveSvelte for its frontend. As a monolith, the entire app is available at `http://localhost:4000`.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed instructions on how to contribute to Sequin.