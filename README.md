<div align="center">

# Sequin

### Send Postgres changes anywhere

[![Docs](https://img.shields.io/badge/docs-sequinstream.com%2Fdocs-blue)](https://sequinstream.com/docs) [![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://opensource.org/licenses/MIT)

<p align="center">
  <a href="https://console.sequinstream.com">Sequin Cloud</a>
  ·
  <a href="https://sequinstream.com/docs">Documentation</a>
  ·
  <a href="https://sequinstream.com">Website</a>
</p>

</div>

## What is Sequin?

Sequin sends Postgres records and changes to your applications and services. It’s designed to never miss an insert, update, or delete and provide exactly-once processing of all changes.

You can receive changes via HTTP push (webhooks) or pull (think SQS).

Use Sequin to add [async triggers](#use-cases) to your existing Postgres tables to reliably trigger side effects. Or, Sequin can add streaming mechanics to Postgres to do the work of SQS / Kafka without the operational overhead.

## Killer features

- **Never miss a message:** Sequin delivers messages from your database to consumers until they are acknowledged (i.e. exactly-once processing guarantees).
- **SQL-based routing:** Filter and route messages to consumers using SQL `where` conditions.
- **Replays:** Rewind consumers to any row on your table. Or republish select messages that match a SQL query.
- **Backfills:** Consumers can queue up records from this moment forward or for the entire table.
- **Simple HTTP API:** Messages are delivered via a simple HTTP push or pull interface for easy integration.
- **Bring your database:** Sequin is not an extension. It works with any Postgres database version 12+.
- **No plpgsql:** Define business logic in the language of your choice and in your application.
- **Transforms** (coming soon!): Transform message payloads by writing functions in Lua, JavaScript, or Go.

## Cloud

[**Try Sequin Cloud now →**](https://console.sequinstream.com/register)

## How Sequin works

![Sequin architecture](./docs/images/readme/arch-diagram.png)

Sequin keeps your data in your Postgres database. You can use your existing database in a new way without copying the data to a new system or mastering a new technology.

Sequin connects to any Postgres database via the Write Ahead Log (WAL). It captures changes to your Postgres tables, turning them into a stream of messages. Sequin buffers messages to ensure they are delivered and allow for replays and rewinds. You can read those messages over an HTTP interface similar to SQS, or Sequin can push changes to you via webhooks.

Sequin is open source. You can run Sequin in its own Docker container or as a sidecar container in your existing deployment.

Sequin comes with a web console/UI for configuration:

1. Connect any Postgres database to Sequin (Sequin uses logical replication).
2. Select the tables, actions, and SQL filters that publish messages to your consumers.
3. Consume messages using HTTP pull and push interfaces. (You can use our SDKs.)

In production, you can configure Sequin as code using Terraform.

## Benchmarks

Sequin efficiently captures changes using the Write Ahead Log. This adds virtually no overhead to the performance of your database. If your database can handle the transaction, so can Sequin with minimal latency.

Postgres Performance is highly dependent on machine resources. But to give you an idea, a `db.m5.xlarge` RDS instance (4 vCPU, 16 GB RAM, $260/mo) can handle inserts at 5,000 messages/second, with bursts up to 10k messages/second.

## Use cases

**Trigger side-effects:** Never miss a change in your database for processing by other systems.
![Queue email](https://github.com/sequinstream/sequin/blob/main/docs/images/readme/use_case_1.svg?)

**Fan out:** Add queue capabilities to your database to broadcast events, distribute workloads, and decouple services.
![Fan out order](https://github.com/sequinstream/sequin/blob/main/docs/images/readme/use_case_2.svg?)

**Replication:** Sync data to other systems to update search indexes, invalidate caches, and generate denormalized views.
![Sync](https://github.com/sequinstream/sequin/blob/main/docs/images/readme/use_case_3.svg?)

## Compare Sequin

| Feature             | Sequin                   | [PG triggers](#pg-triggers)       | [LISTEN / NOTIFY](#listen--notify)  | [Supabase Webhooks](#supabase-webhooks)  | [Amazon SQS](#amazon-sqs)   | [PGMQ](#pgmq)  |
| ------------------- | ------------------------ | ----------------- | ---------------- | ------------------ | ------------ | -------------- |
| Trigger guarantees  | Transactional            | Transactional     | Transactional    | Transactional      | N/A          | Transactional  |
| Processing guarantees | Exactly-once             | Exactly-once      | At-most-once     | At-least-once      | Exactly-once | Exactly-once |
| Order guarantees    | FIFO, strict by PK       | FIFO              | ❌ No            | ❌ No              | FIFO option  | ❌ No          |
| Replay              | ✅ Yes                   | ❌ No             | ❌ No            | ❌ No              | ❌ No        | ✅ Yes         |
| Filtering           | ✅ Yes                   | ✅ Yes            | ✅ Yes           | ✅ PG triggers     | N/A          | ❌ No          |
| Transformation      | ✅ LUA                   | ❌ No             | ❌ No            | ❌ No              | ❌ No        | ❌ No          |
| Backfills           | ✅ Yes                   | ❌ No             | ❌ No            | ❌ No              | N/A          | N/A            |
| Interface           | HTTP pull<br />HTTP push | plpgsql           | Postgres client  | HTTP push          | HTTP pull    | SQL functions  |
| Observability       | Sequin console           | PG logging\*      | PG logging\*     | Supabase dashboard | AWS console  | PG logging\*   |
| Performance         | WAL, minimal overhead    | Serial ops / row^ | Minimal overhead | Serial ops / row^  | N/A          | PG table limits|

<sub>\* **PG logging:** You can configure logging in your database, but nothing is built in. Generally hard to see the state of any given side-effect.</sub>

<sub>^ **Serial ops / row:** Postgres triggers (which power Supabase Webhooks) run serially within each transaction (and block) and can add significant overhead.</sub>

<details>

<summary>PG Triggers v Sequin</summary>

### PG Triggers

[PG Triggers](https://www.postgresql.org/docs/current/sql-createtrigger.html) provide exactly-once processing guarantees within the context of your database. For instance, you can ensure that when a record is inserted in one table, it is appended to another.

Sequin extends this guarantee outside of your database with a simple HTTP interface. Notably, Sequin is much more efficient at processing changes - as the trigger is captured via the WAL while Postgres triggers execute per row, inside transactions.

</details>

<details>

<summary>LISTEN / NOTIFY v Sequin</summary>

### LISTEN / NOTIFY

[`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) delivers a message to any channel listening for changes. It's a simple pub/sub model with at-most once delivery guarantees. If a change happens and no channel is available to hear it - it's gone forever.

Sequin adds a HTTP interface and persistent, durable messaging to provide exactly-once processing guarantees to the NOTIFY implementation. Paired with filtering, transforms, and observability - Sequin is easier to use and monitor.
</details>

<details>

<summary>Supabase Webhooks v Sequin</summary>

### Supabase Webhooks

Supabase Webhooks use pg_net to allow you to trigger a HTTP POST or GET from a Postgres Trigger. If a webhook fails, the response is logged (for a period of time).

Sequin adds retries and message persistence to provide exactly-once processing guarantees. Paired with more queue configuration options, Sequin acts as a sort of outbox stream for Supabase.

</details>

<details>

<summary>Amazon SQS v Sequin</summary>

### Amazon SQS

Amazon SQS is a simple queue with an HTTP interface. It can be configured to provide a highly durable, redundant FIFO queue with exactly one processing.

Sequin provides this same functionality with a transactional enqueue guarantee. Sequin also provide an HTTP push interface to enable easy integration with other services.

</details>

<details>

<summary>PGMQ v Sequin</summary>

### PGMQ

PGMQ is a Postgres extension that provides a durable message queue with a SQL interface that mimics SQS methods. It offers exactly-once processing like SQS, but runs entirely in Postgres. You can add message persistence to PGMQ by archiving (instead of deleting) messages - giving you the ability to replay through the queue. However, PGMQ does not provide any order guarantees.

Like PGMQ, Sequin leverages Postgres for persistence and to provide transactional enqueueing. Importantly, Sequin provides both change data captures and durable stream replay and backfill from existing tables in your database. Sequin comes with an HTTP pull and push interface, filtering, transformation, built in observability, and strict ordering guarantees.
</details>

## Documentation

For full documentation, visit [sequinstream.com/docs](http://sequinstream.com/docs).

## Quick start

Sequin is available as a cloud version or self-hosted. See the following docs to get up and running quickly:

- [Sequin cloud quickstart](https://sequinstream.com/docs/quickstart)
- [Sequin self-hosted quickstart](https://sequinstream.com/docs/self-hosting/docker-compose)

## Contribute

Sequin is open-sourced with an MIT license. The project is just getting started, so the best way to contribute right now is to open an issue or send us a note: [support@sequinstream.com](mailto:support@sequinstream.com)
