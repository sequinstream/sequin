<div align="center">

# Sequin

### Send Postgres changes anywhere
</div>

> [!IMPORTANT]
> _08/30/2024_: We've almost completed a major upgrade to Sequin. `main` should be stable next week. In the meantime:
> * You can [Try Sequin cloud](https://console.sequinstream.com/register) now.
> * Join the [release list](https://sequinstream.com/alpha) to get notified when we hit alpha.

## What is Sequin?

Sequin sends Postgres changes to your applications and services. It’s designed to never miss an insert, update, or delete and provide exactly-once processing of all changes.

You can receive changes via HTTP push (webhooks) or pull (think SQS).

## Killer features

* **Never miss a message:** Sequin delivers messages from your database to consumers until they are acknowledged (i.e. exactly-once processing guarantees).
* **SQL-based routing:** Filter and route messages to consumers using SQL `where` conditions.
* **Replays:** Rewind consumers to any row on your table. Or republish select messages that match a SQL query.
* **Backfills:** Consumers can queue up records from this moment forward or for the entire table.
* **Simple HTTP API:** Messages are delivered via a simple HTTP push or pull interface for easy integration.
* **Bring your database:** Sequin is not an extension. It works with any Postgres database version 12+.
* **No plpgsql:** Define business logic in the language of your choice and in context.
* **Transforms** (coming soon!): Transform message payloads by writing functions in Lua, JavaScript, or Go.

[**Try Sequin Cloud now →**](https://console.sequinstream.com/register)

### How Sequin works

Sequin keeps your data in your Postgres database. You can use your existing database in a new way without copying the data to a new system or mastering a new technology.

Sequin connects to any Postgres database via the Write Ahead Log (WAL). It captures changes to your Postgres tables, turning them into a stream of messages. Messages are persisted in the stream to ensure they are delivered and allow for replays and rewinds.You can read those messages over an HTTP interface similar to SQS, or Sequin can push changes to you via webhooks.

Sequin is open source. You can run Sequin in its own Docker container or as a sidecar container in your existing deployment.

Sequin comes with a web console/UI for configuration:

1. Connect any Postgres database to Sequin (Sequin uses logical replication).
2. Select the tables, actions, and SQL filters that publish messages to your consumers.
3. Consume messages using HTTP pull and push interfaces. (You can use our SDKs.)

In production, you can configure Sequin as code using Terraform.

### Benchmarks

Sequin efficiently captures changes using the Write Ahead Log. This adds virtually no overhead to the performance of your database. If your database can handle the transaction, so can Sequin with minimal latency.

We'll be reporting benchmarks for different database configurations soon.

### Use cases

* **Trigger side-effects:** Never miss a change in your database for processing by other systems.
  ![Queue email](https://github.com/sequinstream/sequin/blob/main/docs/images/readme/use_case_1.svg)
* **Fan out:** Add queue capabilities to your database to broadcast events, distribute workloads, and decouple services.
  ![Fan out order](https://github.com/sequinstream/sequin/blob/main/docs/images/readme/use_case_2.svg)
* **Replication:** Sync data to other systems to update search indexes, invalidate caches, and generate denormalized views.
  ![Sync](https://github.com/sequinstream/sequin/blob/main/docs/images/readme/use_case_3.svg)

### Compare Sequin

| Feature             | Sequin                 | PG triggers   | LISTEN / NOTIFY | Supabase Webhooks | Amazon SQS   |
|---------------------|------------------------|---------------|-----------------|-------------------|--------------|
| Trigger guarantees  | Transactional          | Transactional | Transactional   | Transactional     | N/A          |
| Delivery guarantees | Exactly-once           | Exactly-once  | At-most-once    | At-least-once     | Exactly-once |
| Order guarantees    | FIFO, strict by PK     | FIFO          | ❌ No           | ❌ No             | FIFO option  |
| Replay              | ✅ Yes                 | ❌ No         | ❌ No           | ❌ No             | ❌ No        |
| Filtering           | ✅ Yes                 | ✅ Yes        | ✅ Yes          | ✅ PG triggers    | N/A          |
| Transformation      | ✅ LUA                 | ❌ No         | ❌ No           | ❌ No             | ❌ No        |
| Backfills           | ✅ Yes                 | ❌ No         | ❌ No           | ❌ No             | N/A          |
| Interface           | HTTP pull<br>HTTP push | plpgsql       | Postgres client | HTTP push         | HTTP pull    |
| Observability       | Sequin console         | PG logging*   | PG logging*     | Supabase dashboard | AWS console |
| Performance         | WAL, minimal overhead  | Serial ops / row^   | Minimal overhead | Serial ops / row^ | N/A |

<sub>* **PG logging:** You can configure logging in your database, but nothing is built in. Generally hard to see the state of any given side-effect.</sub>

<sub>^ **Serial ops / row:** Postgres triggers (which power Supabase Webhooks) run serially within each transaction (and block) and can add significant overhead.</sub>


## Documentation

For full documentation, visit [sequinstream.com/docs](http://sequinstream.com/docs).

## Quick start

Sequin is available as a cloud version or self-hosted. See the following docs to get up and running quickly:

* [Sequin cloud quickstart](https://sequinstream.com/docs/quickstart)
* [Sequin self-hosted quickstart](https://sequinstream.com/docs/self-hosting/installation)

## SDKs

Use Sequin's SDKs to dynamically create new consumers and work with messages:

* [JavaScript](https://github.com/sequinstream/sequin-js)
* [Python](https://github.com/sequinstream/sequin-py)
* [Go](https://github.com/sequinstream/sequin-go)
* [Elixir](https://github.com/sequinstream/sequin-elixir) (and [Elixir Broadway](https://github.com/sequinstream/off\_broadway\_sequin))
* [Ruby](https://github.com/sequinstream/sequin-ruby)

## Contribute

Sequin is open-sourced with an MIT license. The project is just getting started, so the best way to contribute right now is to open an issue or send us a note: [support@sequinstream.com](mailto:support@sequinstream.com)
