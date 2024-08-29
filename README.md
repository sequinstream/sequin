# Sequin

> [!IMPORTANT]
> _08/15/2024_: Sequin is undergoing major upgrades, and `main` will not be stable for a couple weeks. [See the `0.3.7` tag for the last stable release](https://github.com/sequinstream/sequin/tree/v0.3.7). Join the [release list](https://sequinstream.com/alpha) to get notified when we hit alpha.

## What is Sequin?

Sequin sends Postgres changes to your applications and services. It’s designed to never miss an insert, update, or delete and provide exactly-once processing of all changes.

You can receive changes via HTTP push (webhooks) or pull (think SQS).

### How Sequin works

Sequin keeps your data in your Postgres database. You can use your existing database in a new way without copying the data to a new system or mastering a new technology.

Sequin connects to your Postgres database (via the WAL). It captures changes to your Postgres tables, turning them into a stream of messages. You can read those messages over an HTTP interface similar to SQS, or Sequin can push changes to you via webhooks. You can even turn historical data in Postgres into a stream of messages.

Sequin is open source. You can run Sequin in its own Docker container or as a sidecar container in your existing deployment.

Sequin comes with a web console/UI for configuration:

1. Connect any Postgres database to Sequin (Sequin uses logical replication).
2. Select the tables, actions, and SQL filters that publish messages to your consumers.
3. Consume messages using HTTP pull and push interfaces. (You can use our SDKs.)

In production, you can configure Sequin as code using Terraform.

### Use cases

* **Trigger side-effects:** Never miss a change in your database for processing by other systems.
  ![Queue email](https://i.imgur.com/SwrEHEs.png)
* **Fan out:** Add queue capabilities to your database to broadcast events, distribute workloads, and decouple services.
  ![Fan out order](https://i.imgur.com/oz6ov48.png)
* **Replication:** Sync data to other systems to update search indexes, invalidate caches, and generate denormalized views.
  ![Sync](https://i.imgur.com/B3uWnx1.png)

### Compare Sequin

* **Postgres triggers:** Sequin sends triggers over HTTP with the added benefits of exactly-once processing, payload transforms, routing, and a superior developer experience. Maintain transactional trigger guarantees with a system that scales and lets you write business logic in the programming language of your choice (no plpgsql!)
* **LISTEN/NOTIFY:**  Sequin adds exactly-once processing, larger data payloads with transformation, routing, and a superior developer experience.
* **Amazon SQS:** Sequin transactionally enqueues changes from your database into a FIFO queue with exactly-once processing. This is especially valuable if messages pertain to database changes. Instead of adding a new dependency to your infrastructure, use the database you already have to reliably queue messages.
* **Debezium / Kafka:** Sequin provides the CDC capabilities of Debezium with a durable message stream like Kafka, except with none of the operational overhead. If your database can handle it, so can Sequin. You don’t need to be a Kafka expert, learn Zookeeper, endlessly configure Debezium, or figure out the Java runtime.

## Documentation

For full documentation, visit [sequinstream.com/docs](http://sequinstream.com/docs).

## Features

* **Exactly-once processing:** Sequin delivers messages from your database to consumers exactly once within a visibility timeout.
* **SQL-based routing:** Filter and route messages to consumers using SQL `where` conditions.
* **Replays:** Rewind consumers to any row on your table. Or republish select messages that match a SQL query.
* **Backfills:** Consumers can queue up records from this moment forward or for the entire table.
* **Dynamic concurrency:** Scale workers for a consumer up or down as needed. No need to plan out partition counts ahead of time.
* **Transforms** (coming soon\!): Transform message payloads by writing functions in Lua, JavaScript, or Go.

## Getting started

### Installation

Sequin is available as a cloud version or self-hosted. See the following docs to get up and running quickly:

* [Sequin cloud quickstart](https://sequinstream.com/docs/quickstart)
* [Sequin self-hosted quickstart](https://sequinstream.com/docs/self-hosting/installation)

### SDKs

Pull and acknowledge messages or dynamically create new consumers:

* [JavaScript](https://github.com/sequinstream/sequin-js)
* [Python](https://github.com/sequinstream/sequin-py)
* [Go](https://github.com/sequinstream/sequin-go)
* [Elixir](https://github.com/sequinstream/sequin-elixir) (and [Elixir Broadway](https://github.com/sequinstream/off\_broadway\_sequin))
* [Ruby](https://github.com/sequinstream/sequin-ruby)

## Contribute

Sequin is open-sourced with an MIT license. The project is just getting started, so the best way to contribute right now is to open an issue or send us a note: [support@sequinstream.com](mailto:support@sequinstream.com)
