# Sequin

> [!IMPORTANT]
> _08/15/2024_: Sequin is undergoing major upgrades, and `main` will not be stable for a couple weeks. [See the `0.3.7` tag for the last stable release](https://github.com/sequinstream/sequin/tree/v0.3.7). Join the [release list](https://sequinstream.com/alpha) to get notified when we hit alpha.

## What is Sequin?

Sequin is an open source message stream built on Postgres. It's like Kafka, but its Postgres foundation gives you more flexibility with less operational overhead.

Sequin runs as a stateless Docker container in front of any Postgres database. It comes with a CLI, HTTP interface, and the ability to pull changes from any Postgres table into a stream.

You can also [roll your own](https://blog.sequinstream.com/build-your-own-sqs-or-kafka-with-postgres/) basic version of Sequin.

### Stream not a queue

Sequin is a message stream, not just a queue. Like Kafka, Sequin persists messages according to a retention policy you specify. You can setup **consumers** to process a filtered subset of messages in the stream. Sequin delivers messages to consumers exactly once within a visibility timeout.

With a stream instead of a queue, Sequin provides features like message replay and consumer rewind. Consumers can "join" the stream at any time and play through the history of messages. You can fan out to many individual services with exactly once delivery to each. Message history also helps with debugging and troubleshooting.

## Why Sequin?

Streams are powerful and versatile data structures. Unlike queues, streams persist messages. This means you can fan out messages, replay messages, and dynamically materialize messages into views.

Kafka popularized streams. Developed at LinkedIn, it prioritizes scale over ergonomics and usability. Kafka uses topics, partitions, and offsets to make it horizontally scalable. But this consumption paradigm is limiting and hard to work with. And Kafka is famously difficult to run.

We think that if streams were easier to use and had more flexible consumption patterns, more developers would use them. We also know Postgres is the perfect data layer for ergonomic streams.

In Sequin, messages are stored like a stream. But consumption patterns are more flexible and ergonomic, like a queue. And you don't need to worry about topics, partitions, or Zookeeper.

### Killer features

- **Store like a stream, consume like a queue**: Sequin persists messages like a stream. But the consumption pattern is more flexible and ergonomic, like a queue.
- [**Key-based routing**](https://sequinstream.com/docs/core/overview#key): Publish messages with a rich key that consumers can use to filter down to the data they care about.
- **Replays**: Rewind consumers to any point in a stream. Or republish select messages that match a SQL query.
- [**Exactly once delivery**](https://sequinstream.com/docs/core/overview#guarantees): Sequin delivers messages to consumers exactly once within a visibility timeout.
- **Dynamic concurrency**: Scale workers for a consumer up or down as needed. No need to plan out partition counts ahead of time.
- [**Upsert support**](https://sequinstream.com/docs/core/streams#policies): Get immediate compaction by key, so only one record for a key exists in the stream at a time.
- [**Stream any Postgres table**](https://sequinstream.com/docs/core/sources#postgres): Sequin can pull changes from any Postgres table into a stream.
- [**It's Just Postgres™**](https://sequinstream.com/docs/core/streams#schema): The core of Sequin is a few Postgres tables. So you can query your streams like any other table.

## Works with any Postgres database

Sequin is compatible with any Postgres database, including those from cloud providers like:

- AWS RDS Postgres
- Azure Database for Postgres
- GCP Cloud SQL
- Neon
- ...and many more

## Benchmarks and performance

Postgres is an extremely robust and high-performance system. It does a stellar job handling message streaming workloads like Sequin, when leveraged properly.

Performance is highly dependent on machine resources. But to give you an idea:

A `db.m5.xlarge` AWS RDS instance (4 vCPU, 16 GB RAM, $260/mo) can handle **5,000 messages/second** with three consumers (concurrency of 50 instances reading and writing). It can burst up to 10k messages/second. This is the case with `batch_size=10` on both publish and receive. Messages at ~10-100kb in size.

## Docs

📖 [Find the full docs here](https://sequinstream.com/docs).

## Installation

### Server

Run the server with Docker Compose. Clone the repo:

```bash
git clone git@github.com:sequinstream/sequin.git
```

After cloning the repo, `cd` into it and boot the server:

```bash
cd docker && docker compose up -d
```

The server runs on `localhost:7376`. The CLI is configured to point to `localhost:7376` by default.

### CLI

Install the Sequin CLI to work with Sequin.

Install with Homebrew:

```bash
brew tap sequinstream/sequin git@github.com:sequinstream/homebrew-sequin
brew install sequin
```

Or install with shell:

```bash
curl -sf https://raw.githubusercontent.com/sequinstream/sequin/main/cli/installer.sh | sh
```

Or build from source:

```bash
git clone git@github.com:sequinstream/sequin.git
cd cli && make build
```

Reload your console and test that it's installed:

```bash
sequin --help
```

## Core concepts

### Messages

A **message** in Sequin consists of a `key` and a `data` payload.

### Key

Sequin uses **key-based routing**. In Sequin, a **key** is a string that serves two purposes:

1. It's the unique key for the message (the primary key of the `messages` table under the hood)
2. It contains attributes that can be used for consumer filtering

In Kafka, you route on write into a topic. But with Sequin, you route on read. Because consumers filter by key, you should choose the format of your keys intentionally.

For example, an e-commerce company might choose a format like:

`orders.[customer_id].[order_id]`

And the keys might look like:

- `orders.cus_a.ord_1`
- `orders.cus_b.ord_2`
- `orders.cus_a.ord_3`

### Streams

A **stream** is a table of messages. When you send a message to Sequin, it is stored in a stream. [Consumers](#consumers) are how you read messages from the stream with delivery guarantees.

### Consumers

A **consumer** is how to read messages with delivery guarantees.

#### Key filter

Consumers are configured with a **key filter** which determines which messages are available to that consumer. These filters follow the [NATS standard](https://docs.nats.io/nats-concepts/subjects). The key filter can match on tokens and can include wildcards:

- The wildcard `*` matches a single token.
- The wildcard `>` will match one or more tokens and can only appear at the end of the filter.

For instance, `orders.cus_a.*` will return all orders for `cus_a` while `orders.>` will return every order for all customers.

#### At-least-once with retries

Your system receives messages from a consumer and then acks them once they have been processed.

If your system doesn't ack a message within a consumer's `ack-wait-ms` period, the message will become available again to that consumer. (`ack-wait-ms` is configurable.)

#### Concurrent

Multiple workers can concurrently receive messages from a consumer. Messages are only delivered once during the `ack-wait-ms` period. If the message is not ack'd before the period elapses, the message becomes available again.

### Guarantees

#### Exactly-once delivery

Exactly-once delivery within a visibility timeout (`ack-wait-ms`).

#### Strict ordering by message key

Messages with the same key will always be delivered to a consumer's workers in order (by insertion time).

If you've set `one-message-per-key=true` for a stream, then all message key conflicts are upserts. If a message has been delivered but not ack'd (is currently being worked), then delivery of the next message with that key will be withheld. When the `ack-wait-ms` expires or the message is ack'd/nack'd, the latest version of the message will be available for delivery.

If you've set `one-message-per-key=false` for a stream, then messages are appended to a stream. Still, if a message has been delivered but not ack'd (is currently being worked), then delivery of the next message with that key will be withheld until that message has finished processing.

Learn more about [the core concepts of Sequin](https://sequinstream.com/docs/core/overview)

## CLI

Sequin comes with a powerful CLI for managing and observing streams, messages, and consumers. [Read more about the CLI](https://sequinstream.com/docs/cli).

Sequin's `observe` is like [htop](https://github.com/htop-dev/htop) for your Sequin instance. You can watch messages flow into your system, into a consumer's outbox, and get ack'd in real-time.

Boot it with:

```bash
sequin observe
```

![sequin's observe command showing streams, consumers, and messages](./docs/images/observe.gif)

## SDKs

- [JavaScript](https://github.com/sequinstream/sequin-js)
- [Elixir](https://github.com/sequinstream/sequin-elixir)
  - And [Elixir Broadway](https://github.com/sequinstream/off_broadway_sequin)
- [Go](https://github.com/sequinstream/sequin-go)
- [Ruby](https://github.com/sequinstream/sequin-ruby)
- [Python](https://github.com/sequinstream/sequin-py)

Don't see the SDK you need? [Open an issue](https://github.com/sequinstream/sequin/issues).

## Sources

Sequin has first-class support for ingesting messages from:

1. `postgres`: Existing Postgres tables (via the WAL)
2. `webhook`: External APIs

For example, with the `postgres` source, you can use Sequin to treat the creates, updates, and deletes from any existing Postgres table as a stream of messages.

[Learn more about sources](https://sequinstream.com/docs/core/sources).

## Examples

### Using CLI

Create a stream:

```bash
sequin stream add mystream
```

Send messages:

```bash
sequin stream send mystream orders.us.cus_a.order_1 '{"product": "Shoes"}'
sequin stream send mystream orders.eu.cus_b.order_2 '{"product": "Pants"}'
```

Create a consumer:

```bash
sequin consumer add mystream us_orders --filter="orders.us.>" --defaults
```

Receive messages:

```bash
sequin consumer receive us_orders
```

### Using HTTP

Create a stream:

```bash
curl -X POST http://localhost:7376/v1/streams \
  -H "Content-Type: application/json" \
  -d '{"name": "mystream"}'
```

Send messages:

```bash
curl -X POST http://localhost:7376/v1/streams/mystream/messages \
  -H "Content-Type: application/json" \
  -d '{
    "key": "orders.us.cus_a.order_1",
    "data": {"product": "Shoes"}
  }'

curl -X POST http://localhost:7376/v1/streams/mystream/messages \
  -H "Content-Type: application/json" \
  -d '{
    "key": "orders.eu.cus_b.order_2",
    "data": {"product": "Pants"}
  }'
```

Create a consumer:

```bash
curl -X POST http://localhost:7376/v1/streams/mystream/consumers \
  -H "Content-Type: application/json" \
  -d '{
    "name": "us_orders",
    "filter": "orders.us.>",
    "ack_wait_ms": 30000,
    "max_ack_pending": 100,
    "max_deliver": 10
  }'
```

Receive messages:

```bash
curl -X POST http://localhost:7376/v1/streams/mystream/consumers/us_orders/messages \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 10}'
```

## How it works

The Sequin server is a stateless Docker container that sits in front of any Postgres database. It provides an HTTP interface for sending and receiving messages, as well as managing the system.

Sequin uses a Postgres schema (default: `sequin_config`) to store metadata about streams, consumers, and other objects.

For the streams themselves, Sequin uses a separate Postgres schema (default: `sequin_stream`). That schema can live on a different Postgres database from the config.

There are two tables in `sequin_stream`: `messages` stores all the messages and `consumer_messages` stores the state of available and delivered messages for each consumer. `messages` is partitioned by stream. And `consumer_messages` is partitioned by consumer.

New messages are written to `messages` and fanned out to `consumer_messages` in the same transaction.

On insert, a message's `key` is tokenized and broken out across 16 token columns. This allows Sequin to leverage Postgres indexes for fast message lookups by key pattern (e.g. `payroll.adp.*.employee.*`).

### Design considerations

We tried and benchmarked several solutions, some that we felt were very clever. But we landed on a `messages` table with an outbox pattern (`consumer_message`) because it's both simple and performant!

Because consumer-message state needs to be tracked on disk, a `consumer_message` row needs to be created _at some point_. We decided it was best to do that on message insert. By moving more load to writes, we can avoid a situation where the system can write a lot faster than it can read.

For the best performance, on top of intelligent indexes, we avoid trigger functions (degrade performance when batching) and are careful about constraints.

## Deployments

You can run Sequin anywhere you can run Docker. You can even "sidecar" Sequin alongside your app.

Read more about [deploying Sequin](https://sequinstream.com/docs/deploying).
