# Sequin

> [!IMPORTANT]
> Sequin is still pre-alpha. Join the [release list](https://sequinstream.com/alpha) to get notified when we launch.

## What is Sequin?

Sequin is an open source message stream built on Postgres. It's comparable to Kafka, but offers more read flexibility with less operational overhead. Sequin comes with a CLI, HTTP interface, and ability to pull changes from any Postgres table into a stream.

### Stream not a queue

Sequin is a message stream, not just a queue. Like Kafka, Sequin persists messages according to a retention policy you specify. You can setup **consumers** to process a filtered subset of messages in the stream. Sequin delivers messages to consumers exactly once within a visibility timeout.

With a stream instead of a queue, Sequin provides features like message replay and consumer rewind. Consumers can "join" the stream at any time and play through the history of messages. You can fan out to many individual services with exactly once delivery to each. Message history also helps with debugging and troubleshooting.

## Why Sequin?

Sequin is tailored for developers who need:

1. **Service decoupling**: Reliably pass messages between services.
2. **Durable LISTEN/NOTIFY**: Easily process changes from Postgres tables.
3. **HTTP endpoints**: Safely ingest and process webhooks from external services.
4. **Observability**: Developer-friendly tools to monitor and debug message flows.
5. **Simplicity**: A powerful stream without a steep learning curve.

If these align with your needs, Sequin offers a streamlined alternative to complex messaging systems like Kafka.

### Contrast to Kafka

Sequin shares a lot of similarities with Kafka:

- **Streams**: Sequin uses streams to fan out messages to many consumers.
- **Retention policies**: Time-based or storage-based retention policies.
- **Compact by key**: Like Kafka, you can compact messages with the same key. (Unlike Kafka, compaction happens immediately on write.)
- **Guaranteed message ordering**: Messages with the same key are delivered to a consumer in order.

But with the read flexibility often found in message queues like AWS SQS:

- **No topics**: Sequin uses [key-based routing](#key) to fan out messages to many consumers.
- **No partitions**: Sequin maintains guaranteed ordering without partitioning.

And with the familiarity of Postgres:

- **Simpler infrastructure**: No need for a separate Kafka broker or Zookeeper instance.
- **Read at rest in SQL**: Sequin stores messages at rest in Postgres, so you can query them like any other table.

## Benchmarks and performance

Postgres is an extremely robust and high-performance system. It does a stellar job handling message streaming workloads like Sequin, when leveraged properly.

Performance is highly dependent on machine resources. But to give you an idea:

A `db.m5.xlarge` AWS RDS instance (4 vCPU, 16 GB RAM, $260/mo) can handle **5,000 messages/second** with three consumers (concurrency of 50 instances reading and writing). It can burst up to 10k messages/second. This is the case with `batch_size=10` on both publish and receive.

Expect this performance to degrade with messages >512kb in size.

## Core concepts

### Messages

A **message** in Sequin consists of a `key` and a `data` payload.

### Key

In Sequin, a **key** is a string that serves two purposes:

1. It’s the unique key for the message (the primary key of the `messages` table under the hood)
2. It contains attributes that can be used for consumer filtering

Because consumers filter by key, you should choose the format of your keys intentionally.

For example, an e-commerce company might choose a format like:

`orders.[customer_id].[order_id]`

And the keys might look like:

- `orders.cus_a.ord_1`
- `orders.cus_b.ord_2`
- `orders.cus_a.ord_3`

### Consumers

A **consumer** is how to read messages with delivery guarantees.

#### Key filter

Consumers are configured with a **key filter** which determines which messages are available to that consumer. These filters follow the [NATS standard](https://docs.nats.io/nats-concepts/subjects). The key filter can match on tokens and can include wildcards:

- The wildcard `*` matches a single token.
- The wildcard `>` will match one or more tokens and can only appear at the end of the filter.

For instance, `orders.cus_a.*` will return all orders for `cus_a` while `orders.>` will return every order for all customers.

#### At-least-once with retries

Your system receives messages from a consumer and then acks them once they have been processed.

If your system doesn’t ack a message within a consumer’s `ack-wait-ms` period, the message will become available again to that consumer. (`ack-wait-ms` is configurable.)

#### Concurrent

Multiple workers can concurrently receive messages from a consumer. Messages are only delivered once during the `ack-wait-ms` period. If the message is not ack’d before the period elapses, the message becomes available again.

## Guarantees

### Exactly-once delivery

Exactly-once delivery within a visibility timeout (`ack-wait-ms`).

### Strict ordering by message key

Messages with the same key will always be delivered to a consumer's workers in order (by insertion time).

If you've set `one-message-per-key=true` for a stream, then all message key conflicts are upserts. If a message has been delivered but not ack'd (is currently being worked), then delivery of the next message with that key will be withheld. When the `ack-wait-ms` expires or the message is ack'd/nack'd, the latest version of the message will be available for delivery.

If you've set `one-message-per-key=false` for a stream, then messages are appended to a stream. Still, if a message has been delivered but not ack'd (is currently being worked), then delivery of the next message with that key will be withheld until that message has finished processing.

## Setup

### Server

Run the server with Docker/compose. After cloning the repo:

```bash
cd docker && docker compose up -d
```

The server runs on `localhost:7376`. The CLI is configured to point to `localhost:7376` by default.

### CLI

#### Homebrew

```bash
brew tap sequinstream/homebrew-sequin git@github.com:sequin-io/homebrew-sequin
brew install sequin
```

#### Shell

```bash
curl -sf https://raw.githubusercontent.com/sequinstream/sequin/main/cli/installer.sh | sh
```

#### Build from source

To build from source, clone this repo then:

```bash
cd cli && make build
```

Reload your console and test that it's installed:

```bash
sequin --help
```

## Getting Started

See the [getting started guide](./docs/getting-started.md).

## CLI

Sequin comes with a CLI you can use to interact with and observe a running Sequin instance. You can use the CLI to set up streams and consumers, and observe messages flowing through your system in real-time.

### `--as-curl`

For every CLI command, you can pass the `--as-curl` flag to have the command printed out as a curl request:

```bash
sequin consumer receive mycon --batch-size=10 --as-curl
```

### Contexts

You can use contexts to easily switch your CLI between environments. For example, if you're debugging production, you can run:

```bash
sequin observe --context=prod
```

And the Sequin CLI will use your settings for prod to connect to your production Sequin instance.

Create a context like so:

```bash
sequin context create [<flags>] [<name>]
```

### Observe

Sequin's `observe` is like [htop](https://github.com/htop-dev/htop) for your Sequin instance. You can watch messages flow into your system, into a consumer's outbox, and get ack'd in real-time.

Boot it with:

```bash
sequin observe
```

![sequin's observe command showing streams, consumers, and messages](./docs/gifs/observe.gif)

## Sending messages

You can send a message to Sequin by specifying the `key` and the `data` payload:

```bash
sequin message send orders.cus_a.ord_1jNsd '{ "quantity": 4 }'
```

When sending via HTTP, `messages` can contain a list of 1 or more messages:

```bash
curl -X 'POST' -d '{"messages":[{"data":"{ \"quantity\": 4 }","key":"orders.cus_a.ord_1jNsd"}, {"data":"{ \"quantity\": 4 }","key":"orders.cus_a.ord_1jNsd"}]}' \
-H 'Content-Type: application/json' \
-H 'Authorization: Bearer {token} \
'https://[sequin-base-url]/api/streams/68658e3e-de48-4005-a5de-2a7cd6868d80/messages'
```

The maximum batch size per send request is 10,000 messages. We generally recommend smaller batch sizes if each message is large. Keeping the entire batch to under ~10MB is usually a good rule in most cases.

## Streams

A **stream** is a table of messages. When you send a message to Sequin, it is stored in a stream. [Consumers](#consumers) are how you read messages from the stream with delivery guarantees.

If you're just starting out, using a single stream for all your data is sufficient. You'll use separate streams when you want to have different policies for a set of messages. They can also be helpful when scaling, as each stream is a separate Postgres table under the hood.

To add a stream to Sequin, run:

```bash
sequin stream add [<stream>] [<flags>]
```

### Policies

Streams can be configured with a number of different policies that affect message processing and retention:

#### `one-message-per-key=[<bool>]`

When `one-message-per-key` is set to `true`, Sequin places a unique constraint on message's `key`. All messages will be upserted. There will only ever be one message with a given key in the stream, the last message with that key received.

This setting is a great fit for processing records where only the latest version of a message matters to your system.

#### `process-unmodified=[<bool>]`

Must be used in combination with `one-message-per-key=true` and defaults to `false`. When `process-unmodified` is false, a stream will ignore messages that (1) have a key that matches an existing message in the stream and (2) are unchanged (according to a SHA256 hash of `data`). This saves the stream from re-inserting the message and all consumers from re-processing it.

You can set this setting to `true` if you want messages to be reprocessed by consumers, even if their contents haven't changed. This can be useful for processing command messages, where the contents don't change but you still want your system to reprocess them.

#### `max-storage=[<gigabytes>]gb`

The approximate maximum storage (in gigabytes) that a stream can expand to.

When a stream hits `max-storage`, messages will be evicted oldest-first, respecting `retain-at-least`.

Postgres' analytics around storage are approximate, so we recommend ensuring a 20% buffer. i.e. if you set a stream's `max-storage` to 100GB, you should ensure that your Postgres volume is able to accommodate a 120GB stream.

#### `retain-up-to=[<duration>]`

Messages that are older than `duration` will be evicted from the stream.

#### `retain-at-least=[<duration>]`

Must be used in combination with `max-storage`. `retain-at-least` is a safety mechanism. It ensures that if a stream hits `max-storage`, Sequin will never evict messages younger than `retain-at-least`. You can set `retain-at-least=infinite` to ensure nothing is evicted.

If a stream hits `max-storage` but can no longer evict messages according to `retain-at-least`, it will stop receiving messages.

It can be a good idea to set `retain-at-least` if you're setting `max-storage`. If your stream starts growing unexpectedly, the combination of these two settings can act as a failsafe.

## Consumers

**Consumers** are central to Sequin. A consumer is how workers in your system receive messages. Using consumers allow you to:

- Filter the message stream down to what a given worker/system cares about
- Safely process messages concurrently
- Ensure messages are processed exactly once

To add a consumer to Sequin, run:

```bash
sequin consumer add [<stream>] [<consumer>] [<flags>]
```

For example:

```bash
# Create a consumer for all orders in the system
sequin consumer add mystream mycon --ack-wait-ms=60000 --filter="orders.>"
```

The flags:

```bash
  --ack-wait-ms=ACK-WAIT-MS          Acknowledgement wait time in milliseconds
  --max-ack-pending=MAX-ACK-PENDING  Maximum number of pending acknowledgements
  --max-deliver=MAX-DELIVER          Maximum number of delivery attempts
  --filter=FILTER                    Key pattern for message filtering
  --defaults                         Use default values for non-required fields
```

When no flags are provided, the CLI will walk you through setup.

### Receiving messages

To receive a message for a consumer, run:

```bash
sequin receive [<stream>] [<consumer>] [<flags>]
```

For example:

```bash
sequin receive mystream mycon --batch-size=10
```

Or, via HTTP:

```bash
curl -X 'GET' https://[sequin-base-url]/api/streams/mystream/consumers/mycon/receive?batch_size=10'
```

After a batch of messages is delivered to your worker, those messages will not be available to other workers for that consumer for `ack-wait-ms`. Messages are delivered with an `ack_id` that your worker will [use to ack them](#acking-messages).

### Acking messages

After your system receives messages from a consumer, it will process them. When it's finished processing, it should ack those messages. Ack'ing ensures those messages will not be redelivered and is how Sequin guarantees exactly-once processing.

You ack messages using their `ack_id`. The `ack_id` for each message is included with every delivery.

Using the CLI:

```bash
sequin consumer ack [<stream>] [<consumer>] [<ack_id>]
```

When ack'ing via HTTP, you can include a batch of ack_ids to acknowledge all together:

```bash
curl -X 'POST' -d '{"ack_ids":["ack_1jdiQuj"]}' -H 'Content-Type: application/json' 'https://[sequin-base-url]/api/streams/mystream/consumers/mycon/ack'
```

### Nacking messages

You can also "negative ack" or "nack" messages. This makes the messages available again for immediate redelivery.

Using the CLI:

```bash
sequin consumer nack mystream mycon [<ack_id>]
```

When nack'ing via HTTP, you can include a batch of ack_ids to nack all together:

```bash
curl -X 'POST' -d '{"ack_ids":["ack_1jdiQuj"]}' -H 'Content-Type: application/json' 'https://[sequin-base-url]/api/streams/mystream/consumers/mycon/nack'
```

Nacking is an advanced feature. Usually, it's fine to just let the `ack-wait-ms` for a message expire and have it be redelivered that way.

### Peeking messages

For debugging purposes, you can also "peek" at messages and their states for a consumer. Peeking lets you see the delivered messages and upcoming messages, without affecting their states.

You can peek via the CLI:

```bash
sequin consumer peek [<flags>] [<stream>] [<consumer>]
```

Or via HTTP:

```bash
curl -X 'GET' 'https://<sequin-base-url>/api/streams/<stream>/consumers/<consumer_name>/messages?limit=10&sort=seq_desc&visible=true'
```

You can also view consumer states in real-time with [observe](#observe).

### Learn more

This is just a subset of consumer configuration and usage. Learn more by running `sequin consumer --help`.

## Sources

Sequin has first-class support for ingesting messages from:

1. `postgres`: Existing Postgres tables (via the WAL)
2. `webhook`: External APIs

### The `postgres` source

You can use Sequin to treat the creates, updates, and deletes from any existing Postgres table as a stream of messages.

From the CLI, connect any Postgres table to Sequin:

```
sequin source postgres add
```

The CLI will walk you through setting up a replication slot and publication.

Learn more about the [`postgres` source](./docs/postgres-source.md).

### Comparison to WAL

The WAL must be processed in serial. This makes it hard to scale, as you can only have one worker safely consume from it. And any one slow or corrupt message will jam your pipeline.

With Sequin, WAL messages can be fanned out to many consumers for parallel processing and independent handling. (To make this safe, messages with the same key will [always be delivered in serial](#how-it-works).)

### Comparison to LISTEN/NOTIFY

Postgres' LISTEN/NOTIFY pushes notifications to your system whenever certain changes occur in Postgres. Because it's an at-most-once system, it works OK if you're fine with missing messages. But if your listening processes are not connected to the database during a NOTIFY event, they will miss that event.

Sequin offers at-least-once delivery and first-class features like fan-out and replay.

### The `webhook` source

You can use Sequin to ingest incoming messages from external APIs.

From the CLI, setup an HTTP endpoint for others to post to with:

```
sequin source webhook add
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

You can run Sequin anywhere you can run Docker. It stores its configuration state and streams in [Postgres](#how-it-works).

You can even "sidecar" Sequin alongside your app.

## Roadmap

- Allow for streams to live on any database
- Control the size of `consumer_messages` – don't fan out to dormant consumers
- Client SDKs
- First-class Webhook source support
