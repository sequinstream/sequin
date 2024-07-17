# Getting Started

In this guide, you'll:

1. Install and boot Sequin
2. Stream your Postgres table to Sequin
3. Consume records from Sequin

## Install and boot Sequin

Sequin is a stateless executable that sits in front of a Postgres database.

To work with Sequin, you'll:

1. Install the Sequin CLI (for interacting with Sequin)
2. Boot a Sequin instance (running in Docker)

The Sequin Docker image comes with its own Postgres database for simplicity, but you can use Sequin with any Postgres database.

#### Install the Sequin CLI

```
brew tap sequin-io/sequin git@github.com:sequin-io/homebrew-sequin
brew install sequin
```

#### Boot Sequin with Docker compose

Now, clone Sequin and boot it with Docker compose:

```
git clone git@github.com:sequin-io/sequin
cd sequin/docker
docker compose up -d
```

## Send messages

**Messages** in Sequin consist of a **key** and a **data** payload. Messages are unique by key.

Keys in Sequin allow for powerful message routing and filtering. For example, you can organize messages from different sources using a hierarchical key structure.

To get a feel for this, let’s simulate an order insert for an e-commerce use case. Send your first message:

```
sequin stream send default mydb.public.orders.insert.ord_1 '{"product": "Shoes"}'
```

All CLI commands use HTTP to communicate with the Sequin backend. You can see what the requests look like by passing the `--as-curl` flag:

```
sequin stream send default mydb.public.orders.insert.ord_1 '{"product": "Shoes", "status": "new"}' --as-curl
```

Send more messages to Sequin:

```
sequin stream send default mydb.public.orders.insert.ord_2 '{"product": "Pants"}'
sequin stream send default mydb.public.orders.insert.ord_3 '{"product": "Shoes"}'
sequin stream send default mydb.public.orders.insert.ord_4 '{"product": "Shirts"}'
```

Now, take a look at how many messages you have in Sequin. Open a second terminal window or split pane so you can **observe your stream** as you enter more Sequin CLI commands:

```
sequin observe
```

The observe tool shows the live state of Sequin. You can read and filter these messages using the `f` command to enter a key filter like:

```
mydb.public.orders.insert.>
mydb.public.orders.insert.ord_1
>
```

But upserting and listing messages is something we can already do in Postgres. The real power of Sequin comes from **streaming existing tables** and receiving messages with **consumers**.

## Streaming an existing table

Run the following command to stream a Postgres table to Sequin. You can choose any local database and table for the purposes of this guide:

```bash
sequin source postgres add
```

The CLI will walk you through setting up a replication slot and publication.

When your Postgres source is setup, run `observe`:

```bash
sequin observe
```

You should see messages corresponding to your Postgres table in the stream.

If you make changes to your Postgres table, you should see those changes propagate as messages in your stream.

## Consuming messages

In Sequin, a **consumer** is how you receive messages with delivery guarantees.

The best way to understand consumers is to see how they work. Add a consumer:

```
# Change filter to match your setup
sequin consumer add pg_events --filter="mydb.>" --defaults
```

> [!NOTE]
> The --defaults flag creates the consumers using reasonable defaults. You can learn more in our docs (coming soon).

Take a look at your terminal running `sequin observe`. Switch to the Consumers tab with the c command and press enter to view consumer details.

You receive messages from a consumer using `receive`. When a consumer receives a message, the message is not delivered to other workers for that consumer. Notice how if you receive for a consumer multiple times in a row, you chew through the available messages until no more are available:

```
echo "Command 1 output:" && sequin consumer receive pg_events --no-ack
echo "\nCommand 2 output:" && sequin consumer receive pg_events --no-ack
echo "\nCommand 3 output:" && sequin consumer receive pg_events --no-ack
```

The three messages are now pending for the consumer. While they are pending, they are not visible to other consumer receive calls. If the messages are not acked, they will be available for re-delivery in 30 seconds (ack-wait-ms).

Over in the `observe` terminal, you can see the messages are in a "pending" state for your consumer.

You can ack messages with `ack`:

```
sequin consumer ack pg_events <ack-id>
```

Consumers are designed so that:

1. You can have multiple workers pulling messages for a single consumer
2. Messages are not lost; if a consumer doesn't ack a message, it's redelivered

> [!TIP]
> You can configure the `ack-wait-ms` setting for a consumer with `sequin consumer edit <consumer-slug> --ack-wait-ms=ACK-WAIT-MS`. This setting controls how long messages remain invisible, waiting to be ack'd, after they are delivered.
