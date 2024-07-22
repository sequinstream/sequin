# Getting Started

In this guide, you'll:

1. Install and boot Sequin
2. Send messages to sequin from the CLI / HTTP
3. Consume messages from sequin via a consumer

## Install and boot Sequin

Sequin is a stateless executable that sits in front of a Postgres database.

To work with Sequin, you'll:

1. Install the Sequin CLI (for interacting with Sequin)
2. Boot a Sequin instance (running in Docker)

The Sequin Docker image comes with its own Postgres database for simplicity, but you can use Sequin with any Postgres database.

#### Install the Sequin CLI

Install with Homebrew:

```
brew tap sequin-io/sequin git@github.com:sequin-io/homebrew-sequin
brew install sequin
```

Install with shell:

```bash
curl -sf https://raw.githubusercontent.com/sequinstream/sequin/main/cli/installer.sh | sh
```

#### Boot Sequin with Docker compose

Now, clone Sequin and boot it with Docker compose:

```
git clone git@github.com:sequinstream/sequin.git
cd sequin/docker
docker compose up -d
```

## Send messages

**Messages** in Sequin consist of a **key** and a **data** payload. Messages are unique by key.

Keys in Sequin allow for powerful message routing and filtering. For example, you can organize messages from different sources using a hierarchical key structure.

To get a feel for this, let’s simulate an order for an eCommerce use case. Send your first message:

```
sequin stream send default orders.us.cus_a.order_1 '{"product": "Shoes"}'
```

All CLI commands use HTTP to communicate with the Sequin backend. You can see what the requests look like by passing the `--as-curl` flag:

```
sequin stream send default orders.us.cus_a.order_1 '{"product": "Shoes"}' --as-curl
```

Send more messages to Sequin:

```
sequin stream send default orders.eu.cus_b.order_2 '{"product": "Pants"}'
sequin stream send default orders.us.cus_c.order_3 '{"product": "Shoes"}'
sequin stream send default orders.us.cus_a.order_4 '{"product": "Shirts"}'
```

Now, take a look at how many messages you have in Sequin. Open a second terminal window or split pane so you can **observe your stream** as you enter more Sequin CLI commands:

```
sequin observe
```

The observe tool shows the live state of Sequin. You can read and filter these messages using the `f` command to enter a key filter like:

```
orders.us.>
orders.*.cus_a.*
>
```

By default, Sequin is configured so that messages are always upserted. So, when cus_b adds another product to their order:

```
sequin stream send default orders.eu.cus_b.order_2 '{"product": ["Socks", "Shoes"]}'
```

The message is updated in the stream. (You can change this setting per consumer by setting `one-message-per-key=false`.)

But upserting and listing messages is something we can already do in Postgres. The real power of Sequin comes from consumers.

## Consuming messages

In Sequin, a **consumer** is how you receive messages with delivery guarantees.

The best way to understand consumers is to see how they work. Add a consumer:

```
sequin consumer add default us_orders --filter="orders.us.>" --defaults
```

> [!NOTE]
> The --defaults flag creates the consumers using reasonable defaults. You can learn more in our docs (coming soon!).

This consumer will receive all orders in the US. A set of workers might then route these orders to different fulfillment systems or trigger updates to customers.

Take a look at your terminal running `sequin observe`. Switch to the Consumers tab with the c command and press `enter` to view consumer details.

You receive messages from a consumer using `receive`. When a consumer receives a message, the message is not delivered to other workers for that consumer. Notice how if you receive for a consumer multiple times in a row, you chew through the available messages until no more are available:

```
echo "Command 1 output:" && sequin consumer receive default us_orders
echo "\nCommand 2 output:" && sequin consumer receive default us_orders
echo "\nCommand 3 output:" && sequin consumer receive default us_orders
echo "\nCommand 4 output:" && sequin consumer receive default us_orders
```

You will have received the three US based orders. The last `receive` should have returned "No messages available."

The three messages are now pending for the consumer. While they are pending, they are not visible to other consumer receive calls. If the messages are not acked, they will be available for re-delivery in 30 seconds (ack-wait-ms).

These critical feature of consumers ensures that:

1. You can have multiple workers pulling messages for a single consumer
2. Messages are not lost; if a consumer doesn't ack a message, it's redelivered

> [!TIP]
> You can configure the `ack-wait-ms` setting for a consumer with `sequin consumer edit <consumer-name> --ack-wait-ms=ACK-WAIT-MS`. This setting controls how long messages remain invisible, waiting to be ack'd, after they are delivered.

## Next Steps

### Ingesting messages

Sequin ships with three powerful way to ingest messages:

1. CLI / HTTP
2. [Postgres Replication](getting-started-with-postgres-replication.md)
3. [Webhook Ingestion](getting-started-with-webhook-ingestion.md)

### Official Clients

When you are ready to integrate Sequin consumers in your application, we have a language specific libraries and example projects:

| Language | Client                                                                   | Example                                                                                           |
| -------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| Elixir   | [OffBroadwaySequin](https://github.com/sequinstream/off_broadway_sequin) | [Sequin with Broadway](https://github.com/sequinstream/sequin/tree/main/examples/elixir_broadway) |
