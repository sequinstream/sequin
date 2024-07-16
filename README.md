# Sequin

## What is Sequin?

Sequin is an open-source message queue (technically a stream) built on PostgreSQL.

Sequin offers message ingestion and consumption similar to SQS or Kafka. But because it is built on Postgres, Sequin offers delivery mechanics (such as SQL filtering) that are not possible with existing solutions.

If you already use Postgres and need a message queue, simply extend your database with Sequin instead of adding a new system with new operational burden.

### Key Features:

- **At-least-once delivery**: All the guarantees you expect of a message queue.
- **SQL reads**: Consume messages with SQL-powered filtering.
- **Simple stack**: Extend your existing Postgres database into a message queue.
- **Predictable scaling**: If Postgres can handle your data, Sequin can as well.
- **Predictable cost**: If you know what Postgres costs, you know what Sequin costs.

## Core concepts

### Messages

A **message** in Sequin consists of a `key` and a `data` payload. Because you `upsert` messages into Sequin, there’s only ever one message per key.

### Key

In Sequin, a **key** serves two purposes:

1. It’s the unique key for the message (the primary key of the `messages` table under the hood)
2. It contains attributes that can be used for consumer filtering

Because consumers filter by key, you should choose the format of your keys intentionally.

For example, a payroll processing system might choose a format like:

`payroll.[platform].[employer-id].[employee-id].[pay-type].[pay-date]`

And the keys might look like:

- `payroll.adp.atreides_corp.employee_a.hourly.2024-02-01`
- `payroll.adp.atreides_corp.employee_b.salary.2024-02-01`
- `payroll.paychex.harkonnen_house.employee_c.commission.2024-02-01`

### Consumers

A **consumer** is how to read messages with delivery guarantees. The guarantees:

#### Key filter

Consumers are configured with a **key filter** which determines which messages are available to that consumer. The key filter can match on tokens and can include wildcards:

- `payroll.>`: Subscribe to all payroll data across all providers
- `payroll.adp.>`: Subscribe to all payroll data from ADP
- `payroll.paychex.>`: Subscribe to all payroll data from Paychex
- `payroll.*.atreides_corp.>`: Subscribe to all payroll data related to Atreides Corp employees
- etc

#### At-least-once with retries

Your system receives messages from a consumer and then acks them once they have been processed.

If your system doesn’t ack a message within a consumer’s `ack-wait-ms` period, the message will become available again to that consumer.

#### Concurrent

Multiple workers can concurrently receive messages from a consumer. Messages are only delivered once during the `ack-wait-ms` period. If the message is not ack’d before the period elapses, the message becomes available again.

## Getting Started

See the [getting started guide](./getting-started.md).
