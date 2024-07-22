# Getting started with the Webhook source

> [!NOTE]
> This guide assumes you have already created a stream and a consumer. If not, please see the [getting started guide](getting-started.md).

With the `webhook` source, Sequin turns webhooks into a message queue. This means you can stream webhooks from any service to your app with at-least-once delivery.

Sequin is a great fit if you need to ingest webhooks without dropping any messages.

## Setup

From the CLI, connect any webhook to Sequin:

```bash
sequin source webhook add
```

### Creating a webhook

A webhook source is connected to a stream. Any ingested webhooks are sent to this stream.

```bash
➜  sequin source webhook add
? Choose a stream:  [Use arrows to move, type to filter]
> webhooks (ID: f6145b24-4a4a-40d8-8134-d8809eece38a)
  default (ID: c9470942-e4d7-466a-9fbe-fadae0e4f890)
```

Then choose a name for the webhook. This is used in the URL of the webhook endpoint and in the key of ingested messages:

```bash
➜ sequin source webhook add
? Choose a stream: webhooks (ID: f6145b24-4a4a-40d8-8134-d8809eece38a)
? Enter the name for the new webhook: my_webhook
```

### Sending webhooks

After creating your webhook, the webhook endpoint is displayed. You can see this again at any time with:

```bash
sequin source webhook info <webhook>
```

You can send your first webhook manually with a cURL command:

```bash
curl -X POST http://localhost:7376/api/webhook/my_webhook \
  -H "Content-Type: application/json" \
  -d '{"key": "value"}'
```

### Observe

Use `sequin observe` to view messages ingested to your stream.

### Key Pattern

Right now only one key format is supported: `<webhook>.<payload-hash>`

ie. `my_webhook.E43ABCF3375244839C012F9633F95862D232A95B00D5BC7348B3098B9FED7F32`

This ensures that unique webhooks have unique keys in your stream. Duplicate webhooks are upserted, which may lead to re-delivery of the same webhook.

> [!NOTE]
> Custom key patterns are coming soon. Please open an issue specifying your use case if this interests you.

### Shape of the message

The webhook payload is stored in your stream as it is received.

```json
{ "key": "value" }
```

### Consuming webhooks

Now that webhooks are in your stream, you can consume them with a consumer.

See the [getting started guide](getting-started.md#consuming-messages) for more information on consumers.

Or see the list of our [official clients](getting-started.md#official-clients)
