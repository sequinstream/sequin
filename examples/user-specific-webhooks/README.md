# User-Specific Webhooks Example

This example demonstrates how to build a system that allows users to subscribe to webhooks for their own data using Sequin. It includes:

- A server that handles webhook subscriptions
- A notification generator for testing
- Step-by-step instructions for setup and testing

You can read the step-by-step tutorial in our [docs](https://sequinstream.com/docs/guides/user-webhooks)

## Prerequisites

- Node.js installed
- PostgreSQL database running
- Sequin running locally
- A Sequin API token (get this from the Sequin console)

## Setup

1. Install dependencies:
```bash
npm install
```

2. Set up your database tables:
```sql
-- Create users table
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

-- Create notifications table
CREATE TABLE notifications (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id) ON DELETE CASCADE,
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Create webhook_subscriptions table
CREATE TABLE webhook_subscriptions (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id) ON DELETE CASCADE,
  endpoint TEXT NOT NULL,
  sequin_sink_id UUID NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

3. Create some test users:
```sql
INSERT INTO users (name) VALUES ('User 1'), ('User 2'), ('User 3');
```

## Running the Server

1. Start the webhook server:
```bash
node server.js
```

The server will start on port 3333.

## Testing the System

### 1. Create a Webhook Subscription

Make a POST request to create a webhook subscription for a user:

```bash
curl -X POST http://localhost:3333/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "userId": 1,
    "webhookUrl": "https://webhook.site/your-unique-url"
  }'
```

:::tip
Replace `your-unique-url` with a URL from [webhook.site](https://webhook.site) to easily inspect incoming webhooks.
:::

### 2. Generate Test Notifications

In a new terminal, start generating notifications:

```bash
node generate-notifications.js
```

This will generate 100 notifications per second for random users. You should see the notifications appear in your webhook.site URL for the subscribed user.

### 3. Start a Backfill (Optional)

If you want to replay (i.e. backfill) all notifications for a user:

```bash
curl -X POST http://localhost:3333/backfill \
  -H "Content-Type: application/json" \
  -d '{"sinkName": "user-1-notifications"}'
```

## Monitoring

You can monitor the webhook system using:

1. The Sequin console at http://localhost:7376
2. The webhook.site interface for your webhook URL
3. The server logs showing notification generation rates

## Troubleshooting

- If you get a foreign key error, make sure you've created users in the database
- If webhooks aren't being received, check that:
  - The Sequin API token is correct
  - The webhook URL is accessible
  - The user ID exists in the database