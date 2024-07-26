# Example: JavaScript Orders Fan-out

This example demonstrates a fan-out pattern using Sequin and the sequin-js library. It simulates an e-commerce system where orders are placed and then processed by multiple services concurrently.

## What this example does

1. Creates an order stream and multiple consumers (payment, inventory, and shipping).
2. Simulates placing orders at regular intervals.
3. Processes each order through three separate services:
   - Payment processing
   - Inventory management
   - Shipping handling

Each service operates independently, consuming messages from the same order stream, showcasing the fan-out pattern.

## Setup and Running

1. Install dependencies:
   ```
   npm install
   ```

2. Ensure you have Sequin running locally. The default URL is http://localhost:7376.

3. Copy the `.env.example` file to `.env` and modify if needed:
   ```
   cp .env.example .env
   ```

4. Set the SEQUIN_URL environment variable if your Sequin instance is running on a different URL:
   ```
   export SEQUIN_URL=http://your-sequin-url
   ```

5. Run the example:
   ```
   node src/index.js
   ```

The application will start placing orders every 5 seconds and processing them through the payment, inventory, and shipping services. You'll see console output indicating the progress of each order through the system.

Note: This example uses a local SQLite database to store product and order information. The database file (ecommerce.db) will be created automatically when you run the application for the first time.