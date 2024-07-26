import { sequin, baseUrl } from "./sequin.js";
import { db } from "./database.js";

const STREAM_NAME = "orders";

export async function setupOrderStream() {
  // Check if the stream already exists using the Sequin API
  try {
    const response = await fetch(`${baseUrl}/api/streams`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const streams = await response.json();
    const streamExists = streams.data.some(
      (stream) => stream.name === STREAM_NAME
    );

    if (streamExists) {
      console.log(`Stream ${STREAM_NAME} already exists.`);
      return;
    }

    // Create the stream if it doesn't exist
    const { res, error } = await sequin.createStream(STREAM_NAME);
    if (error) {
      console.error("Error creating stream:", error.summary);
    } else {
      console.log("Order stream created:", res.name);
    }
  } catch (error) {
    console.error("Error setting up order stream:", error);
  }
}

export async function placeOrder(customerId) {
  // Get a random product
  const product = db
    .prepare("SELECT * FROM product_inventory ORDER BY RANDOM() LIMIT 1")
    .get();
  const quantity = Math.floor(Math.random() * 5) + 1; // Random quantity between 1 and 5

  const orderId = `ORD-${Date.now()}`;
  const orderData = {
    orderId,
    customerId,
    product: {
      id: product.id,
      name: product.product_name,
      price: product.price,
      quantity,
    },
  };

  // Insert into orders table
  db.prepare(
    "INSERT INTO orders (order_id, customer_id, product_payload) VALUES (?, ?, ?)"
  ).run(orderId, customerId, JSON.stringify(orderData.product));

  // Send message to Sequin stream
  const { res, error } = await sequin.sendMessage(
    STREAM_NAME,
    `order.${orderId}`,
    JSON.stringify(orderData)
  );
  if (error) {
    console.error("Error placing order:", error.summary);
  } else {
    console.log("🛍️ Order placed:", orderId);
  }
}
