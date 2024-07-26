import { sequin, baseUrl } from "./sequin.js";
import { db } from "./database.js";

const STREAM_NAME = "orders";
const CONSUMER_NAME = "payment_processor";

export async function setupPaymentConsumer() {
  try {
    const response = await fetch(
      `${baseUrl}/api/streams/${STREAM_NAME}/consumers`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const consumers = await response.json();
    const consumerExists = consumers.data.some(
      (consumer) => consumer.name === CONSUMER_NAME
    );

    if (consumerExists) {
      console.log(`Consumer ${CONSUMER_NAME} already exists.`);
      return;
    }

    const { res, error } = await sequin.createConsumer(
      STREAM_NAME,
      CONSUMER_NAME,
      "order.>"
    );
    if (error) {
      console.error("Error creating payment consumer:", error.summary);
    } else {
      console.log("Payment consumer created:", res.name);
    }
  } catch (error) {
    console.error("Error setting up payment consumer:", error);
  }
}

export async function processPayments() {
  while (true) {
    const { res, error } = await sequin.receiveMessage(
      STREAM_NAME,
      CONSUMER_NAME
    );
    if (error) {
      console.error("Error receiving message:", error.summary);
      continue;
    }
    if (!res) {
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait if no messages
      continue;
    }

    const order = JSON.parse(res.message.data);
    console.log("💳 Processing payment for order:", order.orderId);

    // Simulate payment processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Update paid status in database
    db.prepare("UPDATE orders SET paid = TRUE WHERE order_id = ?").run(
      order.orderId
    );

    await sequin.ackMessage(STREAM_NAME, CONSUMER_NAME, res.ack_id);
    console.log("💳Payment processed for order:", order.orderId);
  }
}
