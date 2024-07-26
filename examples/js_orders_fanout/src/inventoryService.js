import { sequin, baseUrl } from "./sequin.js";
import { db } from "./database.js";

const STREAM_NAME = "orders";
const CONSUMER_NAME = "inventory_manager";

export async function setupInventoryConsumer() {
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
      console.error("Error creating inventory consumer:", error.summary);
    } else {
      console.log("Inventory consumer created:", res.name);
    }
  } catch (error) {
    console.error("Error setting up inventory consumer:", error);
  }
}

export async function manageInventory() {
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
    console.log("📉 Updating inventory for order:", order.orderId);

    // Update inventory in database
    const updateInventory = db.prepare(
      "UPDATE product_inventory SET quantity_available = quantity_available - ? WHERE id = ?"
    );
    updateInventory.run(order.product.quantity, order.product.id);

    await sequin.ackMessage(STREAM_NAME, CONSUMER_NAME, res.ack_id);
    console.log("📉Inventory updated for order:", order.orderId);
  }
}
