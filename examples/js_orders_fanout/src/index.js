import { initializeDatabase } from "./database.js";
import { setupOrderStream, placeOrder } from "./orderService.js";
import { setupPaymentConsumer, processPayments } from "./paymentService.js";
import { setupInventoryConsumer, manageInventory } from "./inventoryService.js";
import { setupShippingConsumer, processShipping } from "./shippingService.js";

async function main() {
  try {
    initializeDatabase();

    await setupOrderStream();
    await setupPaymentConsumer();
    await setupInventoryConsumer();
    await setupShippingConsumer();

    // Start processing services
    processPayments();
    manageInventory();
    processShipping();

    // Simulate placing orders
    let customerId = 1;
    setInterval(async () => {
      await placeOrder(`CUST-${customerId}`);
      console.log(`Placed order for CUST-${customerId}`);
      customerId++;
    }, 5000); // Place a new order every 5 seconds
  } catch (error) {
    console.error("An error occurred in the main function:", error);
  }
}

main().catch(console.error);
