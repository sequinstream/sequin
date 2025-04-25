// /**
//  * Example client code that demonstrates how to start a workflow.
//  * This is a template that can be used as a reference for creating new clients.
//  */

// import { Connection, Client } from '@temporalio/client';
// import { nanoid } from 'nanoid';
// import { TASK_QUEUE_NAME } from './shared';

// // Import the workflow
// import { userDeletionCleanupWorkflow } from './workflows';

// /**
//  * Demonstrates how to:
//  * 1. Connect to a Temporal server
//  * 2. Start a workflow
//  * 3. Wait for the workflow result
//  */
// async function run() {
//   // Connect to the Temporal server
//   // In development, this defaults to localhost:7233
//   const connection = await Connection.connect({ address: 'localhost:7233' });

//   // Example of production configuration:
//   // {
//   //   address: 'foo.bar.tmprl.cloud',
//   //   tls: {
//   //     // Add TLS configuration here
//   //   }
//   // }

//   // Create a client to interact with the Temporal server
//   const client = new Client({
//     connection,
//     // Uncomment to use a specific namespace
//     // namespace: 'foo.bar', // defaults to 'default' if not specified
//   });

//   // Start a new workflow execution
//   const handle = await client.workflow.start(userDeletionCleanupWorkflow, {
//     // The task queue the workflow will run on
//     taskQueue: TASK_QUEUE_NAME,
//     // The workflow takes no arguments since it uses signals
//     args: [],
//     // Generate a unique workflow ID
//     // In production, use a meaningful business ID like customerId or transactionId
//     workflowId: 'workflow-' + nanoid(),
//   });

//   console.log(`Started workflow ${handle.workflowId}`);

//   // Wait for and log the workflow result
//   console.log(await handle.result());
// }

// // Run the client and handle any errors
// run().catch((err) => {
//   console.error(err);
//   process.exit(1);
// });
