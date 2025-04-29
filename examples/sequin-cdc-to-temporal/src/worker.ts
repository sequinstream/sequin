/**
 * This file sets up a Temporal worker that processes workflow tasks.
 * The worker is responsible for executing both workflows and activities.
 */

import { Worker } from '@temporalio/worker';
import * as activities from './activities';
import { TASK_QUEUE_NAME } from './shared';

/**
 * Creates and runs a Temporal worker that:
 * 1. Polls the Temporal server for tasks
 * 2. Executes workflows and activities when tasks are received
 * 3. Reports results back to the Temporal server
 */
async function runWorker() {
  // Create a worker that connects to the Temporal server
  const worker = await Worker.create({
    // Path to the module containing our workflow definitions
    workflowsPath: require.resolve('./workflows'),
    // Register all exported activities for execution
    activities,
    // The task queue this worker will poll for work
    taskQueue: TASK_QUEUE_NAME
  });

  console.log(`Worker started for task queue: ${TASK_QUEUE_NAME}`);
  // Start polling for and processing workflow tasks
  await worker.run();
}

// Start the worker and handle any errors
runWorker().catch((err) => {
  console.error('Worker failed: ', err);
  process.exit(1);
});
