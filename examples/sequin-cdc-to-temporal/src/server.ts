import { Connection, Client } from '@temporalio/client';
import { userDeletionCleanupWorkflow, userDeletedSignal } from './workflows';
import express, { Request, Response, RequestHandler } from 'express';
import { TASK_QUEUE_NAME } from './shared';

const app = express();
const PORT = process.env.PORT || 3333;

app.use(express.json()); // parse JSON request bodies

/**
 * Health check endpoint that confirms the service is running.
 * In a production environment, this could be expanded to check:
 * - Database connectivity
 * - Temporal server connectivity
 * - Other dependent services
 */
app.get('/', (_req, res) => {
  res.send('Sequin-Temporal Webhook Receiver is running');
});

// Create a single Temporal client instance for the entire application
let client: Client;

/**
 * Establishes the connection to the Temporal server.
 * This should be called once when the application starts.
 * By default, connects to localhost:7233
 */
async function setupTemporal() {
  const connection = await Connection.connect();
  client = new Client({ connection });
}

/**
 * Handles incoming webhooks from Sequin containing CDC events.
 * For each user deletion event:
 * 1. Extracts the user ID and email from the event
 * 2. Creates a deterministic workflow ID
 * 3. Starts (or signals) a workflow to handle the deletion cleanup
 */
const webhookHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
  const { data } = req.body || {};
  if (!Array.isArray(data)) {
    res.status(400).send('Bad request');
    return;
  }

  try {
    for (const change of data) {
      if (
        change.action === 'delete' &&
        change.metadata?.table_name === 'users'
      ) {
        const { id: userId, email } = change.record;

        const workflowId = `user-delete-${userId}`;

        await client.workflow.signalWithStart(userDeletionCleanupWorkflow, {
          workflowId,
          taskQueue: TASK_QUEUE_NAME,
          args: [],                         // workflow takes no start args
          signal: userDeletedSignal,
          signalArgs: [userId, email]
        });

        console.log(`Signalled (or started) workflow ${workflowId}`);
      }
    }
    res.sendStatus(200);                    // acknowledge Sequin
  } catch (err) {
    console.error(err);
    res.sendStatus(500);                    // Sequin will retry
  }
};

app.post('/sequin-webhook', webhookHandler);

// Initialize Temporal connection and start the Express server
setupTemporal().then(() => {
  app.listen(PORT, () => console.log(`Webhook listener started on port ${PORT}`));
});
