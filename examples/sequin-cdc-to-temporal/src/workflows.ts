// src/workflows.ts

import { defineSignal, condition, proxyActivities, setHandler } from '@temporalio/workflow';
import * as activities from './activities';

// Configure activities for use in the Workflow
const { cleanUpExternalSystems, sendDeletionConfirmation } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 5 }       // optional hardening
});

/**
 * Signal shape: (userId, email)
 * You could also pass an object if you prefer.
 */
export const userDeletedSignal = defineSignal<[string, string]>('userDeleted');

/**
 * A *long-lived* workflow that does nothing until it receives
 * a `userDeletedSignal`.  It then runs the two activities and
 * completes.  (You could keep it open for more signals if needed.)
 */
export async function userDeletionCleanupWorkflow(): Promise<void> {
  let done = false;

  // Signal handler runs inside workflow lock – fully deterministic
  setHandler(userDeletedSignal, async (userId: string, email: string) => {
    await cleanUpExternalSystems(userId);
    await sendDeletionConfirmation(email);
    done = true;
  });

  // Park the workflow until the signal arrives
  await condition(() => done);
}
